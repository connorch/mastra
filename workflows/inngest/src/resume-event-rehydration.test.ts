import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { describe, it, expect, vi } from 'vitest';
import { z } from 'zod';
import type { InngestRun } from './run';
import type { InngestWorkflow } from './workflow';
import { init } from './index';

type InngestHandler = (ctx: { event: { data: any }; step: any; attempt: number }) => Promise<any>;

/**
 * End-to-end unit tests for the slim resume path: `_resumeAndSendEvent()` ships
 * the resume event without stepResults/initialState copies, and the workflow
 * handler rehydrates both from its own persisted snapshot before executing.
 *
 * These tests do NOT require a live Inngest dev server. They capture the raw
 * handler passed to `inngest.createFunction()` and drive it directly with mock
 * durable step tools, feeding it the exact event payload `resumeAsync()` sends.
 */
describe('resume event snapshot rehydration', () => {
  function createMockStepTools() {
    return {
      run: vi.fn(async (_id: string, fn: () => Promise<unknown>) => fn()),
      sleep: vi.fn(),
      sleepUntil: vi.fn(),
      invoke: vi.fn(),
      waitForEvent: vi.fn(),
      sendEvent: vi.fn(),
    };
  }

  function createHarness() {
    const inngest = new Inngest({ id: 'resume-rehydration-test', baseUrl: 'http://localhost:9999' });
    const sendMock = vi.fn().mockResolvedValue({ ids: ['evt_123'] });
    (inngest as any).send = sendMock;

    const { createWorkflow, createStep } = init(inngest);

    const step1 = createStep({
      id: 'step1',
      inputSchema: z.object({ value: z.string() }),
      resumeSchema: z.object({ resumed: z.string() }),
      suspendSchema: z.object({}),
      outputSchema: z.object({ result: z.string() }),
      execute: async ({ inputData, resumeData, suspend }) => {
        if (!resumeData) {
          return suspend({});
        }
        return { result: `${inputData.value}:${resumeData.resumed}` };
      },
    });

    const workflow = createWorkflow({
      id: 'resume-rehydration-wf',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ result: z.string() }),
      steps: [step1],
    });
    workflow.then(step1).commit();

    const mastra = new Mastra({
      storage: new MockStore(),
      workflows: { 'resume-rehydration-wf': workflow as any },
    });

    // Capture the raw Inngest handler so tests can drive it without a dev server.
    const createFunctionSpy = vi.spyOn(inngest, 'createFunction');
    (workflow as unknown as InngestWorkflow<any, any, any, any, any, any>).getFunction();
    const handler = createFunctionSpy.mock.calls[0]?.[1] as unknown as InngestHandler;
    expect(handler).toBeTypeOf('function');

    return { workflow, mastra, sendMock, handler };
  }

  it('suspend→resume round-trips through a resume event with no state copies', async () => {
    const { workflow, mastra, sendMock, handler } = createHarness();

    const run = (await workflow.createRun()) as unknown as InngestRun;

    // Initial execution: the step suspends and the handler persists the snapshot.
    const initial = await handler({
      event: { data: { inputData: { value: 'hello' }, runId: run.runId } },
      step: createMockStepTools(),
      attempt: 0,
    });
    expect(initial.result.status).toBe('suspended');

    const workflowsStore = await mastra.getStorage()!.getStore('workflows');
    const suspendedSnapshot = await workflowsStore!.loadWorkflowSnapshot({
      workflowName: 'resume-rehydration-wf',
      runId: run.runId,
    });
    expect(suspendedSnapshot?.suspendedPaths).toHaveProperty('step1');

    // Dispatch the resume through the real client path, capturing the event.
    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });
    const resumeEvent = sendMock.mock.calls.at(-1)![0];
    expect(resumeEvent.name).toBe('workflow.resume-rehydration-wf');
    expect(resumeEvent.data.stepResults).toBeUndefined();
    expect(resumeEvent.data.initialState).toBeUndefined();
    expect(resumeEvent.data.resume.stepResults).toBeUndefined();

    // Feed the exact captured payload back through the handler, as Inngest would.
    // The handler must rehydrate stepResults/initialState from its own snapshot.
    const resumed = await handler({
      event: { data: resumeEvent.data },
      step: createMockStepTools(),
      attempt: 0,
    });
    expect(resumed.result.status).toBe('success');
    expect(resumed.result.result).toEqual({ result: 'hello:world' });
  });

  it('still executes old-style resume events that carry state copies', async () => {
    const { workflow, sendMock, handler } = createHarness();

    const run = (await workflow.createRun()) as unknown as InngestRun;

    const initial = await handler({
      event: { data: { inputData: { value: 'hello' }, runId: run.runId } },
      step: createMockStepTools(),
      attempt: 0,
    });
    expect(initial.result.status).toBe('suspended');

    await run.resumeAsync({ step: 'step1', resumeData: { resumed: 'world' } });
    const resumeEvent = sendMock.mock.calls.at(-1)![0];

    // Reconstruct the legacy wire format: state copies shipped in the event.
    const legacyData = {
      ...resumeEvent.data,
      initialState: {},
      stepResults: { input: { value: 'hello' }, step1: { status: 'suspended', payload: { value: 'hello' } } },
      resume: {
        ...resumeEvent.data.resume,
        stepResults: { input: { value: 'hello' }, step1: { status: 'suspended', payload: { value: 'hello' } } },
      },
    };

    const resumed = await handler({
      event: { data: legacyData },
      step: createMockStepTools(),
      attempt: 0,
    });
    expect(resumed.result.status).toBe('success');
    expect(resumed.result.result).toEqual({ result: 'hello:world' });
  });

  it('fails resume with a non-retriable error when no snapshot exists for rehydration', async () => {
    const { handler } = createHarness();

    await expect(
      handler({
        event: {
          data: {
            inputData: { resumed: 'world' },
            runId: 'missing-run-id',
            workflowId: 'resume-rehydration-wf',
            resume: {
              steps: ['step1'],
              resumePayload: { resumed: 'world' },
              resumePath: [0],
            },
          },
        },
        step: createMockStepTools(),
        attempt: 0,
      }),
    ).rejects.toThrow('Cannot resume run missing-run-id: snapshot not found for rehydration');
  });
});
