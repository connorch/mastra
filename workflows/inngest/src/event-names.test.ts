import { Mastra } from '@mastra/core/mastra';
import { MockStore } from '@mastra/core/storage';
import { Inngest } from 'inngest';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import { z } from 'zod';
import { workflowCancelEventName, workflowEventName } from './event-names';
import type { InngestRun } from './run';
import { init } from './index';

/**
 * Workflow trigger/cancel event names must be scoped by the Inngest app id:
 * Inngest matches event-triggered functions across every app in an
 * environment by event name alone, so unscoped `workflow.<id>` events make
 * two apps sharing one environment (e.g. staging and production) execute
 * each other's runs. These tests pin the app-id prefix on both the function
 * triggers and every send site.
 */
describe('app-scoped workflow event names', () => {
  let inngest: Inngest;
  let sendMock: ReturnType<typeof vi.fn>;

  function buildWorkflow() {
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
      id: 'scoped-events-wf',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ result: z.string() }),
      steps: [step1],
    });
    workflow.then(step1).commit();

    return { workflow, step1 };
  }

  async function createRun() {
    const { workflow } = buildWorkflow();

    new Mastra({
      storage: new MockStore(),

      workflows: { 'scoped-events-wf': workflow as any },
    });

    const run = (await workflow.createRun()) as unknown as InngestRun;
    return { run, workflow };
  }

  beforeEach(() => {
    sendMock = vi.fn().mockResolvedValue({ ids: ['evt_123'] });
    inngest = new Inngest({ id: 'scoped-test-app', baseUrl: 'http://localhost:9999' });

    (inngest as any).send = sendMock;
  });

  it('builds names prefixed with the Inngest app id', () => {
    expect(workflowEventName(inngest, 'my-wf')).toBe('scoped-test-app.workflow.my-wf');
    expect(workflowCancelEventName(inngest, 'my-wf')).toBe('scoped-test-app.cancel.workflow.my-wf');
  });

  it('registers the workflow function with the scoped trigger and cancel events', async () => {
    const { workflow } = await createRun();

    const fn = (workflow as any).getFunction();
    const config = fn['opts'] ?? fn['config'] ?? {};

    expect(JSON.stringify(config)).toContain('scoped-test-app.workflow.scoped-events-wf');
    expect(JSON.stringify(config)).toContain('scoped-test-app.cancel.workflow.scoped-events-wf');
  });

  it('startAsync sends the scoped workflow event', async () => {
    const { run } = await createRun();

    await run.startAsync({ inputData: { value: 'hello' } });

    expect(sendMock).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'scoped-test-app.workflow.scoped-events-wf' }),
    );
  });

  it('cancel sends the scoped cancel event', async () => {
    const { run } = await createRun();

    await run.cancel();

    expect(sendMock).toHaveBeenCalledWith(
      expect.objectContaining({ name: 'scoped-test-app.cancel.workflow.scoped-events-wf' }),
    );
  });
});
