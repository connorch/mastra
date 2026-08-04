/**
 * Regression test: the serialized request context must survive across
 * agentic-loop iterations.
 *
 * `prepareForDurableExecution` snapshots the caller's request context onto
 * `workflowInput.requestContextEntries`, and the durable LLM step restores it
 * via `restoreRequestContext(input.requestContextEntries)` when it has to
 * rebuild runtime state from the Mastra instance (cross-isolate execution:
 * Inngest workers, recovered runs, evicted registry entries).
 *
 * Previously the field was dropped after the first iteration: the
 * `map-to-llm-input` mapping did not forward it to the LLM step, and
 * `createBaseIterationStateUpdate` omitted it when rebuilding iteration
 * state, so dynamic `getModel` / `getToolsForExecution` resolvers saw an
 * EMPTY request context from iteration 2 on.
 *
 * The test forces the cross-isolate rebuild path on every iteration by
 * tagging the mock model with `__metadataOnly` — resolveRuntimeDependencies
 * treats a metadata-only registry model as non-hydrated and re-resolves
 * model/tools from the agent with the restored request context each step.
 */

import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { AISDKV5LanguageModel } from '../../../llm/model/aisdk/v5/model';
import { Mastra } from '../../../mastra';
import { RequestContext } from '../../../request-context';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';

/** Model that calls a tool on the first step (forcing iteration 2), then answers. */
function createToolCallThenTextModel() {
  const counter = { llmCalls: 0 };
  const mock = new MockLanguageModelV2({
    doStream: async () => {
      counter.llmCalls++;
      if (counter.llmCalls === 1) {
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallId: 'call-1',
              toolName: 'echo',
              input: JSON.stringify({ msg: 'hi' }),
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 15, outputTokens: 10, totalTokens: 25 },
            },
          ]),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      }
      return {
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'done' },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 20, outputTokens: 15, totalTokens: 35 },
          },
        ]),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
  // Pre-wrap the mock so getModel returns this exact instance every time
  // (resolveModelConfig passes AISDKV5LanguageModel instances through instead
  // of re-wrapping), and mark it metadata-only so resolveRuntimeDependencies
  // never trusts the registry entry and rebuilds from the agent on EVERY
  // iteration — simulating cross-isolate step execution in-process. The flag
  // only affects registry hydration checks; the model still runs.
  const model = new AISDKV5LanguageModel(mock);
  (model as unknown as { __metadataOnly?: boolean }).__metadataOnly = true;
  return { model, counter };
}

describe('DurableAgent request context carry across iterations', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('dynamic getModel sees the request context on iteration 2+ (cross-isolate rebuild path)', async () => {
    const { model, counter } = createToolCallThenTextModel();

    // Record the tenantId visible to the dynamic model resolver on each call.
    const seenTenantIds: unknown[] = [];

    const echoTool = createTool({
      id: 'echo',
      description: 'Echoes input',
      inputSchema: z.object({ msg: z.string() }),
      execute: async input => `echo:${input.msg}`,
    });

    const baseAgent = new Agent({
      id: 'ctx-carry-agent',
      name: 'Ctx Carry Agent',
      instructions: 'Test request context carry',
      model: ({ requestContext }) => {
        seenTenantIds.push(requestContext.get('tenantId'));
        return model;
      },
      tools: { echo: echoTool },
    });

    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
    new Mastra({ agents: { durableAgent }, storage: new InMemoryStore(), logger: false });

    const requestContext = new RequestContext();
    requestContext.set('tenantId', 'tenant-42');

    // Registration/introspection may resolve the model outside any request —
    // only calls made from this run (prepare + per-iteration rebuilds) matter.
    const preRunCalls = seenTenantIds.length;

    const { output, cleanup } = await durableAgent.stream('Use the tool', {
      requestContext,
      maxSteps: 5,
    });

    await output.consumeStream();
    cleanup();

    // The loop really ran two LLM iterations (tool call, then final text).
    expect(counter.llmCalls).toBe(2);
    expect(await output.text).toBe('done');

    // getModel ran at least once during preparation and once per iteration's
    // runtime rebuild (3+ run-scoped calls total).
    const runCalls = seenTenantIds.slice(preRunCalls);
    expect(runCalls.length).toBeGreaterThanOrEqual(3);

    // EVERY run-scoped resolution — including the iteration-2 rebuild, which
    // restores the context from the serialized `requestContextEntries` — must
    // see the caller's value. Before the fix the entries were dropped from
    // iteration state, so rebuild-path calls saw an empty context (undefined).
    expect(runCalls).toEqual(runCalls.map(() => 'tenant-42'));
  }, 30000);
});
