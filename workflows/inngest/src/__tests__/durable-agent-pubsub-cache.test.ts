/**
 * Regression tests for durable-agent pubsub cache retention.
 *
 * `createInngestAgent` wraps every workflow-run pubsub in a `CachingPubSub`
 * (via `__setPubsubFactory`) so `observe()` can replay `agent.stream.*`
 * history. That same pubsub also carries the workflow's own watch events
 * (`workflow.events.v2.*`), which can be megabytes per event and are never
 * replayed from history. Caching them retained the whole run's payload in the
 * cache (with the default `InMemoryServerCache`, inside the serving process)
 * for the cache TTL - enough to exhaust a 128 MB Cloudflare Workers isolate
 * on multi-iteration runs.
 */

import { Agent } from '@mastra/core/agent';
import { AGENT_STREAM_TOPIC } from '@mastra/core/agent/durable';
import { InMemoryServerCache } from '@mastra/core/cache';
import { EventEmitterPubSub } from '@mastra/core/events';
import type { Event } from '@mastra/core/events';
import { Inngest } from 'inngest';
import { describe, it, expect } from 'vitest';
import { createInngestAgent } from '../index';
import type { InngestWorkflow } from '../workflow';

function createTestAgent(cache: InMemoryServerCache) {
  const agent = new Agent({
    id: 'cache-test-agent',
    name: 'Cache Test Agent',
    instructions: 'Test',
    model: { provider: 'test', modelId: 'test-model', specificationVersion: 'v2' } as never,
  });
  const inngest = new Inngest({ id: 'pubsub-cache-tests' });
  // In-memory inner pubsub so publishes don't attempt real Inngest Realtime
  // network calls; the caching layer under test wraps whatever inner is given.
  return createInngestAgent({ agent, inngest, cache, pubsub: new EventEmitterPubSub() });
}

describe('createInngestAgent pubsub cache retention', () => {
  it('caches agent-stream events but not workflow watch events', async () => {
    const cache = new InMemoryServerCache();
    const durableAgent = createTestAgent(cache);
    const workflow = durableAgent.getDurableWorkflows()[0] as unknown as InngestWorkflow;

    const factory = workflow.__getPubsubFactory();
    expect(factory).toBeDefined();

    // Simulate the executor: the workflow run publishes through the
    // factory-wrapped pubsub.
    const wrapped = factory!(new EventEmitterPubSub());

    const runId = 'run-cache-1';
    const watchTopic = `workflow.events.v2.${runId}`;
    const agentTopic = AGENT_STREAM_TOPIC(runId);

    // Live delivery of watch events must keep working.
    const liveWatchEvents: Event[] = [];
    await wrapped.subscribe(watchTopic, event => {
      liveWatchEvents.push(event);
    });

    // A production-sized watch event is ~1.8 MB; a modest stand-in is enough
    // to assert on retained state.
    const watchPayload = { type: 'workflow-step-result', payload: { blob: 'x'.repeat(64 * 1024) } };
    const watchEventCount = 10;
    for (let i = 0; i < watchEventCount; i++) {
      await wrapped.publish(watchTopic, { type: 'watch', runId, data: { ...watchPayload, index: i } });
    }
    await wrapped.publish(agentTopic, {
      type: 'chunk',
      runId,
      data: { type: 'text-delta', payload: { text: 'hello' } },
    });

    expect(liveWatchEvents).toHaveLength(watchEventCount);

    // Agent-stream history must remain cached - that is the replay guarantee
    // observe() depends on.
    expect(await cache.listLength(`pubsub:${agentTopic}`)).toBe(1);

    // Watch events must NOT be retained in the cache: nothing ever replays
    // them, so caching them only pins the run's full payload in memory.
    expect(await cache.listLength(`pubsub:${watchTopic}`)).toBe(0);
  });

  it('does not cache watch events on the agent-level pubsub either', async () => {
    const cache = new InMemoryServerCache();
    const durableAgent = createTestAgent(cache);

    const runId = 'run-cache-2';
    const watchTopic = `workflow.events.v2.${runId}`;
    const agentTopic = AGENT_STREAM_TOPIC(runId);

    await durableAgent.pubsub.publish(watchTopic, { type: 'watch', runId, data: { big: 'y'.repeat(1024) } });
    await durableAgent.pubsub.publish(agentTopic, {
      type: 'chunk',
      runId,
      data: { type: 'text-delta', payload: { text: 'hi' } },
    });

    expect(await cache.listLength(`pubsub:${agentTopic}`)).toBe(1);
    expect(await cache.listLength(`pubsub:${watchTopic}`)).toBe(0);
  });
});
