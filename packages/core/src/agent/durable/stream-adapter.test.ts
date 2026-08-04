/**
 * Regression tests for durable agent stream event payload size.
 *
 * The step-start pubsub event used to ship the full model request body
 * (system prompt + entire message history + tool definitions - growing every
 * iteration, easily 100 KB+ per step). Nothing consumes it from durable
 * stream events, yet it travelled over the pubsub transport and was retained
 * in every consumer's replay buffer (`MastraModelOutput#bufferedChunks`) for
 * the life of the run.
 */

import { describe, it, expect } from 'vitest';
import { EventEmitterPubSub } from '../../events/event-emitter';
import type { Event } from '../../events/types';
import { AGENT_STREAM_TOPIC, AgentStreamEventTypes, DurableStepIds } from './constants';
import { emitStepStartEvent } from './stream-adapter';

describe('emitStepStartEvent', () => {
  it('does not ship the model request body on the step-start event', async () => {
    const pubsub = new EventEmitterPubSub();
    const runId = 'run-step-start';
    const received: Event[] = [];
    await pubsub.subscribe(AGENT_STREAM_TOPIC(runId), event => {
      received.push(event);
    });

    const hugeRequest = {
      body: JSON.stringify({ messages: [{ role: 'system', content: 'x'.repeat(100_000) }] }),
    };
    await emitStepStartEvent(pubsub, runId, {
      stepId: DurableStepIds.LLM_EXECUTION,
      request: hugeRequest,
      warnings: [],
    });

    expect(received).toHaveLength(1);
    const event = received[0]! as Event & { data: Record<string, unknown> };
    expect(event.type).toBe(AgentStreamEventTypes.STEP_START);
    expect(event.data).toMatchObject({
      type: 'step-start',
      stepId: DurableStepIds.LLM_EXECUTION,
      warnings: [],
    });
    // The chunk keeps its `StepStartPayload` shape, but the request body is
    // never shipped.
    expect(event.data.request).toEqual({});
  });
});
