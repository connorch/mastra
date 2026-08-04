/**
 * Regression tests for InngestPubSub handling of the Inngest Realtime message
 * size cap (~512 KB on the dev server and Inngest Cloud).
 *
 * An oversized publish is not rejected - it is DELIVERED TRUNCATED as a raw
 * JSON string subscribers cannot parse. A truncated terminal event leaves
 * every attached durable stream open forever (streams only close on
 * finish/error/abort). Two defenses under test:
 *
 * - publish: the LIVE copy of known-huge lifecycle events (`finish`,
 *   `step-start`) is slimmed past a soft limit to the fields stream
 *   consumers actually read; under-limit events pass through untouched. The
 *   CachingPubSub cache is written before publish and keeps the full event
 *   for replays.
 * - subscribe: a raw-string (truncated) message is salvaged into a minimal
 *   terminal envelope when its surviving head identifies a terminal event,
 *   so streams still close; truncated non-terminal messages are dropped.
 */

import type { Inngest } from 'inngest';
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest';

const harness = vi.hoisted(() => {
  const state: {
    handlers: Array<(message: unknown) => void>;
  } = { handlers: [] };

  const subscribe = async (_token: unknown, callback?: (message: unknown) => void) => {
    if (callback) {
      state.handlers.push(callback);
    }
    return { cancel: () => {} };
  };

  return { state, subscribe };
});

vi.mock('inngest/realtime', () => ({ subscribe: harness.subscribe }));

// The suite runs with --no-isolate, so another test file may already have
// loaded './pubsub' with the REAL 'inngest/realtime' in the shared module
// registry - in which case the vi.mock above would never apply. Reset the
// registry and re-import the subject so its 'inngest/realtime' binding
// resolves to the mock regardless of file execution order.
let InngestPubSub: typeof import('./pubsub').InngestPubSub;
beforeAll(async () => {
  vi.resetModules();
  ({ InngestPubSub } = await import('./pubsub'));
});

/** Fake Inngest client exposing only what InngestPubSub touches. */
function makeInngest() {
  const publish = vi.fn().mockResolvedValue(undefined);
  return { client: { realtime: { publish } } as unknown as Inngest, publish };
}

// Comfortably past the 400k-char soft limit on its own.
const HUGE = 'x'.repeat(500_000);

describe('InngestPubSub realtime size cap', () => {
  let pubsub: InstanceType<typeof InngestPubSub>;
  let publishMock: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    harness.state.handlers.length = 0;
    const { client, publish } = makeInngest();
    publishMock = publish;
    pubsub = new InngestPubSub(client, 'test-workflow');
  });

  describe('publish slims oversized agent events', () => {
    it('keeps only stream-consumed fields of an over-limit finish event', async () => {
      const event = {
        type: 'finish',
        runId: 'run-1',
        data: {
          output: {
            text: 'final answer',
            usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
            steps: [{ toolResults: [{ result: HUGE }] }],
          },
          stepResult: { reason: 'stop', warnings: [] },
        },
      };

      await pubsub.publish('agent.stream.run-1', event);

      expect(publishMock).toHaveBeenCalledTimes(1);
      const sent = publishMock.mock.calls[0]![1];
      // The unbounded per-step accumulator is dropped from the live copy...
      expect(sent.data.output.steps).toEqual([]);
      // ...while everything the stream adapter's finish converter reads survives.
      expect(sent.data.output.text).toBe('final answer');
      expect(sent.data.output.usage).toEqual({ inputTokens: 10, outputTokens: 20, totalTokens: 30 });
      expect(sent.data.stepResult).toEqual({ reason: 'stop', warnings: [] });
      expect(sent.type).toBe('finish');
      expect(sent.runId).toBe('run-1');
      expect(JSON.stringify(sent).length).toBeLessThan(500_000);
    });

    it('also drops text when a finish event is over the limit on text alone', async () => {
      const event = {
        type: 'finish',
        runId: 'run-2',
        data: {
          output: { text: HUGE, usage: { totalTokens: 5 }, steps: [] },
          stepResult: { reason: 'stop' },
        },
      };

      await pubsub.publish('agent.stream.run-2', event);

      const sent = publishMock.mock.calls[0]![1];
      expect(sent.data.output.text).toBeUndefined();
      expect(sent.data.output.usage).toEqual({ totalTokens: 5 });
      expect(sent.data.stepResult).toEqual({ reason: 'stop' });
      expect(JSON.stringify(sent).length).toBeLessThan(500_000);
    });

    it('blanks the model request of an over-limit step-start event', async () => {
      const event = {
        type: 'step-start',
        runId: 'run-3',
        data: { type: 'step-start', stepId: 'step-1', request: { body: HUGE }, warnings: [] },
      };

      await pubsub.publish('agent.stream.run-3', event);

      const sent = publishMock.mock.calls[0]![1];
      expect(sent.data.request).toEqual({});
      expect(sent.data.stepId).toBe('step-1');
      expect(sent.data.type).toBe('step-start');
      expect(JSON.stringify(sent).length).toBeLessThan(500_000);
    });

    it('publishes under-limit agent events untouched', async () => {
      const event = {
        type: 'finish',
        runId: 'run-4',
        data: {
          output: { text: 'hi', usage: { totalTokens: 1 }, steps: [{ toolResults: [{ result: 'small' }] }] },
          stepResult: { reason: 'stop' },
        },
      };

      await pubsub.publish('agent.stream.run-4', event);

      // Same reference: no copy, no field surgery.
      expect(publishMock.mock.calls[0]![1]).toBe(event);
    });

    it('publishes oversized non-lifecycle agent events untouched', async () => {
      const event = {
        type: 'chunk',
        runId: 'run-5',
        data: { type: 'tool-result', payload: { result: HUGE } },
      };

      await pubsub.publish('agent.stream.run-5', event);

      // Chunks have no slim-safe subset; the subscribe-side salvage drops the
      // truncated copy and the cache replay carries the full one.
      expect(publishMock.mock.calls[0]![1]).toBe(event);
    });
  });

  describe('subscribe salvages truncated (raw string) agent messages', () => {
    /** Simulate realtime delivery of an event truncated at the size cap. */
    const truncated = (event: object, cutAt = 4000) => JSON.stringify(event).slice(0, cutAt);

    it('synthesizes a minimal finish envelope so streams still close', async () => {
      const received: any[] = [];
      await pubsub.subscribe('agent.stream.run-1', event => received.push(event));
      const handler = harness.state.handlers[0]!;

      handler({
        data: truncated({
          type: 'finish',
          runId: 'run-1',
          data: { output: { text: HUGE, usage: { totalTokens: 1 }, steps: [] }, stepResult: { reason: 'stop' } },
        }),
      });

      expect(received).toHaveLength(1);
      expect(received[0].type).toBe('finish');
      expect(received[0].runId).toBe('run-1');
      // Minimal payload the finish converter can terminate on.
      expect(received[0].data.stepResult.reason).toBe('stop');
      expect(received[0].data.output.usage).toEqual({});
      expect(received[0].id).toBeDefined();
      expect(received[0].createdAt).toBeInstanceOf(Date);
    });

    it('synthesizes error and abort envelopes', async () => {
      const received: any[] = [];
      await pubsub.subscribe('agent.stream.run-2', event => received.push(event));
      const handler = harness.state.handlers[0]!;

      handler({ data: truncated({ type: 'error', runId: 'run-2', data: { error: { message: HUGE } } }) });
      handler({ data: truncated({ type: 'abort', runId: 'run-2', data: { steps: [{ payload: HUGE }] } }) });

      expect(received).toHaveLength(2);
      expect(received[0].type).toBe('error');
      expect(received[0].data.error.message).toMatch(/size limit/);
      expect(received[1].type).toBe('abort');
      expect(received[1].data).toEqual({ steps: [] });
    });

    it('drops truncated non-terminal messages instead of delivering garbage', async () => {
      const received: any[] = [];
      await pubsub.subscribe('agent.stream.run-3', event => received.push(event));
      const handler = harness.state.handlers[0]!;

      handler({
        data: truncated({ type: 'chunk', runId: 'run-3', data: { type: 'tool-result', payload: { result: HUGE } } }),
      });
      // Head too mangled to even identify the event: also dropped.
      handler({ data: HUGE.slice(0, 100) });

      expect(received).toHaveLength(0);
    });

    it('still delivers well-formed (object) agent messages', async () => {
      const received: any[] = [];
      await pubsub.subscribe('agent.stream.run-4', event => received.push(event));
      const handler = harness.state.handlers[0]!;

      handler({ data: { type: 'chunk', runId: 'run-4', data: { type: 'text-delta', payload: { text: 'hi' } } } });

      expect(received).toHaveLength(1);
      expect(received[0].type).toBe('chunk');
      expect(received[0].data).toEqual({ type: 'text-delta', payload: { text: 'hi' } });
    });
  });
});
