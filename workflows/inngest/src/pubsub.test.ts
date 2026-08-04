/**
 * Regression tests for InngestPubSub subscription memory retention.
 *
 * `subscribe()` from `inngest/realtime` (SDK v4) delivers messages to the
 * provided callback through an internal fanout stream, but ALSO creates and
 * returns a second fanout stream. If the caller never reads that returned
 * stream, the SDK's fanout (`writer.ready.then(() => writer.write(chunk))`)
 * retains every message in a pending-write closure for the life of the
 * subscription - for a durable agent run that is the entire event payload of
 * the run, which is enough to exhaust a 128 MB Cloudflare Workers isolate.
 *
 * The mock below reproduces the SDK's fanout semantics (real
 * TransformStreams, ready-gated writes) and exposes retention counters so the
 * tests can assert on retained state:
 * - InngestPubSub must cancel the unread returned stream immediately.
 * - Unsubscribing the last callback must close the whole subscription (the
 *   WebSocket), not just cancel the returned stream.
 */

import { Inngest } from 'inngest';
import { describe, it, expect, beforeEach, vi } from 'vitest';
import { InngestPubSub } from './pubsub';

const harness = vi.hoisted(() => {
  /**
   * Faithful model of the SDK's StreamFanout: identity TransformStreams whose
   * writers are fed via `writer.ready.then(() => writer.write(chunk))`. An
   * unread stream stops resolving `ready` once its queue fills, so every
   * later chunk stays retained in the pending closure - exactly the SDK
   * behavior under test.
   */
  class MockFanout {
    writers = new Set<WritableStreamDefaultWriter<unknown>>();
    pending = new Map<WritableStreamDefaultWriter<unknown>, number>();

    createStream(): ReadableStream<unknown> {
      const { readable, writable } = new TransformStream<unknown, unknown>();
      const writer = writable.getWriter();
      this.writers.add(writer);
      this.pending.set(writer, 0);
      writer.closed
        .catch(() => {})
        .finally(() => {
          this.writers.delete(writer);
          this.pending.delete(writer);
        });
      return readable;
    }

    write(chunk: unknown): void {
      for (const writer of this.writers) {
        this.pending.set(writer, (this.pending.get(writer) ?? 0) + 1);
        writer.ready
          .then(() => writer.write(chunk))
          .then(() => {
            if (this.pending.has(writer)) {
              this.pending.set(writer, this.pending.get(writer)! - 1);
            }
          })
          .catch(() => {
            this.writers.delete(writer);
            this.pending.delete(writer);
          });
      }
    }

    /** Total chunks retained in pending-write closures across all writers. */
    retained(): number {
      let total = 0;
      for (const count of this.pending.values()) total += count;
      return total;
    }

    close(): void {
      for (const writer of this.writers) {
        try {
          void writer.close().catch(() => {});
        } catch {
          // already errored
        }
      }
      this.writers.clear();
      this.pending.clear();
    }
  }

  interface MockSubscription {
    fanout: MockFanout;
    deliver: (message: unknown) => void;
    closed: boolean;
  }

  const state: { subscriptions: MockSubscription[] } = { subscriptions: [] };

  const subscribe = async (_token: unknown, callback?: (message: unknown) => unknown) => {
    const fanout = new MockFanout();
    const sub: MockSubscription = {
      fanout,
      deliver: message => fanout.write(message),
      closed: false,
    };
    state.subscriptions.push(sub);

    // SDK: callback gets its own internal stream, drained by a reader loop.
    if (callback) {
      const callbackStream = fanout.createStream();
      void (async () => {
        const reader = callbackStream.getReader();
        try {
          while (!sub.closed) {
            const { done, value } = await reader.read();
            if (done) break;
            await callback(value);
          }
        } finally {
          reader.releaseLock();
        }
      })();
    }

    // SDK: a SECOND stream is created and returned regardless of the callback.
    const retStream = fanout.createStream();
    const extras = {
      getJsonStream: () => fanout.createStream(),
      getEncodedStream: () => fanout.createStream(),
      close: (_reason?: string) => {
        sub.closed = true;
        fanout.close();
      },
      unsubscribe: (_reason?: string) => {
        sub.closed = true;
        fanout.close();
      },
    };
    return Object.assign(retStream, extras);
  };

  return { state, subscribe };
});

vi.mock('inngest/realtime', () => ({ subscribe: harness.subscribe }));

const flush = async (ms = 20) => new Promise(resolve => setTimeout(resolve, ms));

describe('InngestPubSub subscription retention', () => {
  let pubsub: InngestPubSub;

  beforeEach(() => {
    harness.state.subscriptions.length = 0;
    pubsub = new InngestPubSub(new Inngest({ id: 'retention-test' }), 'test-workflow');
  });

  it('does not retain messages in the unread stream returned by the SDK', async () => {
    const received: unknown[] = [];
    await pubsub.subscribe('agent.stream.run-1', event => {
      received.push(event);
    });
    // Let the immediate cancel of the unread returned stream propagate.
    await flush();

    const sub = harness.state.subscriptions[0]!;
    // Only the callback's internal stream may remain attached to the fanout.
    expect(sub.fanout.writers.size).toBe(1);

    const messageCount = 50;
    for (let i = 0; i < messageCount; i++) {
      sub.deliver({ data: { type: 'chunk', runId: 'run-1', data: { index: i, payload: 'x'.repeat(1024) } } });
    }
    await flush();

    expect(received).toHaveLength(messageCount);
    // Nothing may be left sitting in pending-write closures.
    expect(sub.fanout.retained()).toBe(0);
  });

  it('closes the whole subscription when the last callback unsubscribes', async () => {
    const cb = () => {};
    await pubsub.subscribe('agent.stream.run-2', cb);
    await flush();

    const sub = harness.state.subscriptions[0]!;
    expect(sub.closed).toBe(false);

    await pubsub.unsubscribe('agent.stream.run-2', cb);
    // close() must tear down the underlying subscription, not merely cancel
    // the returned stream - otherwise the WebSocket (and its message parsing)
    // outlives the run.
    expect(sub.closed).toBe(true);
  });

  it('keeps the subscription open while other callbacks remain', async () => {
    const cb1 = () => {};
    const cb2 = () => {};
    await pubsub.subscribe('agent.stream.run-3', cb1);
    await pubsub.subscribe('agent.stream.run-3', cb2);
    await flush();

    // Second subscribe reuses the first subscription.
    expect(harness.state.subscriptions).toHaveLength(1);
    const sub = harness.state.subscriptions[0]!;

    await pubsub.unsubscribe('agent.stream.run-3', cb1);
    expect(sub.closed).toBe(false);

    await pubsub.unsubscribe('agent.stream.run-3', cb2);
    expect(sub.closed).toBe(true);
  });
});
