/**
 * Regression tests for InngestRun.getRunOutput() memory retention.
 *
 * `getRunOutput` runs in the request that called `run.start()` and holds an
 * Inngest Realtime `watch` subscription on the workflow channel for the whole
 * run. Workflow watch events carry full step payloads (megabytes each on
 * large runs). The SDK's `subscribe(token, callback)` returns a second fanout
 * stream in addition to feeding the callback; if that returned stream is
 * never read, every watch event is retained in its pending-write closures
 * until the subscription closes - which, before this fix, was never (cleanup
 * only cancelled the returned stream, leaving the WebSocket open).
 *
 * See `pubsub.test.ts` for the mock's fidelity notes.
 */

import type { Mastra } from '@mastra/core/mastra';
import { Inngest } from 'inngest';
import { describe, it, expect, beforeAll, beforeEach, vi } from 'vitest';

const harness = vi.hoisted(() => {
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

const flush = async (ms = 25) => new Promise(resolve => setTimeout(resolve, ms));

// The suite runs with --no-isolate, so another test file may already have
// loaded './run' with the REAL 'inngest/realtime' in the shared module
// registry - in which case the vi.mock above would never apply. Reset the
// registry and re-import the subject so its 'inngest/realtime' binding
// resolves to the mock regardless of file execution order.
let InngestRun: typeof import('./run').InngestRun;
beforeAll(async () => {
  vi.resetModules();
  ({ InngestRun } = await import('./run'));
});

function createRun() {
  const workflowsStore = {
    loadWorkflowSnapshot: vi.fn().mockResolvedValue({
      status: 'running',
      context: {},
      value: {},
    }),
  };
  const mastra = {
    getStorage: () => ({
      getStore: async (name: string) => (name === 'workflows' ? workflowsStore : undefined),
    }),
  } as unknown as Mastra;

  const run = new InngestRun(
    {
      workflowId: 'test-workflow',
      runId: 'run-oom',
      executionEngine: {} as never,
      executionGraph: { id: 'test-workflow', steps: [] } as never,
      serializedStepGraph: [],
      mastra,
      workflowSteps: {},
      workflowEngineType: 'inngest' as never,
    },
    new Inngest({ id: 'run-retention-test' }),
  );
  return { run, workflowsStore };
}

describe('InngestRun.getRunOutput watch subscription retention', () => {
  beforeEach(() => {
    harness.state.subscriptions.length = 0;
  });

  it('does not retain watch events and closes the subscription once resolved', async () => {
    const { run } = createRun();
    const outputPromise = run.getRunOutput('event-1', 60_000);

    // Wait for the realtime subscription to be established.
    await flush();
    expect(harness.state.subscriptions).toHaveLength(1);
    const sub = harness.state.subscriptions[0]!;

    // The unread returned stream must be detached immediately: only the
    // callback's internal stream may remain attached to the fanout.
    expect(sub.fanout.writers.size).toBe(1);

    // Simulate a long run: many non-terminal watch events with large payloads.
    const watchEventCount = 30;
    for (let i = 0; i < watchEventCount; i++) {
      sub.deliver({
        data: { type: 'workflow-step-result', payload: { stepIndex: i, blob: 'x'.repeat(4096) } },
      });
    }
    await flush();
    // None of them may be left retained in pending-write closures.
    expect(sub.fanout.retained()).toBe(0);

    // Terminal event resolves getRunOutput via the realtime path.
    sub.deliver({ data: { type: 'workflow-finish', payload: { status: 'success', result: { ok: true } } } });
    const output = await outputPromise;
    expect(output?.output?.result?.status).toBe('success');

    // Cleanup must close the whole subscription (WebSocket teardown), not
    // just cancel the returned stream.
    await flush();
    expect(sub.closed).toBe(true);
  });
});
