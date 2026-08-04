import { PubSub } from '@mastra/core/events';
import type { Event } from '@mastra/core/events';
import type { Inngest } from 'inngest';
import { subscribe } from 'inngest/realtime';

/**
 * Build a TopicRef compatible with Inngest SDK v4's `inngest.realtime.publish()`.
 * The runtime only requires `channel` and `topic`; `config.schema` is optional and
 * we leave it absent so no validation runs.
 */
function buildTopicRef(channel: string, topic: string) {
  return { channel, topic, config: {} as any };
}

/**
 * Parse a topic string and extract the runId and topic type.
 *
 * Supported formats:
 * - "workflow.events.v2.{runId}" - workflow events
 * - "agent.stream.{runId}" - agent stream events
 *
 * @returns { runId, topicType } or null if not a recognized format
 */
function parseTopic(topic: string): { runId: string; topicType: 'workflow' | 'agent' } | null {
  // Try workflow format first
  const workflowMatch = topic.match(/^workflow\.events\.v2\.(.+)$/);
  if (workflowMatch && workflowMatch[1]) {
    return { runId: workflowMatch[1], topicType: 'workflow' };
  }

  // Try agent stream format
  const agentMatch = topic.match(/^agent\.stream\.(.+)$/);
  if (agentMatch && agentMatch[1]) {
    return { runId: agentMatch[1], topicType: 'agent' };
  }

  return null;
}

// PATCH(walton): realtime transports cap message size (~512KB on the dev
// server and Inngest Cloud), and an oversized publish is DELIVERED TRUNCATED
// as a raw string subscribers cannot parse. A truncated terminal event would
// leave every attached stream open forever (the durable stream only closes on
// finish/error/abort), so slim the LIVE copy of the known-huge lifecycle
// events to the fields stream consumers actually read: the finish converter
// reads only stepResult.reason and output.usage, and step-start's `request`
// (the full model request) is never enqueued to observers at all. The
// CachingPubSub cache is written before this publish and keeps the full
// event for replays.
const REALTIME_AGENT_EVENT_SOFT_LIMIT_CHARS = 400_000;
function slimOversizedAgentEvent(event: any): any {
  let serializedLength: number;
  try {
    serializedLength = JSON.stringify(event)?.length ?? 0;
  } catch {
    return event;
  }
  if (serializedLength <= REALTIME_AGENT_EVENT_SOFT_LIMIT_CHARS) return event;
  if (event?.type === 'finish') {
    return {
      ...event,
      data: {
        output: { usage: event.data?.output?.usage },
        stepResult: { reason: event.data?.stepResult?.reason },
      },
    };
  }
  if (event?.type === 'step-start') {
    const { request: _request, ...rest } = event.data ?? {};
    return { ...event, data: rest };
  }
  return event;
}

/**
 * PubSub implementation for Inngest workflows.
 *
 * This bridges the PubSub abstract class interface with Inngest's realtime system:
 * - publish() uses `inngest.realtime.publish()` (Inngest SDK v4 client API).
 *   This is non-durable: it executes immediately and is not memoized as a step.
 *   When called inside an Inngest function it auto-includes the current runId.
 * - subscribe() uses `inngest/realtime` subscribe for real-time streaming.
 *
 * Supported topic formats:
 * - "workflow.events.v2.{runId}" - workflow events
 *   -> Inngest channel: "workflow:{workflowId}:{runId}", topic: "watch"
 * - "agent.stream.{runId}" - agent stream events (for InngestAgent)
 *   -> Inngest channel: "agent:{runId}", topic: "agent-stream"
 */
export class InngestPubSub extends PubSub {
  private inngest: Inngest;
  private workflowId: string;
  private subscriptions: Map<
    string,
    {
      unsubscribe: () => void;
      callbacks: Set<(event: Event, ack?: () => Promise<void>) => void>;
    }
  > = new Map();

  constructor(inngest: Inngest, workflowId: string) {
    super();
    this.inngest = inngest;
    this.workflowId = workflowId;
  }

  /**
   * Publish an event to Inngest's realtime system.
   *
   * Supported topic formats:
   * - "workflow.events.v2.{runId}" - workflow events
   *   -> channel: "workflow:{workflowId}:{runId}", topic: "watch"
   * - "agent.stream.{runId}" - agent stream events
   *   -> channel: "agent:{runId}", topic: "agent-stream"
   *   (Note: agent stream uses runId-only channel so nested workflows can publish to same channel)
   */
  async publish(topic: string, event: Omit<Event, 'id' | 'createdAt'>): Promise<void> {
    const parsed = parseTopic(topic);
    if (!parsed) {
      return; // Ignore unrecognized topic formats
    }

    const { runId, topicType } = parsed;

    // Use different Inngest topics and channels for different event types
    // Agent stream events use a runId-only channel so nested workflows publish to the same channel
    const inngestTopic = topicType === 'agent' ? 'agent-stream' : 'watch';
    const channel = topicType === 'agent' ? `agent:${runId}` : `workflow:${this.workflowId}:${runId}`;

    try {
      // For agent stream events, send the full event structure so subscribers can access type/runId/data
      // For workflow events, send just the data (existing behavior)
      const dataToSend = topicType === 'agent' ? slimOversizedAgentEvent(event) : event.data;
      await this.inngest.realtime.publish(buildTopicRef(channel, inngestTopic), dataToSend);
    } catch (err: any) {
      // For agent stream terminal events, rethrow — losing a finish/error event
      // causes the client stream to hang indefinitely
      if (topicType === 'agent' && (event.type === 'finish' || event.type === 'error')) {
        throw err;
      }
      // Non-terminal events: log but don't throw
      console.error('InngestPubSub publish error:', err?.message ?? err);
    }
  }

  /**
   * Subscribe to events from Inngest's realtime system.
   *
   * Supported topic formats:
   * - "workflow.events.v2.{runId}" - workflow events
   *   -> channel: "workflow:{workflowId}:{runId}", topic: "watch"
   * - "agent.stream.{runId}" - agent stream events
   *   -> channel: "agent:{runId}", topic: "agent-stream"
   *   (Note: agent stream uses runId-only channel so nested workflows can publish to same channel)
   */
  async subscribe(topic: string, cb: (event: Event, ack?: () => Promise<void>) => void): Promise<void> {
    const parsed = parseTopic(topic);
    if (!parsed) {
      return; // Ignore unrecognized topic formats
    }

    const { runId, topicType } = parsed;

    // Check if we already have a subscription for this topic
    if (this.subscriptions.has(topic)) {
      this.subscriptions.get(topic)!.callbacks.add(cb);
      return;
    }

    const callbacks = new Set<(event: Event, ack?: () => Promise<void>) => void>([cb]);

    // Use different Inngest topics and channels for different event types
    // Agent stream events use a runId-only channel so nested workflows publish to the same channel
    const inngestTopic = topicType === 'agent' ? 'agent-stream' : 'watch';
    const channel = topicType === 'agent' ? `agent:${runId}` : `workflow:${this.workflowId}:${runId}`;

    // Await the subscribe call to ensure the WebSocket connection is established
    // before we consider the subscription "ready". This prevents race conditions
    // where the workflow triggers before the subscription can receive events.
    const subscription = await subscribe(
      {
        channel,
        topics: [inngestTopic],
        app: this.inngest,
      },
      (message: any) => {
        // PATCH(walton): a message over the realtime size cap arrives
        // truncated as a raw JSON string. Salvage terminal events (type and
        // runId serialize first, inside the surviving head) into minimal
        // envelopes so attached streams still close; drop everything else —
        // the CachingPubSub cache carries the full copy for replays.
        if (topicType === 'agent' && typeof message.data === 'string') {
          const head = message.data.slice(0, 4096);
          const salvagedType = /"type"\s*:\s*"([^"]+)"/.exec(head)?.[1];
          const salvagedRunId = /"runId"\s*:\s*"([^"]+)"/.exec(head)?.[1];
          console.warn(`InngestPubSub: truncated realtime message on ${channel} (type=${salvagedType ?? 'unknown'})`);
          if (!salvagedType || !salvagedRunId) return;
          const salvagedData =
            salvagedType === 'finish'
              ? { output: { usage: {} }, stepResult: { reason: 'stop' } }
              : salvagedType === 'abort'
                ? { steps: [] }
                : salvagedType === 'error'
                  ? { error: { message: "The run's error event exceeded the realtime message size limit." } }
                  : null;
          if (!salvagedData) return;
          const salvagedEvent = {
            id: crypto.randomUUID(),
            createdAt: new Date(),
            type: salvagedType,
            runId: salvagedRunId,
            data: salvagedData,
          } as unknown as Event;
          for (const callback of callbacks) {
            callback(salvagedEvent);
          }
          return;
        }
        // For agent stream events, message.data is the full AgentStreamEvent structure (type, runId, data)
        // For workflow events, wrap message.data in a PubSub Event format
        // IMPORTANT: Always generate a unique `id` and `createdAt` for every event.
        // CachingPubSub deduplicates events by `id` — without a unique id, all events
        // after the first would be filtered out (since undefined === undefined in the seen set).
        let event: Event;
        if (topicType === 'agent' && message.data?.type && message.data?.runId) {
          // Agent stream event - spread the AgentStreamEvent data and add required Event fields
          event = {
            id: crypto.randomUUID(),
            createdAt: new Date(),
            ...message.data,
          } as unknown as Event;
        } else {
          // Workflow event or fallback - wrap in standard Event format
          event = {
            id: crypto.randomUUID(),
            type: inngestTopic,
            runId,
            data: message.data,
            createdAt: new Date(),
          };
        }

        for (const callback of callbacks) {
          callback(event);
        }
      },
    );

    // The SDK delivers every message to the callback via its own internal
    // stream AND fans it out to this returned stream. Nothing here ever reads
    // the returned stream, so without cancelling it every message on the
    // channel is retained in its write queue for the life of the subscription.
    // For a durable agent run that is the entire event payload of the run -
    // enough to exhaust a 128 MB Cloudflare Workers isolate on long streams.
    // cancel() detaches only this unread branch; callback delivery continues.
    void Promise.resolve(subscription.cancel()).catch(() => {});

    this.subscriptions.set(topic, {
      unsubscribe: () => {
        try {
          // close() tears down the underlying WebSocket subscription. Plain
          // cancel() would only detach the (already-cancelled) returned
          // stream and leave the socket connected (and parsing messages)
          // until process exit.
          subscription.close('unsubscribed');
        } catch (err) {
          console.error('InngestPubSub unsubscribe error:', err);
        }
      },
      callbacks,
    });
  }

  /**
   * Unsubscribe a callback from a topic.
   * If no callbacks remain, the underlying Inngest subscription is cancelled.
   */
  async unsubscribe(topic: string, cb: (event: Event, ack?: () => Promise<void>) => void): Promise<void> {
    const sub = this.subscriptions.get(topic);
    if (!sub) {
      return;
    }

    sub.callbacks.delete(cb);

    // If no more callbacks, cancel the subscription
    if (sub.callbacks.size === 0) {
      sub.unsubscribe();
      this.subscriptions.delete(topic);
    }
  }

  /**
   * Flush any pending operations. No-op for Inngest.
   */
  async flush(): Promise<void> {
    // No-op for Inngest
  }

  /**
   * Clean up all subscriptions during graceful shutdown.
   */
  async close(): Promise<void> {
    for (const [, sub] of this.subscriptions) {
      sub.unsubscribe();
    }
    this.subscriptions.clear();
  }
}
