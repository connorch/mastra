---
'@mastra/inngest': patch
---

Fixed a memory leak that could crash the request streaming a durable agent run on memory-constrained runtimes such as Cloudflare Workers ("Worker exceeded memory limit").

Two sources of unbounded memory retention were removed:

- **Unread Inngest Realtime streams.** `subscribe()` from `inngest/realtime` delivers events to the callback but also returns a second stream. Mastra never read that returned stream, so every event on the channel stayed retained in memory for the life of the subscription - for a durable agent run, the run's entire event payload. The unread stream is now cancelled immediately, and unsubscribing now closes the underlying WebSocket instead of leaving it connected until the process exits. This applies to every durable agent consumer (`stream()`, `observe()`, `resume()`, `generate()`) and to `run.start()` on Inngest workflows, which holds a `watch` subscription whose events can be megabytes each.
- **Workflow watch events in the stream cache.** The pubsub that durable agent workflows publish through cached every event so `observe()` can replay missed `agent.stream.*` events after a reconnect. Workflow watch events (`workflow.events.v2.*`) went into the same cache even though nothing ever replays them - retaining every step's full payload in the server's memory for the cache TTL. Watch events are now delivered live-only; agent stream replay is unchanged.
