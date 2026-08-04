---
'@mastra/core': patch
---

Reduced memory retained by durable agent streaming, which could crash streaming requests on memory-constrained runtimes such as Cloudflare Workers ("Worker exceeded memory limit"):

- Durable agent `step-start` stream events no longer ship the full model request body (system prompt, entire message history, and tool definitions - easily 100 KB+ per step, growing every iteration). Nothing consumed it, but it travelled over the event transport and was retained in every consumer's replay buffer for the life of the run. The event keeps its shape with an empty `request: {}`.
- `CachingPubSub` accepts a new `shouldCache` option to exclude topics from cached history. Durable agents use it so that only replayable `agent.stream.*` events are cached; high-volume workflow watch events are delivered live-only instead of being retained in the cache with no reader.
