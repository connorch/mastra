---
'@mastra/inngest': patch
---

Fixed Inngest durable agents dropping the request context from the LLM step's input. The map-to-llm-input mapping now forwards the serialized request context, so dynamic model and tool resolvers see the run's request context on every iteration of a durable agent run.
