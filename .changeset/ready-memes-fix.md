---
'@mastra/core': patch
---

Fixed durable agents losing the request context after the first iteration. Dynamic getModel and getToolsForExecution resolvers now see the run's request context on every iteration when durable steps execute cross-process (Inngest workers, recovered runs), instead of an empty context from iteration 2 on. The serialized request context snapshot is now carried through iteration state and forwarded to the LLM execution step.
