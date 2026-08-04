---
'@mastra/inngest': patch
---

Resuming a durable Inngest agent no longer copies the run's accumulated state into the resume event.

`createInngestAgent().resume()` was loading the suspended agentic-loop snapshot and shipping it back through Inngest as `initialState`, a top-level `stepResults`, and a second copy under `resume.stepResults`. For a durable agent that snapshot holds the whole conversation - message list, accumulated steps, tool results - so a long-running human-in-the-loop run could suspend successfully and then fail every resume attempt once the payload outgrew Inngest's event size limit.

The event now carries only the resume data and targeting information; the workflow handler rehydrates the state from its own persisted snapshot by run ID, keeping the resume event the same size at any conversation length. No API change.
