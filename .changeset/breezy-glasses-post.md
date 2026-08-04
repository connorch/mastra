---
'@mastra/inngest': patch
---

Fixed resume failing on large durable runs with Inngest. Resume events and nested invoke-resume payloads no longer carry copies of the suspended run's stepResults and initialState; the workflow handler rehydrates them from its own persisted snapshot instead. Previously these copies grew with accumulated state and could exceed Inngest's event size limits, failing the resume.
