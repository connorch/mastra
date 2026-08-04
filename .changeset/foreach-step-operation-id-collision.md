---
'@mastra/core': patch
---

Include the foreach iteration index in per-step durable operation ids. Parallel foreach iterations (e.g. the durable agent's concurrent tool calls) executed the same step with identical operation ids for the step body, span, and event operations; durable engines like Inngest memoize and schedule operations by id, so the collisions cross-wired or stalled runs nondeterministically (reliably hanging runs on the Inngest dev server, intermittently on Inngest Cloud, with "Duplicate step ID ... detected across parallel chains" warnings). Ids now use `step[index]` for foreach iterations; the index is stable across retries and resumes so memoization is unaffected. In-flight foreach runs started before this change resume with mismatched ids.
