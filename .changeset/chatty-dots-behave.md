---
'@mastra/inngest': patch
---

Fixed durable agent streams hanging forever when a run's events exceed the Inngest Realtime message size limit (~512 KB).

Inngest Realtime delivers an oversized message truncated as a raw string that subscribers cannot parse. When that happened to a terminal event (`finish`, `error`, `abort`), every client stream attached to the run stayed open forever, since durable streams only close on a terminal event.

Two defenses were added in the Inngest pubsub transport:

- Before publishing, the live copy of known-huge lifecycle events is slimmed past a ~400 KB soft limit: `finish` drops the accumulated `output.steps` (and `output.text` if still over the limit), keeping `output.usage` and `stepResult`; `step-start` blanks the full model request body. The pubsub cache is written before publish, so replays still carry the full event.
- On subscribe, a truncated raw-string message that can still be identified as a terminal event is salvaged into a minimal terminal envelope so attached streams close; truncated non-terminal messages are dropped instead of being delivered as unparseable garbage.
