---
'@mastra/inngest': patch
---

Scope workflow trigger and cancel event names by the Inngest app id. Inngest matches event-triggered functions across every app in an environment by event name alone, so the previous unscoped `workflow.<id>` / `cancel.workflow.<id>` names made two apps sharing one environment (e.g. staging and production deployments of the same codebase) execute each other's runs against their own storage and publish failure events onto the run's shared realtime channel. Event names are now `<appId>.workflow.<id>` / `<appId>.cancel.workflow.<id>`. Note: in-flight runs started before this change listen for the old names and will not resume across the upgrade.
