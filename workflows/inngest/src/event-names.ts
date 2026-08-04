import type { Inngest } from 'inngest';

/**
 * Builds the Inngest event name that starts or resumes a workflow, scoped by
 * the Inngest app id. Inngest matches event-triggered functions across every
 * app registered in an environment by event name alone, so an unscoped
 * `workflow.<id>` collides when two deployments of the same codebase (e.g.
 * staging and production) share one environment: each app would execute the
 * other's runs against its own storage, then publish failure events onto the
 * run's shared realtime channel. The app-id prefix keeps the send/trigger
 * pairing app-local.
 */
export function workflowEventName(inngest: Inngest.Any, workflowId: string): string {
  return `${inngest.id}.workflow.${workflowId}`;
}

/** Cancellation counterpart of {@link workflowEventName}. */
export function workflowCancelEventName(inngest: Inngest.Any, workflowId: string): string {
  return `${inngest.id}.cancel.workflow.${workflowId}`;
}
