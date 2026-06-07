import type { JobStatus, TaskStatus } from "../api/types";

// CSS classes are bound to the union members below; any value outside the
// declared statuses falls through to `b-unknown` rather than emitting an
// invalid `b-<garbage>` class via string concatenation.
const KNOWN: ReadonlySet<string> = new Set<JobStatus | TaskStatus>([
  "PENDING",
  "CLAIMED",
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "PENDING_CLEANUP",
  "UPSTREAM_FAILED",
]);

export function StatusBadge({
  status,
  reason,
}: {
  status: JobStatus | TaskStatus;
  // Optional explanation surfaced on hover — e.g. why a CANCELLED task was
  // aborted (fail-fast sibling) vs. left blank for an operator cancellation.
  reason?: string | null;
}) {
  const cls = KNOWN.has(status) ? `b-${status}` : "b-unknown";
  return (
    <span className={`badge ${cls}`} title={reason ?? undefined}>
      {status}
    </span>
  );
}
