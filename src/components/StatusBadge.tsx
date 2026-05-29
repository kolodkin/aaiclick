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

export function StatusBadge({ status }: { status: JobStatus | TaskStatus }) {
  const cls = KNOWN.has(status) ? `b-${status}` : "b-unknown";
  return <span className={`badge ${cls}`}>{status}</span>;
}
