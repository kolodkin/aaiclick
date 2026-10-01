import type { JobStatus, TaskStatus } from "../api/types";
import { statusClass } from "../lib/status";

export function StatusBadge({
  status,
  reason,
}: {
  status: JobStatus | TaskStatus;
  // Optional explanation surfaced on hover — e.g. why a CANCELLED task was
  // aborted (fail-fast sibling) vs. left blank for an operator cancellation.
  reason?: string | null;
}) {
  return (
    <span className={`badge ${statusClass(status)}`} title={reason ?? undefined}>
      {status}
    </span>
  );
}
