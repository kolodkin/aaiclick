import type { JobStatus, TaskStatus } from "../api/types";

// Mirrors TASK_COMPLETED + NON_SUCCESS_TASK_STATUSES in
// aaiclick/orchestration/models.py.
const TERMINAL_TASK: ReadonlySet<TaskStatus> = new Set<TaskStatus>([
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "UPSTREAM_FAILED",
]);

export function isTerminalTask(status: TaskStatus): boolean {
  return TERMINAL_TASK.has(status);
}

// Mirrors TASK_PENDING in the same module: queued, not yet claimed. No output
// exists yet — which is a different thing from output not having arrived.
export function isTaskStarted(status: TaskStatus): boolean {
  return status !== "PENDING";
}

// The `b-<STATUS>` color classes are bound to the union members below; any
// value outside the declared statuses falls through to `b-unknown` rather than
// emitting an invalid `b-<garbage>` class via string concatenation.
const KNOWN: ReadonlySet<string> = new Set<JobStatus | TaskStatus>([
  "PENDING",
  "RUNNING",
  "COMPLETED",
  "FAILED",
  "CANCELLED",
  "PENDING_FAILURE_CLEANUP",
  "PENDING_CANCELLED_CLEANUP",
  "UPSTREAM_FAILED",
]);

export function statusClass(status: JobStatus | TaskStatus): string {
  return KNOWN.has(status) ? `b-${status}` : "b-unknown";
}
