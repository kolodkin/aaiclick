import type { TaskStatus } from "../api/types";

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
