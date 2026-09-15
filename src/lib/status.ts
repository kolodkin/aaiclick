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

// Mirrors TASK_PENDING / TASK_CLAIMED in the same module: queued, or claimed
// by a worker that has not begun executing. No output exists yet — which is a
// different thing from output not having arrived.
const NOT_STARTED_TASK: ReadonlySet<TaskStatus> = new Set<TaskStatus>(["PENDING", "CLAIMED"]);

export function isTaskStarted(status: TaskStatus): boolean {
  return !NOT_STARTED_TASK.has(status);
}
