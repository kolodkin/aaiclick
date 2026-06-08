export type JobStatus = "PENDING" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
export type TaskStatus =
  | "PENDING"
  | "CLAIMED"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED"
  | "PENDING_CLEANUP"
  | "UPSTREAM_FAILED";

export interface Page<T> {
  items: T[];
  total: number | null;
  next_cursor: string | null;
}

export interface JobView {
  id: number;
  name: string;
  status: JobStatus;
  run_type: string;
  preservation_mode: string;
  registered_job_id: number | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  total_tasks: number;
  completed_tasks: number;
}

export interface TaskView {
  id: number;
  job_id: number;
  entrypoint: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
}

export interface JobDetail extends JobView {
  tasks: TaskView[];
  duration_ms: number | null;
}

export interface TaskDetail extends TaskView {
  kwargs: Record<string, unknown>;
  result: Record<string, unknown> | null;
  log_path: string | null;
  worker_id: number | null;
  max_retries: number;
}

export interface TaskLogs {
  available: boolean;
  log_path: string | null;
  lines: string[];
}

export interface RegisteredJobView {
  id: number;
  name: string;
  entrypoint: string;
  enabled: boolean;
  schedule: string | null;
  default_kwargs: Record<string, unknown> | null;
  preservation_mode: string | null;
  next_run_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface Problem {
  title: string;
  status: number;
  detail: string | null;
  code: string | null;
}

export interface RunJobRequest {
  name: string;
  kwargs?: Record<string, unknown>;
  preservation_mode?: string | null;
}

export interface RegisterJobRequest {
  name?: string;
  entrypoint: string;
  schedule?: string | null;
  default_kwargs?: Record<string, unknown> | null;
  enabled?: boolean;
  preservation_mode?: string | null;
}
