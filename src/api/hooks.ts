import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJSON, postJSON } from "./client";
import { isTaskStarted, isTerminalTask } from "../lib/status";
import type {
  JobDetail,
  JobGraphView,
  JobView,
  Page,
  RegisteredJobView,
  RegisterJobRequest,
  RunJobRequest,
  TaskDetail,
  TaskLogs,
  TaskStatus,
} from "./types";

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => fetchJSON<Page<JobView>>("/jobs"),
  });
}

export function useJob(ref: string) {
  return useQuery({
    queryKey: ["job", ref],
    queryFn: () => fetchJSON<JobDetail>(`/jobs/${encodeURIComponent(ref)}`),
    enabled: ref.length > 0,
  });
}

export function useJobGraph(ref: string) {
  return useQuery({
    queryKey: ["job-graph", ref],
    queryFn: () => fetchJSON<JobGraphView>(`/jobs/${encodeURIComponent(ref)}/graph`),
    enabled: ref.length > 0,
  });
}

export function useTask(id: string) {
  return useQuery({
    queryKey: ["task", id],
    queryFn: () => fetchJSON<TaskDetail>(`/tasks/${id}`),
    enabled: id.length > 0,
  });
}

// Logs reach ClickHouse from the task process on its own flush cadence, not
// through a SQL commit, so no /events signal marks a new line — a running task
// keeps the 2 s poll, and the `changed` signal for the final status write
// triggers the last refetch. A task that has not started cannot have produced
// output, so it is not fetched at all. `false`, not `undefined`: an unset
// interval inherits the QueryClient default and would poll a finished task's
// immutable logs every 2 s whenever the stream is down.
export function useTaskLogs(id: string, status: TaskStatus) {
  const started = isTaskStarted(status);
  return useQuery({
    queryKey: ["task-logs", id],
    queryFn: () => fetchJSON<TaskLogs>(`/tasks/${id}/logs`),
    enabled: id.length > 0 && started,
    refetchInterval: started && !isTerminalTask(status) ? 2000 : false,
  });
}

// Registered jobs change only via register/enable/disable mutations, all of
// which invalidate this key — no background polling needed.
export function useRegisteredJobs() {
  return useQuery({
    queryKey: ["registered-jobs"],
    queryFn: () => fetchJSON<Page<RegisteredJobView>>("/registered-jobs"),
    refetchInterval: false,
  });
}

export function useRunJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: RunJobRequest) => postJSON<JobView>("/jobs:run", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useCancelJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (ref: string) => postJSON<JobView>(`/jobs/${encodeURIComponent(ref)}/cancel`),
    onSuccess: (_data, ref) => {
      qc.invalidateQueries({ queryKey: ["jobs"] });
      qc.invalidateQueries({ queryKey: ["job", ref] });
    },
  });
}

export function useRegisterJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: RegisterJobRequest) => postJSON<RegisteredJobView>("/registered-jobs", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["registered-jobs"] }),
  });
}

export function useToggleRegisteredJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      postJSON<RegisteredJobView>(`/registered-jobs/${encodeURIComponent(name)}/${enabled ? "enable" : "disable"}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["registered-jobs"] }),
  });
}
