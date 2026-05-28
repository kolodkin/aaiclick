import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJSON, postJSON } from "./client";
import type {
  JobDetail,
  JobView,
  Page,
  RegisteredJobView,
  RegisterJobRequest,
  RunJobRequest,
  TaskDetail,
  TaskLogs,
} from "./types";

const POLL = 2000;

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => fetchJSON<Page<JobView>>("/jobs"),
    refetchInterval: POLL,
  });
}

export function useJob(ref: string) {
  return useQuery({
    queryKey: ["job", ref],
    queryFn: () => fetchJSON<JobDetail>(`/jobs/${encodeURIComponent(ref)}`),
    refetchInterval: POLL,
    enabled: ref.length > 0,
  });
}

export function useTask(id: number) {
  return useQuery({
    queryKey: ["task", id],
    queryFn: () => fetchJSON<TaskDetail>(`/tasks/${id}`),
    refetchInterval: POLL,
    enabled: Number.isFinite(id),
  });
}

export function useTaskLogs(id: number) {
  return useQuery({
    queryKey: ["task-logs", id],
    queryFn: () => fetchJSON<TaskLogs>(`/tasks/${id}/logs`),
    refetchInterval: POLL,
    enabled: Number.isFinite(id),
  });
}

export function useRegisteredJobs() {
  return useQuery({
    queryKey: ["registered-jobs"],
    queryFn: () => fetchJSON<Page<RegisteredJobView>>("/registered-jobs"),
    refetchInterval: POLL,
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
