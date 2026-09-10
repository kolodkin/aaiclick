import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { deleteJSON, fetchJSON, postJSON, putJSON } from "./client";
import type {
  Dashboard,
  DashboardBody,
  DashboardResults,
  DashboardSummary,
  Deleted,
  JobDetail,
  JobGraphView,
  JobView,
  ObjectDetail,
  ObjectQueryRequest,
  ObjectQueryResult,
  ObjectView,
  Page,
  RegisteredJobView,
  RegisterJobRequest,
  RunJobRequest,
  SavedQuery,
  SavedQueryBody,
  TaskDetail,
  TaskLogs,
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

export function useTaskLogs(id: string) {
  return useQuery({
    queryKey: ["task-logs", id],
    queryFn: () => fetchJSON<TaskLogs>(`/tasks/${id}/logs`),
    enabled: id.length > 0,
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

// --- viewer ---------------------------------------------------------------

function objectsPath(scope: string): string {
  const job = scope.startsWith("job:") ? scope.slice(4) : null;
  return job ? `/objects?scope=job&job=${encodeURIComponent(job)}` : "/objects";
}

export function useObjects(scope: string) {
  return useQuery({
    queryKey: ["objects", scope],
    queryFn: () => fetchJSON<Page<ObjectView>>(objectsPath(scope)),
  });
}

export function useObject(name: string) {
  return useQuery({
    queryKey: ["object", name],
    queryFn: () => fetchJSON<ObjectDetail>(`/objects/${encodeURIComponent(name)}`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

// A query is a mutation: it runs on demand (Execute, paging, params) rather
// than polling, and its result is per-panel state.
export function useQueryObject() {
  return useMutation({
    mutationFn: (req: ObjectQueryRequest) => postJSON<ObjectQueryResult>("/viewer/query", req),
  });
}

export function useSavedQueries(scope: string, object?: string) {
  const params = new URLSearchParams({ scope });
  if (object) params.set("object", object);
  return useQuery({
    queryKey: ["saved-queries", scope, object ?? ""],
    queryFn: () => fetchJSON<Page<SavedQuery>>(`/viewer/queries?${params}`),
    refetchInterval: false,
  });
}

export function useSaveQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: SavedQueryBody }) =>
      putJSON<SavedQuery>(`/viewer/queries/${encodeURIComponent(name)}`, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["saved-queries"] }),
  });
}

export function useDeleteSavedQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteJSON<Deleted>(`/viewer/queries/${encodeURIComponent(name)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["saved-queries"] }),
  });
}

export function useDashboards() {
  return useQuery({
    queryKey: ["dashboards"],
    queryFn: () => fetchJSON<Page<DashboardSummary>>("/viewer/dashboards"),
    refetchInterval: false,
  });
}

export function useDashboard(name: string) {
  return useQuery({
    queryKey: ["dashboard", name],
    queryFn: () => fetchJSON<Dashboard>(`/viewer/dashboards/${encodeURIComponent(name)}`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

export function useRunDashboard(name: string) {
  return useQuery({
    queryKey: ["dashboard-results", name],
    queryFn: () => postJSON<DashboardResults>(`/viewer/dashboards/${encodeURIComponent(name)}:run`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

export function useSaveDashboard() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: DashboardBody }) =>
      putJSON<Dashboard>(`/viewer/dashboards/${encodeURIComponent(name)}`, body),
    onSuccess: (_d, { name }) => {
      qc.invalidateQueries({ queryKey: ["dashboards"] });
      qc.invalidateQueries({ queryKey: ["dashboard", name] });
    },
  });
}
