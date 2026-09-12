import { useEffect } from "react";
import { useMutation, useQuery, useQueryClient, type QueryKey } from "@tanstack/react-query";
import type { QueryRows } from "@qv/core";
import { deleteJSON, fetchJSON, postJSON, postText, putJSON } from "./client";
import { isTaskStarted, isTerminalTask } from "../lib/status";
import { jobOfScope } from "../lib/viewer";
import type {
  ApiTokenCreated,
  ApiTokenView,
  AuditEntryView,
  ChangePasswordRequest,
  CreateApiTokenRequest,
  CreateUserRequest,
  Dashboard,
  DashboardResults,
  DashboardSummary,
  Deleted,
  JobDetail,
  JobGraphView,
  JobView,
  MfaSetupView,
  ObjectDetail,
  ObjectQueryRequest,
  ObjectView,
  Page,
  PasswordResetLinkView,
  RegisterJobRequest,
  RegisteredJobView,
  RunJobRequest,
  SavedQuery,
  SavedQueryBody,
  TaskDetail,
  TaskLogs,
  TaskStatus,
  UserView,
} from "./types";

// A mutation that refreshes one query key on success — the pattern behind
// every write in this file.
function useInvalidating<V, R>(queryKey: QueryKey, run: (value: V) => Promise<R>) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: run,
    onSuccess: () => qc.invalidateQueries({ queryKey }),
  });
}

// `poll: false` for callers that only need names (the viewer's scope tree).
export function useJobs({ poll = true }: { poll?: boolean } = {}) {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => fetchJSON<Page<JobView>>("/jobs"),
    refetchInterval: poll ? undefined : false,
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
  const terminal = isTerminalTask(status);
  const qc = useQueryClient();
  const query = useQuery({
    queryKey: ["task-logs", id],
    queryFn: () => fetchJSON<TaskLogs>(`/tasks/${id}/logs`),
    enabled: id.length > 0 && started,
    refetchInterval: started && !terminal ? 2000 : false,
  });

  // Going terminal stops the timer, but whatever the task wrote since the last
  // poll is not on screen yet — and this key is not in LIVE_KEYS, so no
  // `changed` frame will ever fetch it. Without this the panel stays up to one
  // poll interval short of the truth, permanently.
  useEffect(() => {
    if (started && terminal) void qc.invalidateQueries({ queryKey: ["task-logs", id] });
  }, [started, terminal, id, qc]);

  return query;
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

function jobParam(scope: string): string {
  const job = jobOfScope(scope);
  return job ? `?job=${encodeURIComponent(job)}` : "";
}

export function useObjects(scope: string) {
  return useQuery({
    queryKey: ["objects", scope],
    queryFn: () => fetchJSON<Page<ObjectView>>(`/objects${jobParam(scope)}`),
    refetchInterval: false,
  });
}

export function useObject(scope: string, name: string) {
  return useQuery({
    queryKey: ["object", scope, name],
    queryFn: () => fetchJSON<ObjectDetail>(`/objects/${encodeURIComponent(name)}${jobParam(scope)}`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

const PREVIEW_LIMIT = 100;

// `POST /viewer/query` returns ClickHouse's JSONCompact verbatim — the kernel's
// `QueryRows` shape (`meta`, `data`) plus `rows` / `statistics`.
const queryRows = (req: Omit<ObjectQueryRequest, "fmt">) =>
  postJSON<QueryRows>("/viewer/query", { ...req, fmt: "json" });

// The first page of an object's rows — a POST-backed read, cached per object.
export function useObjectRows(scope: string, object: string) {
  return useQuery({
    queryKey: ["object-rows", scope, object],
    queryFn: () => queryRows({ scope, object, limit: PREVIEW_LIMIT, offset: 0 }),
    refetchInterval: false,
  });
}

// The query panel runs on demand (Execute, paging, params): a mutation whose
// result is panel state.
export function useQueryObject() {
  return useMutation({ mutationFn: queryRows });
}

// The same page as `CSVWithNames` text, for download.
export function useQueryObjectCsv() {
  return useMutation({
    mutationFn: (req: Omit<ObjectQueryRequest, "fmt">) => postText("/viewer/query", { ...req, fmt: "csv" }),
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

// --- API tokens ---------------------------------------------------------

export function useApiTokens() {
  return useQuery({
    queryKey: ["api-tokens"],
    queryFn: () => fetchJSON<Page<ApiTokenView>>("/auth/tokens"),
    refetchInterval: false,
  });
}

export function useCreateApiToken() {
  return useInvalidating(["api-tokens"], (req: CreateApiTokenRequest) =>
    postJSON<ApiTokenCreated>("/auth/tokens", req),
  );
}

export function useRevokeApiToken() {
  return useInvalidating(["api-tokens"], (id: string) => deleteJSON<void>(`/auth/tokens/${id}`));
}

// --- account ------------------------------------------------------------

export function useChangePassword() {
  return useMutation({
    mutationFn: (req: ChangePasswordRequest) => putJSON<void>("/auth/me/password", req),
  });
}

export function useMfaSetup() {
  return useMutation({ mutationFn: () => postJSON<MfaSetupView>("/auth/me/mfa/setup") });
}

export function useMfaEnable() {
  return useMutation({ mutationFn: (code: string) => postJSON<void>("/auth/me/mfa/enable", { code }) });
}

export function useMfaDisable() {
  return useMutation({
    mutationFn: (req: { password: string; code: string }) => postJSON<void>("/auth/me/mfa/disable", req),
  });
}

// --- users (superadmin) -------------------------------------------------

export function useUsers() {
  return useQuery({
    queryKey: ["users"],
    queryFn: () => fetchJSON<Page<UserView>>("/users?limit=200"),
    refetchInterval: false,
  });
}

function useUserMutation<V>(run: (v: V) => Promise<UserView>) {
  return useInvalidating(["users"], run);
}

export function useCreateUser() {
  return useUserMutation((req: CreateUserRequest) => postJSON<UserView>("/users", req));
}

export function useSetSuperadmin() {
  return useUserMutation(({ id, superadmin }: { id: string; superadmin: boolean }) =>
    putJSON<UserView>(`/users/${id}/superadmin`, { superadmin }),
  );
}

export function useSetDisabled() {
  return useUserMutation(({ id, disabled }: { id: string; disabled: boolean }) =>
    postJSON<UserView>(`/users/${id}/${disabled ? "disable" : "enable"}`),
  );
}

export function useSetUserPassword() {
  return useUserMutation(({ id, password }: { id: string; password: string }) =>
    putJSON<UserView>(`/users/${id}/password`, { password }),
  );
}

export function useCreateResetLink() {
  return useMutation({ mutationFn: (id: string) => postJSON<PasswordResetLinkView>(`/users/${id}/password-reset`) });
}

export function useResetUserMfa() {
  return useUserMutation((id: string) => postJSON<UserView>(`/users/${id}/mfa/reset`));
}

export function useSetUserEmail() {
  return useUserMutation(({ id, email }: { id: string; email: string | null }) =>
    putJSON<UserView>(`/users/${id}/email`, { email }),
  );
}

// --- audit (superadmin) -------------------------------------------------

export interface AuditQuery {
  username?: string;
  method?: string;
  path?: string;
  limit?: number;
}

export function useAudit(query: AuditQuery) {
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== "") params.set(key, String(value));
  }
  return useQuery({
    queryKey: ["audit", params.toString()],
    queryFn: () => fetchJSON<Page<AuditEntryView>>(`/audit?${params.toString()}`),
    refetchInterval: false,
    // Keep the previous page on screen while a changed filter loads, so typing
    // does not unmount the table on every keystroke.
    placeholderData: (previous) => previous,
  });
}
