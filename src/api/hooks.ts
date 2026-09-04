import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { deleteJSON, fetchJSON, postJSON, postNoContent, putJSON } from "./client";
import type {
  ApiTokenCreated,
  ApiTokenView,
  AuditEntryView,
  ChangePasswordRequest,
  CreateApiTokenRequest,
  CreateUserRequest,
  MfaSetupView,
  PasswordResetLinkView,
  JobDetail,
  JobGraphView,
  JobView,
  Page,
  RegisteredJobView,
  RegisterJobRequest,
  RunJobRequest,
  TaskDetail,
  TaskLogs,
  UserView,
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

// --- API tokens ---------------------------------------------------------

export function useApiTokens() {
  return useQuery({
    queryKey: ["api-tokens"],
    queryFn: () => fetchJSON<Page<ApiTokenView>>("/auth/tokens"),
    refetchInterval: false,
  });
}

export function useCreateApiToken() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: CreateApiTokenRequest) => postJSON<ApiTokenCreated>("/auth/tokens", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["api-tokens"] }),
  });
}

export function useRevokeApiToken() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (id: string) => deleteJSON<void>(`/auth/tokens/${id}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["api-tokens"] }),
  });
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
  return useMutation({ mutationFn: (code: string) => postNoContent("/auth/me/mfa/enable", { code }) });
}

export function useMfaDisable() {
  return useMutation({
    mutationFn: (req: { password: string; code: string }) => postNoContent("/auth/me/mfa/disable", req),
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
  const qc = useQueryClient();
  return useMutation({
    mutationFn: run,
    onSuccess: () => qc.invalidateQueries({ queryKey: ["users"] }),
  });
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
  });
}
