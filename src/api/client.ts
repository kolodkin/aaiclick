import { clearSession, getAccessToken, getActiveTenantId, tryRefresh } from "../lib/auth";
import type { Problem } from "./types";

export const API = "/api/v0";

export class ApiError extends Error {
  status: number;
  problem: Problem | null;
  constructor(status: number, problem: Problem | null, message: string) {
    super(message);
    this.status = status;
    this.problem = problem;
  }
}

async function parseError(res: Response): Promise<ApiError> {
  let problem: Problem | null = null;
  try {
    problem = (await res.json()) as Problem;
  } catch {
    problem = null;
  }
  const detail = problem?.detail ?? problem?.title ?? res.statusText;
  return new ApiError(res.status, problem, detail);
}

function authHeaders(extra?: HeadersInit): Record<string, string> {
  const headers: Record<string, string> = { ...(extra as Record<string, string>) };
  const token = getAccessToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  const tenantId = getActiveTenantId();
  if (tenantId) headers["X-Tenant-Id"] = tenantId;
  return headers;
}

// Single chokepoint: attach the bearer token, and on a 401 try one silent
// refresh + retry. If still unauthorized, clear the session and signal the
// AuthProvider to drop back to the login screen.
async function request(path: string, init: RequestInit = {}): Promise<Response> {
  let res = await fetch(`${API}${path}`, { ...init, headers: authHeaders(init.headers) });
  if (res.status === 401 && (await tryRefresh())) {
    res = await fetch(`${API}${path}`, { ...init, headers: authHeaders(init.headers) });
  }
  if (res.status === 401) {
    clearSession();
    window.dispatchEvent(new Event("aaiclick:unauthorized"));
  }
  return res;
}

export async function fetchJSON<T>(path: string): Promise<T> {
  const res = await request(path);
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

export async function postJSON<T>(path: string, body?: unknown): Promise<T> {
  const res = await request(path, {
    method: "POST",
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

// Mutations that answer 204 (no body) or a JSON body — `T` is `void` for the
// former. Shared by PUT / DELETE / body-less POST call sites.
async function send<T>(path: string, method: string, body?: unknown): Promise<T> {
  const res = await request(path, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export function putJSON<T>(path: string, body?: unknown): Promise<T> {
  return send<T>(path, "PUT", body);
}

export function deleteJSON<T>(path: string, body?: unknown): Promise<T> {
  return send<T>(path, "DELETE", body);
}

export function postNoContent(path: string, body?: unknown): Promise<void> {
  return send<void>(path, "POST", body);
}
