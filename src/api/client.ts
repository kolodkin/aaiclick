import { clearSession, getAccessToken, getActiveTenantId, tryRefresh } from "../lib/auth";
import { ApiError, parseError } from "./problem";

export const API = "/api/v0";

export { ApiError };

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

// The one place a verb's request shape and error handling live; callers below
// only choose how to decode the body.
async function send(method: string, path: string, body?: unknown): Promise<Response> {
  const res = await request(path, {
    method,
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  return res;
}

// `T` is `void` at the call sites whose route answers 204 (no body to decode).
async function sendJSON<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await send(method, path, body);
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export async function postText(path: string, body: unknown): Promise<string> {
  return (await send("POST", path, body)).text();
}

export const fetchJSON = <T>(path: string) => sendJSON<T>("GET", path);
export const postJSON = <T>(path: string, body?: unknown) => sendJSON<T>("POST", path, body);
export const putJSON = <T>(path: string, body: unknown) => sendJSON<T>("PUT", path, body);
export const deleteJSON = <T>(path: string) => sendJSON<T>("DELETE", path);

// Open a long-lived response (server-sent events) through the same auth
// chokepoint. The caller reads `res.body`; `signal` aborts the connection.
export function openStream(path: string, signal: AbortSignal): Promise<Response> {
  return request(path, { signal, headers: { Accept: "text/event-stream" } });
}
