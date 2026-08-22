// Module-level token store. `fetchJSON`/`postJSON` are plain functions outside
// React, so the access token lives in a module singleton; the refresh token
// persists in localStorage so a page reload can re-establish a session.
import type { MeView } from "../api/types";

// Local base + POST helper. We deliberately do NOT route through client.ts's
// `request` (it would recurse: this module IS the 401-refresh path), and we
// keep the prefix local to avoid an import cycle with client.ts.
const API = "/api/v0";
const REFRESH_KEY = "aaiclick.refresh";
// Mirrors DEFAULT_TENANT_ID in aaiclick/tenancy.py.
const DEFAULT_TENANT_ID = 1;
let accessToken: string | null = null;

function postAuth(path: string, body: unknown): Promise<Response> {
  return fetch(`${API}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function getAccessToken(): string | null {
  return accessToken;
}

// The session's principal, kept so the active tenant is *derived* rather than
// tracked as a second piece of state that transitions could forget to update.
let currentMe: MeView | null = null;

// Active tenant sent as X-Tenant-Id by client.ts. Until the tenant switcher
// lands (tenant RBAC phase 3) it is the user's first membership, else the
// default tenant for superadmins / local mode (docs/designs/tenant_rbac.md).
export function getActiveTenantId(): string | null {
  if (currentMe === null) return null;
  return String(currentMe.tenants[0]?.tenant_id ?? DEFAULT_TENANT_ID);
}

function setAccessToken(token: string | null): void {
  accessToken = token;
}

function getRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_KEY);
}

function setRefreshToken(token: string | null): void {
  if (token) localStorage.setItem(REFRESH_KEY, token);
  else localStorage.removeItem(REFRESH_KEY);
}

export type { MeView };

export function clearSession(): void {
  accessToken = null;
  currentMe = null;
  setRefreshToken(null);
}

interface TokenPair {
  access_token: string;
  refresh_token: string;
  expires_in: number;
}

export async function login(username: string, password: string): Promise<void> {
  const res = await postAuth("/auth/login", { username, password });
  if (!res.ok) throw new Error("login failed");
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
}

export async function tryRefresh(): Promise<boolean> {
  const rt = getRefreshToken();
  if (!rt) return false;
  const res = await postAuth("/auth/refresh", { refresh_token: rt });
  if (!res.ok) {
    clearSession();
    return false;
  }
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
  return true;
}

export async function logout(): Promise<void> {
  const rt = getRefreshToken();
  if (rt) {
    await postAuth("/auth/logout", { refresh_token: rt }).catch(() => undefined);
  }
  clearSession();
}

// Resolve the current session. Returns the principal when authenticated (or
// when the server has auth disabled → synthetic admin), else null. Tries a
// refresh once if the access token is missing/expired.
export async function fetchMe(): Promise<MeView | null> {
  const headers: Record<string, string> = {};
  const token = getAccessToken();
  if (token) headers.Authorization = `Bearer ${token}`;
  let res = await fetch(`${API}/auth/me`, { headers });
  if (res.status === 401 && (await tryRefresh())) {
    res = await fetch(`${API}/auth/me`, {
      headers: { Authorization: `Bearer ${getAccessToken()}` },
    });
  }
  if (!res.ok) {
    currentMe = null;
    return null;
  }
  currentMe = (await res.json()) as MeView;
  return currentMe;
}
