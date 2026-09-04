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
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
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

// Carries the Problem code so the form can tell "needs an MFA code" from
// "wrong credentials".
export class LoginError extends Error {
  code: string | null;
  constructor(code: string | null) {
    super("login failed");
    this.code = code;
  }
}

export async function login(username: string, password: string, totpCode?: string): Promise<void> {
  const res = await postAuth("/auth/login", { username, password, totp_code: totpCode ?? null });
  if (!res.ok) {
    let code: string | null = null;
    try {
      code = ((await res.json()) as { code?: string }).code ?? null;
    } catch {
      code = null;
    }
    throw new LoginError(code);
  }
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
}

export interface OidcConfig {
  enabled: boolean;
  label: string;
}

export async function fetchOidcConfig(): Promise<OidcConfig> {
  const res = await fetch(`${API}/auth/oidc/config`);
  if (!res.ok) return { enabled: false, label: "SSO" };
  return (await res.json()) as OidcConfig;
}

// Ask the server for the provider URL (it records the login state), then
// leave the SPA for the identity provider.
export async function startOidcLogin(): Promise<void> {
  const res = await postAuth("/auth/oidc/start", undefined);
  if (!res.ok) throw new Error("SSO start failed");
  const { authorization_url } = (await res.json()) as { authorization_url: string };
  window.location.assign(authorization_url);
}

// The provider redirects back to the site root with ?code=&state=. Trade
// them for a session, then strip the parameters so a reload cannot replay.
export async function completeOidcLogin(code: string, state: string): Promise<boolean> {
  const res = await postAuth("/auth/oidc/callback", { code, state });
  const url = new URL(window.location.href);
  url.searchParams.delete("code");
  url.searchParams.delete("state");
  window.history.replaceState({}, "", url);
  if (!res.ok) return false;
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
  return true;
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
