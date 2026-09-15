// Module-level token store. `fetchJSON`/`postJSON` are plain functions outside
// React, so the access token lives in a module singleton; the refresh token
// persists in localStorage so a page reload can re-establish a session.
import { parseError } from "../api/problem";
import type { MeView, Role, ScopeLevel } from "../api/types";

// Local base + POST helper. We deliberately do NOT route through client.ts's
// `request` (it would recurse: this module IS the 401-refresh path), and we
// keep the prefix local to avoid an import cycle with client.ts.
const API = "/api/v0";
const REFRESH_KEY = "aaiclick.refresh";
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

// The session's principal, kept so the role is *derived* rather than tracked
// as a second piece of state that transitions could forget to update.
let currentMe: MeView | null = null;

// Ordered low to high; the index is the comparison, as in aaiclick/auth/models.py.
export const SCOPE_LEVELS: ScopeLevel[] = ["read", "write", "admin"];

// The role -> scope bridge, mirroring ROLE_SCOPES in aaiclick/auth/models.py.
const ROLE_SCOPES: Record<Role, ScopeLevel> = {
  viewer: "read",
  member: "write",
  admin: "admin",
};

// The signed-in user's role, or null before /auth/me has answered.
export function activeRole(): Role | null {
  return currentMe?.role ?? null;
}

// The highest scope this user may delegate, mirroring _mint_ceiling. The server
// is the authority — this only keeps a form from offering a certain refusal.
export function mintableScopes(): ScopeLevel[] {
  const role = activeRole();
  if (role === null) return [];
  return SCOPE_LEVELS.slice(0, SCOPE_LEVELS.indexOf(ROLE_SCOPES[role]) + 1);
}

// Only an admin may invite; any role may be granted.
export function invitableRoles(): Role[] {
  return activeRole() === "admin" ? ["viewer", "member", "admin"] : [];
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

// Store a freshly minted pair. The access token stays in memory; the refresh
// token persists so a reload can re-establish the session.
async function storePair(res: Response): Promise<void> {
  if (!res.ok) throw await parseError(res);
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
}

// Throws `ApiError`; `error.code === "mfa_required"` means the password was
// accepted and the account needs a second factor.
export async function login(username: string, password: string, totpCode?: string): Promise<void> {
  await storePair(await postAuth("/auth/login", { username, password, totp_code: totpCode ?? null }));
}

export async function tryRefresh(): Promise<boolean> {
  const rt = getRefreshToken();
  if (!rt) return false;
  try {
    await storePair(await postAuth("/auth/refresh", { refresh_token: rt }));
    return true;
  } catch {
    clearSession();
    return false;
  }
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

export async function redeemPasswordReset(token: string, newPassword: string): Promise<void> {
  const res = await postAuth("/auth/password-reset", { token, new_password: newPassword });
  if (!res.ok) throw await parseError(res);
}
