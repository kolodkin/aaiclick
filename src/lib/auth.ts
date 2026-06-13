// Module-level token store. `fetchJSON`/`postJSON` are plain functions outside
// React, so the access token lives in a module singleton; the refresh token
// persists in localStorage so a page reload can re-establish a session.

const REFRESH_KEY = "aaiclick.refresh";
let accessToken: string | null = null;

export function getAccessToken(): string | null {
  return accessToken;
}

export function setAccessToken(token: string | null): void {
  accessToken = token;
}

export function getRefreshToken(): string | null {
  return localStorage.getItem(REFRESH_KEY);
}

export function setRefreshToken(token: string | null): void {
  if (token) localStorage.setItem(REFRESH_KEY, token);
  else localStorage.removeItem(REFRESH_KEY);
}

export function clearSession(): void {
  accessToken = null;
  setRefreshToken(null);
}

export interface TokenPair {
  access_token: string;
  refresh_token: string;
  expires_in: number;
}

export interface MeView {
  id: number;
  username: string;
  role: string;
}

export async function login(username: string, password: string): Promise<void> {
  const res = await fetch("/api/v0/auth/login", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  if (!res.ok) throw new Error("login failed");
  const pair = (await res.json()) as TokenPair;
  setAccessToken(pair.access_token);
  setRefreshToken(pair.refresh_token);
}

export async function tryRefresh(): Promise<boolean> {
  const rt = getRefreshToken();
  if (!rt) return false;
  const res = await fetch("/api/v0/auth/refresh", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ refresh_token: rt }),
  });
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
    await fetch("/api/v0/auth/logout", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ refresh_token: rt }),
    }).catch(() => undefined);
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
  let res = await fetch("/api/v0/auth/me", { headers });
  if (res.status === 401 && (await tryRefresh())) {
    res = await fetch("/api/v0/auth/me", {
      headers: { Authorization: `Bearer ${getAccessToken()}` },
    });
  }
  if (!res.ok) return null;
  return (await res.json()) as MeView;
}
