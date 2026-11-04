// See docs/designs/ui.md.
import { ALL, PERSISTENT, scopeKey } from "./lib/viewer";

// Named JobViewMode, not JobView — `src/api/types.ts` already exports JobView
// as the server model.
export type JobViewMode = "table" | "graph" | "timings" | "duration";
// A trailing view word after `@job <name>`; shared with the prompt dropdown.
export const JOB_VIEW_SUFFIX = /\s+(graph|timings|duration)$/;

export type Route =
  | { kind: "home" }
  | { kind: "jobs" }
  | { kind: "registered" }
  | { kind: "register"; name: string }
  | { kind: "job"; name: string; view: JobViewMode }
  | { kind: "task"; id: string }
  | { kind: "run-confirm"; name: string }
  | { kind: "run-form"; name: string }
  | { kind: "cancel-confirm"; ref: string }
  | { kind: "toggle-confirm"; name: string; enabled: boolean }
  | { kind: "tokens" }
  | { kind: "account" }
  | { kind: "users" }
  | { kind: "invite" }
  | { kind: "audit" }
  | { kind: "reset"; token: string }
  | { kind: "data"; scope: string; object: string | null }
  | { kind: "query"; scope: string; object: string | null }
  | { kind: "dashboard"; name: string | null }
  | { kind: "sandbox" }
  | { kind: "sandbox-file"; id: string }
  | { kind: "unknown"; raw: string };

// `[persistent | job <ref>] [<object>]` after `@data` / `@query`. Nothing
// means all scopes; a bare object is a persistent one.
function parseScoped(rest: string): { scope: string; object: string | null } {
  const words = rest.split(/\s+/).filter(Boolean);
  if (words[0] === "job" && words[1]) return { scope: scopeKey(words[1]), object: words[2] ?? null };
  if (words[0] === PERSISTENT) words.shift();
  else if (words.length === 0) return { scope: ALL, object: null };
  return { scope: PERSISTENT, object: words[0] ?? null };
}

export function parsePrompt(raw: string): Route {
  const p = raw.trim();
  if (p === "" || p === "help") return { kind: "home" };
  if (p === "@jobs") return { kind: "jobs" };
  if (p === "@registered") return { kind: "registered" };
  if (p === "@tokens") return { kind: "tokens" };
  if (p === "@account") return { kind: "account" };
  if (p === "@users") return { kind: "users" };
  if (p === "@invite") return { kind: "invite" };
  if (p === "@audit") return { kind: "audit" };
  if (p === "@sandbox") return { kind: "sandbox" };
  if (p.startsWith("@sandbox ")) return { kind: "sandbox-file", id: p.slice(9).trim() };
  if (p.startsWith("reset ")) return { kind: "reset", token: p.slice(6).trim() };
  if (p === "register") return { kind: "register", name: "" };
  if (p.startsWith("register ")) return { kind: "register", name: p.slice(9).trim() };
  if (p === "@data" || p.startsWith("@data ")) return { kind: "data", ...parseScoped(p.slice(5)) };
  if (p === "@query" || p.startsWith("@query ")) return { kind: "query", ...parseScoped(p.slice(6)) };
  if (p === "@dashboard") return { kind: "dashboard", name: null };
  if (p.startsWith("@dashboard ")) return { kind: "dashboard", name: p.slice(11).trim() || null };
  if (p.startsWith("@job ")) {
    const rest = p.slice(5).trim();
    const m = JOB_VIEW_SUFFIX.exec(rest);
    if (m && m.index > 0) return { kind: "job", name: rest.slice(0, m.index), view: m[1] as JobViewMode };
    return { kind: "job", name: rest, view: "table" };
  }
  // Ids are 64-bit snowflakes — keep them as opaque strings (parseInt would
  // round past Number.MAX_SAFE_INTEGER and break the lookup).
  if (p.startsWith("@task ")) return { kind: "task", id: p.slice(6).trim() };
  if (p.startsWith("run ")) {
    const rest = p.slice(4).trim();
    if (rest.endsWith("?")) return { kind: "run-form", name: rest.slice(0, -1).trim() };
    return { kind: "run-confirm", name: rest };
  }
  if (p.startsWith("cancel ")) return { kind: "cancel-confirm", ref: p.slice(7).trim() };
  if (p.startsWith("enable ")) return { kind: "toggle-confirm", name: p.slice(7).trim(), enabled: true };
  if (p.startsWith("disable ")) return { kind: "toggle-confirm", name: p.slice(8).trim(), enabled: false };
  return { kind: "unknown", raw: p };
}

// Commands completing what was typed, or all of them when nothing matches.
export function suggestCommands(raw: string, commands: string[]): string[] {
  const hits = commands.filter((c) => c.startsWith(raw));
  return hits.length ? hits : commands;
}

// Routes that render without a session — a reset link is followed by someone
// who by definition cannot sign in yet.
export const PUBLIC_ROUTES: ReadonlySet<Route["kind"]> = new Set(["reset"]);

const PARAM = "p";

export function promptFromUrl(): string {
  return new URLSearchParams(window.location.search).get(PARAM) ?? "";
}

// The app URL for a prompt, so a link can open it in a new tab.
export function promptHref(prompt: string): string {
  return `?${new URLSearchParams({ [PARAM]: prompt })}`;
}

export function pushPromptToUrl(prompt: string): void {
  const url = new URL(window.location.href);
  if (prompt) url.searchParams.set(PARAM, prompt);
  else url.searchParams.delete(PARAM);
  window.history.pushState({}, "", url);
}
