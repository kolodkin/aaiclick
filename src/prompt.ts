// Named JobViewMode, not JobView — `src/api/types.ts` already exports JobView
// as the server model.
export type JobViewMode = "table" | "graph";

export type Route =
  | { kind: "home" }
  | { kind: "all" }
  | { kind: "jobs" }
  | { kind: "registered" }
  | { kind: "register"; name: string }
  | { kind: "job"; name: string; view: JobViewMode }
  | { kind: "task"; id: string }
  | { kind: "run-confirm"; name: string }
  | { kind: "run-form"; name: string }
  | { kind: "cancel-confirm"; ref: string }
  | { kind: "tokens" }
  | { kind: "account" }
  | { kind: "users" }
  | { kind: "audit" }
  | { kind: "reset"; token: string }
  | { kind: "data"; job: string | null; object: string | null }
  | { kind: "query"; job: string | null; object: string | null }
  | { kind: "dashboard"; name: string | null }
  | { kind: "unknown"; raw: string };

// `[job <ref>] [<object>]` after `@data` / `@query`.
function parseScoped(rest: string): { job: string | null; object: string | null } {
  const words = rest.split(/\s+/).filter(Boolean);
  let job: string | null = null;
  if (words[0] === "job" && words[1]) {
    job = words[1];
    words.splice(0, 2);
  }
  return { job, object: words[0] ?? null };
}

export function parsePrompt(raw: string): Route {
  const p = raw.trim();
  if (p === "") return { kind: "home" };
  if (p === "@all") return { kind: "all" };
  if (p === "@jobs") return { kind: "jobs" };
  if (p === "@registered") return { kind: "registered" };
  if (p === "@tokens") return { kind: "tokens" };
  if (p === "@account") return { kind: "account" };
  if (p === "@users") return { kind: "users" };
  if (p === "@audit") return { kind: "audit" };
  if (p.startsWith("reset ")) return { kind: "reset", token: p.slice(6).trim() };
  if (p === "register") return { kind: "register", name: "" };
  if (p.startsWith("register ")) return { kind: "register", name: p.slice(9).trim() };
  if (p === "@data" || p.startsWith("@data ")) return { kind: "data", ...parseScoped(p.slice(5)) };
  if (p === "@query" || p.startsWith("@query ")) return { kind: "query", ...parseScoped(p.slice(6)) };
  if (p === "@dashboard") return { kind: "dashboard", name: null };
  if (p.startsWith("@dashboard ")) return { kind: "dashboard", name: p.slice(11).trim() || null };
  if (p.startsWith("@job ")) {
    const rest = p.slice(5).trim();
    if (rest.endsWith(" graph")) {
      return { kind: "job", name: rest.slice(0, -6).trim(), view: "graph" };
    }
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
  return { kind: "unknown", raw: p };
}

// Routes that render without a session — a reset link is followed by someone
// who by definition cannot sign in yet.
export const PUBLIC_ROUTES: ReadonlySet<Route["kind"]> = new Set(["reset"]);

const PARAM = "p";

export function promptFromUrl(): string {
  return new URLSearchParams(window.location.search).get(PARAM) ?? "";
}

export function pushPromptToUrl(prompt: string): void {
  const url = new URL(window.location.href);
  if (prompt) url.searchParams.set(PARAM, prompt);
  else url.searchParams.delete(PARAM);
  window.history.pushState({}, "", url);
}
