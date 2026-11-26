// See docs/designs/ui.md.
import { PERSISTENT } from "./lib/viewer";

// Named JobViewMode, not JobView — `src/api/types.ts` already exports JobView
// as the server model.
export type JobViewMode = "table" | "graph" | "timings" | "duration" | "data";
export type TaskViewMode = "log" | "data";
// A trailing view word in a `@job` draft; the prompt dropdown strips it to get the typed name.
export const JOB_VIEW_SUFFIX = /\s+(graph|timings|duration|data)$/;
// `<ref> [graph | timings | duration | data [<item>] | task <name>]` after
// `@job`. The UI links by id; a typed name stands for the latest run, and
// `task <name>` for its latest task of that name. Words past the last
// argument are ignored, as after `@data <object>`.
const JOB_PROMPT = /^(\S+)(?:\s+(graph|timings|duration|data|task)(?:\s+(\S+))?)?(?:\s|$)/;

export type Route =
  | { kind: "home" }
  | { kind: "jobs" }
  | { kind: "registered" }
  | { kind: "register"; name: string }
  | { kind: "job"; ref: string; view: JobViewMode; item: string | null }
  | { kind: "job-task"; job: string; task: string }
  | { kind: "task"; id: string; view: TaskViewMode }
  | { kind: "run-confirm"; name: string }
  | { kind: "run-form"; name: string }
  | { kind: "cancel-confirm"; ref: string }
  | { kind: "toggle-confirm"; name: string; enabled: boolean }
  | { kind: "unregister-confirm"; name: string }
  | { kind: "tokens" }
  | { kind: "account" }
  | { kind: "users" }
  | { kind: "invite" }
  | { kind: "audit" }
  | { kind: "reset"; token: string }
  | { kind: "data"; object: string | null }
  | { kind: "dashboard"; name: string | null }
  | { kind: "sandbox" }
  | { kind: "sandbox-file"; id: string }
  | { kind: "unknown"; raw: string };

// `[persistent] [<object>]` after `@data`: the object, if one is named.
function parseObject(rest: string): string | null {
  const words = rest.split(/\s+/).filter(Boolean);
  if (words[0] === PERSISTENT) words.shift();
  return words[0] ?? null;
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
  if (p === "@data" || p.startsWith("@data ")) return { kind: "data", object: parseObject(p.slice(5)) };
  if (p === "@dashboard") return { kind: "dashboard", name: null };
  if (p.startsWith("@dashboard ")) return { kind: "dashboard", name: p.slice(11).trim() || null };
  if (p.startsWith("@job ")) {
    const m = JOB_PROMPT.exec(p.slice(5).trim());
    if (!m) return { kind: "unknown", raw: p };
    const [, ref, view = "table", arg] = m;
    if (view === "task") return arg ? { kind: "job-task", job: ref, task: arg } : { kind: "unknown", raw: p };
    return { kind: "job", ref, view: view as JobViewMode, item: view === "data" ? (arg ?? null) : null };
  }
  // Ids are 64-bit snowflakes — keep them as opaque strings (parseInt would
  // round past Number.MAX_SAFE_INTEGER and break the lookup).
  if (p.startsWith("@task ")) {
    const [id, view] = p.slice(6).trim().split(/\s+/);
    return { kind: "task", id, view: view === "data" ? "data" : "log" };
  }
  if (p.startsWith("run ")) {
    const rest = p.slice(4).trim();
    if (rest.endsWith("?")) return { kind: "run-form", name: rest.slice(0, -1).trim() };
    return { kind: "run-confirm", name: rest };
  }
  if (p.startsWith("cancel ")) return { kind: "cancel-confirm", ref: p.slice(7).trim() };
  if (p.startsWith("enable ")) return { kind: "toggle-confirm", name: p.slice(7).trim(), enabled: true };
  if (p.startsWith("disable ")) return { kind: "toggle-confirm", name: p.slice(8).trim(), enabled: false };
  if (p.startsWith("unregister ")) return { kind: "unregister-confirm", name: p.slice("unregister ".length).trim() };
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
