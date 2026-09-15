function formatSeconds(secs: number): string {
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ${String(secs % 60).padStart(2, "0")}s`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ${String(mins % 60).padStart(2, "0")}m`;
}

// `at` is an ISO string or epoch ms; `now` lets a caller that re-renders on a
// timer pass the clock reading that render used instead of reading it again.
export function relativeTime(at: string | number | null | undefined, now: number = Date.now()): string {
  if (!at) return "—";
  const then = typeof at === "number" ? at : new Date(at).getTime();
  const secs = Math.max(0, Math.round((now - then) / 1000));
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.round(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

export function durationBetween(start: string | null | undefined, end: string | null | undefined): string {
  if (!start) return "—";
  const from = new Date(start).getTime();
  const to = end ? new Date(end).getTime() : Date.now();
  return formatSeconds(Math.max(0, Math.round((to - from) / 1000)));
}

export function durationMs(ms: number | null | undefined): string {
  if (ms == null) return "—";
  return formatSeconds(Math.max(0, Math.round(ms / 1000)));
}

/**
 * Last segment of a fully-qualified entrypoint — the function name.
 *
 * Mirrors `_short_entrypoint` in `aaiclick/orchestration/view_models.py`, which
 * `TaskStatsView` uses server-side.
 */
export function shortEntrypoint(entrypoint: string): string {
  if (entrypoint.includes(":")) return entrypoint.split(":").pop() ?? entrypoint;
  if (entrypoint.includes(".")) return entrypoint.split(".").pop() ?? entrypoint;
  return entrypoint;
}
