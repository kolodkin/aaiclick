function formatSeconds(secs: number): string {
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ${String(secs % 60).padStart(2, "0")}s`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ${String(mins % 60).padStart(2, "0")}m`;
}

export function relativeTime(iso: string | null | undefined): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  const secs = Math.max(0, Math.round((Date.now() - then) / 1000));
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
