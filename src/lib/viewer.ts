// Adapters between aaiclick's wire format and the queryview-core kernel.
import type { Field, OrderCol } from "@qv/core";
import type { ColumnInfo, OrderByPair } from "../api/types";

export const PERSISTENT = "persistent";
export const AAI_ID = "aai_id";

// A scope key as the backend expects it: "persistent" or "job:<id|name>".
export function scopeKey(job: string | null): string {
  return job ? `job:${job}` : PERSISTENT;
}

// The job reference inside a scope key, or null for the persistent scope.
export function jobOfScope(scope: string): string | null {
  return scope.startsWith("job:") ? scope.slice(4) : null;
}

export function scopeLabel(scope: string): string {
  const job = jobOfScope(scope);
  return job ? `Job ${job}` : "Persistent";
}

// `<verb> [job <ref>] [<object>]` — the `@data` / `@query` prompt for a scope.
function scopedPrompt(verb: "@data" | "@query", scope: string, object?: string): string {
  const job = jobOfScope(scope);
  return [verb, job ? `job ${job}` : "", object ?? ""].filter(Boolean).join(" ");
}

export const dataPrompt = (scope: string, object?: string) => scopedPrompt("@data", scope, object);
export const queryPrompt = (scope: string, object?: string) => scopedPrompt("@query", scope, object);

// The backend serialises OrderBy as a [name, dir] pair; the kernel wants objects.
export function orderColsToPairs(cols: OrderCol[]): OrderByPair[] {
  return cols.map((c) => [c.name, c.dir]);
}

export function pairsToOrderCols(pairs: OrderByPair[] | null | undefined): OrderCol[] {
  return (pairs ?? []).map(([name, dir]) => ({ name, dir }));
}

// The Fields picker's input, from the object's registered schema (aai_id hidden,
// like the server's projection).
export function fieldsFromSchema(columns: Record<string, Pick<ColumnInfo, "type">>): Field[] {
  return Object.entries(columns)
    .filter(([name]) => name !== AAI_ID)
    .map(([name, info]) => ({ name, type: info.type }));
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null) return "—";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return `${value.toFixed(1)} ${units[i]}`;
}
