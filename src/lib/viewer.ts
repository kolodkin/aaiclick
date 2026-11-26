// Adapters between aaiclick's wire format and the queryview-core kernel.
import type { Field, OrderCol } from "@qv/core";
import type { ColumnInfo, ObjectView, OrderByPair } from "../api/types";

export const PERSISTENT = "persistent";
export const AAI_ID = "aai_id";

// A scope key: "persistent" or "job:<id|name>".
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

// Last 6 digits of a job or task snowflake — enough to tell reruns apart.
export function shortId(id: string): string {
  return id.slice(-6);
}

// "<job name> #<short id>" when the scope is a job and its name is known, else `scopeLabel`.
export function jobScopeLabel(scope: string, jobName: string | null | undefined): string {
  const id = jobOfScope(scope);
  return id && jobName ? `${jobName} #${shortId(id)}` : scopeLabel(scope);
}

// The scope an object lives in, keyed by job id so reruns stay distinct.
export function objectScope(o: ObjectView): string {
  return o.job_id != null ? scopeKey(String(o.job_id)) : PERSISTENT;
}

// The prompt for a scope's data, or one item of it: `@job <ref> data [<item>]`
// for a job, `@data [<object>]` for the persistent scope.
export function dataPrompt(scope: string, object?: string): string {
  const job = jobOfScope(scope);
  return [job ? `@job ${job} data` : "@data", object ?? ""].filter(Boolean).join(" ");
}

// The backend serialises OrderBy as a [name, dir] pair; the kernel wants objects.
export function orderColsToPairs(cols: OrderCol[]): OrderByPair[] {
  return cols.map((c) => [c.name, c.dir]);
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

// A task input or result inside a job scope (`<task_id>.<kwarg>` or `<task_id>`)
// rather than an object: object names never start with a digit. The server
// validates the rest and answers "not found" for a malformed one.
export function isAddress(name: string): boolean {
  return /^\d/.test(name);
}
