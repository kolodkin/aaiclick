import { useState } from "react";
import type { ObjectView } from "../api/types";
import { relativeTime } from "../lib/format";
import { dataPrompt, formatBytes, objectScope } from "../lib/viewer";

// Whose rows to show: the job's own kwargs and result, the other tasks', or the objects.
type Filter = "job" | "tasks" | "objects" | "all";
const FILTERS: Filter[] = ["job", "tasks", "objects", "all"];

function matches(o: ObjectView, filter: Filter): boolean {
  if (filter === "all") return true;
  if (filter === "objects") return o.kind === "object";
  return o.owner === (filter === "job" ? "job" : "task");
}

// The Value / Target cell of a task row: a value's JSON preview, or a ref's
// type tag and what it points at — a link when the target can be opened.
export function TargetCell({ o, onPrompt }: { o: ObjectView; onPrompt: (v: string) => void }) {
  if (o.kind === "object") return <>—</>;
  return (
    <span className="value-preview">
      {o.ref_type && <span className="io-tag">{o.ref_type}</span>}
      {o.target ? (
        <button
          type="button"
          className="ref-link"
          onClick={(e) => {
            e.stopPropagation();
            onPrompt(dataPrompt(objectScope(o), o.target ?? undefined));
          }}
        >
          → {o.preview}
        </button>
      ) : (
        o.preview
      )}
    </span>
  );
}

export function ObjectsTable({ objects, onPrompt }: { objects: ObjectView[]; onPrompt: (v: string) => void }) {
  const [filter, setFilter] = useState<Filter>("all");
  if (objects.length === 0) return <p className="sub">No objects in this scope</p>;
  // Task rows only come with a job scope, where Scope and Job would repeat one
  // value on every row; they give way to Kind and Value / Target.
  const hasTaskRows = objects.some((o) => o.kind !== "object");
  // The chips are hidden without task rows, so a filter left from a job scope must not apply.
  const shown = hasTaskRows ? objects.filter((o) => matches(o, filter)) : objects;
  return (
    <>
      {hasTaskRows && (
        <div className="chips" role="group" aria-label="Filter rows">
          {FILTERS.map((f) => (
            <button
              key={f}
              type="button"
              className={`chip${filter === f ? " chip-active" : ""}`}
              aria-pressed={filter === f}
              data-testid={`filter-chip-${f}`}
              onClick={() => setFilter(f)}
            >
              {f} {objects.filter((o) => matches(o, f)).length}
            </button>
          ))}
        </div>
      )}
      <table data-testid="objects-table">
        <thead>
          <tr>
            <th>Name</th>
            {hasTaskRows && <th>Kind</th>}
            {hasTaskRows && <th>Value / Target</th>}
            {!hasTaskRows && <th>Scope</th>}
            {!hasTaskRows && <th>Job</th>}
            <th>Rows</th>
            <th>Size</th>
            <th>Created</th>
          </tr>
        </thead>
        <tbody>
          {shown.map((o) => (
            <tr
              key={o.kind === "object" ? o.table : o.name}
              className="clickable"
              onClick={() => onPrompt(dataPrompt(objectScope(o), o.name))}
            >
              <td>
                <span className="name-link mono">{o.label ?? o.name}</span>
                {o.direction && <span className="io-tag">{o.direction}</span>}
              </td>
              {hasTaskRows && (
                <td>
                  <span className={`badge kind-${o.kind}`}>{o.kind}</span>
                </td>
              )}
              {hasTaskRows && (
                <td className="mono">
                  <TargetCell o={o} onPrompt={onPrompt} />
                </td>
              )}
              {!hasTaskRows && <td>{o.job_id != null ? "job" : "persistent"}</td>}
              {!hasTaskRows && (
                <td className="mono" title={o.job_id != null ? `#${o.job_id}` : undefined}>
                  {o.job_id != null ? (o.job_name ?? `#${o.job_id}`) : "—"}
                </td>
              )}
              <td className="mono">{o.row_count ?? "—"}</td>
              <td className="mono">{formatBytes(o.size_bytes)}</td>
              <td>{relativeTime(o.created_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </>
  );
}
