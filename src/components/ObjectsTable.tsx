import type { ObjectView } from "../api/types";
import { relativeTime } from "../lib/format";
import { dataPrompt, formatBytes, objectScope } from "../lib/viewer";

export function ObjectsTable({ objects, onPrompt }: { objects: ObjectView[]; onPrompt: (v: string) => void }) {
  if (objects.length === 0) return <p className="sub">No objects in this scope</p>;
  return (
    <table data-testid="objects-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Scope</th>
          <th>Job</th>
          <th>Rows</th>
          <th>Size</th>
          <th>Created</th>
        </tr>
      </thead>
      <tbody>
        {objects.map((o) => (
          <tr key={o.table} className="clickable" onClick={() => onPrompt(dataPrompt(objectScope(o), o.name))}>
            <td>
              <span className="name-link mono">{o.name}</span>
            </td>
            <td>{o.job_id != null ? "job" : "persistent"}</td>
            <td className="mono" title={o.job_id != null ? `#${o.job_id}` : undefined}>
              {o.job_id != null ? (o.job_name ?? `#${o.job_id}`) : "—"}
            </td>
            <td className="mono">{o.row_count ?? "—"}</td>
            <td className="mono">{formatBytes(o.size_bytes)}</td>
            <td>{relativeTime(o.created_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
