import type { ObjectView } from "../api/types";
import { relativeTime } from "../lib/format";
import { dataPrompt, formatBytes, queryPrompt } from "../lib/viewer";

export function ObjectsTable({
  objects,
  scope,
  onPrompt,
}: {
  objects: ObjectView[];
  scope: string;
  onPrompt: (v: string) => void;
}) {
  if (objects.length === 0) return <p className="sub">No objects in this scope</p>;
  return (
    <table data-testid="objects-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Rows</th>
          <th>Size</th>
          <th>Created</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {objects.map((o) => (
          <tr key={o.table} className="clickable" onClick={() => onPrompt(dataPrompt(scope, o.name))}>
            <td>
              <span className="name-link mono">{o.name}</span>
            </td>
            <td className="mono">{o.row_count ?? "—"}</td>
            <td className="mono">{formatBytes(o.size_bytes)}</td>
            <td>{relativeTime(o.created_at)}</td>
            <td className="row-actions">
              <button
                type="button"
                className="btn btn-sm"
                data-testid="object-query"
                onClick={(e) => {
                  e.stopPropagation();
                  onPrompt(queryPrompt(scope, o.name));
                }}
              >
                Query
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
