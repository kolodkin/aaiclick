import { useMemo } from "react";
import { useObject, useObjects } from "../api/hooks";
import { Chips } from "../components/Chips";
import { QueryPanel } from "../components/QueryPanel";
import { ScopeTree } from "../components/ScopeTree";
import { dataPrompt, fieldsFromSchema, queryPrompt, scopeKey, scopeLabel } from "../lib/viewer";

export function Query({
  job,
  object,
  onPrompt,
}: {
  job: string | null;
  object: string | null;
  onPrompt: (v: string) => void;
}) {
  const scope = scopeKey(job);
  const objects = useObjects(scope);
  const detail = useObject(scope, object ?? "");
  const fields = useMemo(
    () => (detail.data ? fieldsFromSchema(detail.data.table_schema.columns) : []),
    [detail.data],
  );

  return (
    <>
      <h2>Query</h2>
      <p className="sub">Filter, order, and page through one object · save as a named query</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@data", cmd: dataPrompt(scope, object ?? undefined) },
          { label: "@dashboard", cmd: "@dashboard" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="viewer-layout">
        <ScopeTree scope={scope} onScope={(s) => onPrompt(queryPrompt(s))} />
        <div className="viewer-main">
          <div className="field inline">
            <label htmlFor="query-object">Object</label>
            <select
              id="query-object"
              data-testid="query-object"
              value={object ?? ""}
              onChange={(e) => onPrompt(queryPrompt(scope, e.target.value || undefined))}
            >
              <option value="">Pick an object in {scopeLabel(scope)}…</option>
              {objects.data?.items.map((o) => (
                <option key={o.table} value={o.name}>
                  {o.name}
                </option>
              ))}
            </select>
          </div>
          {/* Mounted once the schema is known (so its state seeds from the real
              field list) and keyed so another object remounts it fresh. */}
          {object && !detail.isPending && (
            <QueryPanel key={`${scope}/${object}`} scope={scope} object={object} fields={fields} />
          )}
        </div>
      </div>
    </>
  );
}
