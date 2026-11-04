// See docs/designs/ui.md — "Query (`@query [persistent | job <ref>] [<object>]`)".
import { useMemo } from "react";
import { useObject, useObjects } from "../api/hooks";
import { Chips } from "../components/Chips";
import { LiveStatus } from "../components/LiveStatus";
import { QueryPanel } from "../components/QueryPanel";
import { ScopeTree } from "../components/ScopeTree";
import { dataPrompt, fieldsFromSchema, objectScope, objectScopeLabel, queryPrompt, scopeLabel } from "../lib/viewer";

export function Query({
  scope,
  object,
  onPrompt,
}: {
  scope: string;
  object: string | null;
  onPrompt: (v: string) => void;
}) {
  const objects = useObjects(scope);
  const detail = useObject(scope, object ?? "");
  const items = objects.data?.items ?? [];
  const fields = useMemo(() => (detail.data ? fieldsFromSchema(detail.data.table_schema.columns) : []), [detail.data]);

  return (
    <>
      <h2>Query</h2>
      <p className="sub">
        Filter, order, and page through one object · save as a named query ·{" "}
        <LiveStatus updatedAt={objects.dataUpdatedAt} queryKey="objects" />
      </p>
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
              value={items.find((o) => o.name === object)?.table ?? ""}
              onChange={(e) => {
                // Values are table names: unique across scopes, unlike object names.
                const o = items.find((i) => i.table === e.target.value);
                onPrompt(o ? queryPrompt(objectScope(o), o.name) : queryPrompt(scope));
              }}
            >
              <option value="">Pick an object in {scopeLabel(scope)}…</option>
              {items.map((o) => (
                <option key={o.table} value={o.table}>
                  {o.name} ({objectScopeLabel(o)})
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
