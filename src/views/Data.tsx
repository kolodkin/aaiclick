// See docs/designs/ui.md — "Data (`@data [persistent | job <ref>] [<object>]`)".
import { useMemo, useState } from "react";
import { columnNames, columnTypes, FieldPickers, ResultsTable, shownColumnIndices, type OrderCol } from "@qv/core";
import { useObject, useObjectRows, useObjects } from "../api/hooks";
import type { ObjectView } from "../api/types";
import { Chips } from "../components/Chips";
import { LiveStatus } from "../components/LiveStatus";
import { MetaGrid } from "../components/MetaGrid";
import { ObjectsTable } from "../components/ObjectsTable";
import { ScopeTree } from "../components/ScopeTree";
import { relativeTime } from "../lib/format";
import { dataPrompt, fieldsFromSchema, formatBytes, jobOfScope, jobScopeLabel, orderColsToPairs } from "../lib/viewer";

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

export function Data({
  scope,
  object,
  onPrompt,
}: {
  scope: string;
  object: string | null;
  onPrompt: (v: string) => void;
}) {
  const objects = useObjects(scope);
  const selected = object ? objects.data?.items.find((o) => o.name === object) : undefined;

  return (
    <>
      <h2>Data</h2>
      <p className="sub">
        Objects of the selected scope · click a row to browse it ·{" "}
        <LiveStatus updatedAt={objects.dataUpdatedAt} queryKey="objects" />
      </p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@dashboard", cmd: "@dashboard" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="viewer-layout">
        <ScopeTree scope={scope} onScope={(s) => onPrompt(dataPrompt(s))} />
        <div className="viewer-main">
          {objects.isLoading && <p className="sub">loading…</p>}
          {objects.isError && <p className="err">failed to load objects</p>}
          {object ? (
            <ObjectExplorer
              key={`${scope}/${object}`}
              scope={scope}
              name={object}
              view={selected}
              onPrompt={onPrompt}
            />
          ) : (
            objects.data && <ObjectsTable objects={objects.data.items} onPrompt={onPrompt} />
          )}
        </div>
      </div>
    </>
  );
}

// The explorer for one object: field visibility is client-side, order-by and
// paging re-run the page on the server.
function ObjectExplorer({
  scope,
  name,
  view,
  onPrompt,
}: {
  scope: string;
  name: string;
  view: ObjectView | undefined;
  onPrompt: (v: string) => void;
}) {
  const detail = useObject(scope, name);
  const fields = useMemo(() => (detail.data ? fieldsFromSchema(detail.data.table_schema.columns) : []), [detail.data]);
  // null until the user picks: every field shows.
  const [visibleCols, setVisibleCols] = useState<string[] | null>(null);
  const [orderBy, setOrderBy] = useState<OrderCol[]>([]);
  const [limit, setLimit] = useState(DEFAULT_LIMIT);
  const [limitDraft, setLimitDraft] = useState(String(DEFAULT_LIMIT));
  const [offset, setOffset] = useState(0);
  const query = useObjectRows({ scope, object: name, order_by: orderColsToPairs(orderBy), limit, offset });
  const rows = query.data ?? null;
  const columns = rows ? columnNames(rows) : [];
  const busy = query.isFetching;
  const scopeText = jobScopeLabel(scope, view?.job_name);
  // By id, not name: a name resolves to the latest rerun, and `view` may still be loading.
  const job = jobOfScope(scope);

  function applyLimit() {
    const next = Math.min(MAX_LIMIT, Math.max(1, Math.floor(Number(limitDraft)) || DEFAULT_LIMIT));
    setLimitDraft(String(next));
    if (next !== limit) {
      setLimit(next);
      setOffset(0);
    }
  }

  return (
    <>
      <div className="detail-head">
        <div className="title-row">
          <h2>
            <span className="mono">{name}</span>
          </h2>
        </div>
        <MetaGrid
          items={[
            { k: "Scope", v: scopeText },
            { k: "Rows", v: view?.row_count ?? "—", mono: true },
            { k: "Size", v: formatBytes(view?.size_bytes) },
            { k: "Created", v: relativeTime(view?.created_at) },
          ]}
        />
      </div>
      <Chips
        chips={[
          { label: `← ${scopeText}`, cmd: job ? `@job ${job}` : dataPrompt(scope) },
          ...(job ? [{ label: "@data", cmd: dataPrompt(scope) }] : []),
        ]}
        onPrompt={onPrompt}
      />
      <div className="flex flex-col gap-3">
        <div className="flex flex-wrap items-center gap-2 text-sm">
          <button
            type="button"
            className="btn btn-sm"
            data-testid="object-refresh"
            aria-label="Refresh rows"
            title="Refresh rows"
            disabled={busy}
            onClick={() => query.refetch()}
          >
            ↻
          </button>
          <label>
            Limit{" "}
            <input
              type="number"
              min={1}
              max={MAX_LIMIT}
              value={limitDraft}
              data-testid="object-limit"
              className="glass-input w-20 px-2 py-1"
              onChange={(e) => setLimitDraft(e.target.value)}
              onBlur={applyLimit}
              onKeyDown={(e) => e.key === "Enter" && applyLimit()}
            />
          </label>
          <button
            type="button"
            className="btn btn-sm"
            data-testid="object-prev"
            disabled={busy || offset === 0}
            onClick={() => setOffset(Math.max(0, offset - limit))}
          >
            ← Previous
          </button>
          <button
            type="button"
            className="btn btn-sm"
            data-testid="object-next"
            disabled={busy || (rows !== null && rows.data.length < limit)}
            onClick={() => setOffset(offset + limit)}
          >
            Next →
          </button>
          <span className="text-xs text-slate-400" data-testid="object-offset">
            offset {offset}
          </span>
        </div>
        {fields.length > 0 && (
          <FieldPickers
            fields={fields}
            visibleCols={visibleCols ?? fields.map((f) => f.name)}
            orderBy={orderBy}
            onVisibleColsChange={setVisibleCols}
            onOrderByChange={(next) => {
              setOrderBy(next);
              setOffset(0);
            }}
          />
        )}
        {query.isPending && <p className="sub">loading rows…</p>}
        {query.isError && <p className="err">{query.error.message}</p>}
        {rows && (
          <div className="object-rows">
            <ResultsTable
              columns={columns}
              rows={rows.data}
              types={columnTypes(rows)}
              shownIdx={shownColumnIndices(columns, fields, visibleCols ?? columns)}
              testid="object-rows"
              dimmed={busy}
            />
          </div>
        )}
      </div>
    </>
  );
}
