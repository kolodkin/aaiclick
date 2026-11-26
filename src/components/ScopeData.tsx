// The data of one scope — a job's task values and objects, or the persistent
// objects — and the view of one item of it. See
// docs/designs/ui.md — "Data (`@data [<object>]`)".
import { useEffect, useMemo, useRef, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { columnNames, columnTypes, FieldPickers, ResultsTable, shownColumnIndices, type OrderCol } from "@qv/core";
import { useObject, useObjectRows, useObjects } from "../api/hooks";
import type { ObjectView } from "../api/types";
import { relativeTime } from "../lib/format";
import { dataPrompt, fieldsFromSchema, formatBytes, isAddress, jobScopeLabel, orderColsToPairs } from "../lib/viewer";
import { Chips } from "./Chips";
import { MetaGrid } from "./MetaGrid";
import { ObjectsTable, TargetCell } from "./ObjectsTable";

const DEFAULT_LIMIT = 100;
const MAX_LIMIT = 1000;

// `object` names one item to open instead of the listing. `taskId` keeps only
// that task's inputs and result. `refreshKey` changes when the scope may have
// new rows (a task finished), so the listing refetches then.
interface ScopeDataProps {
  scope: string;
  object: string | null;
  taskId?: string;
  refreshKey?: string | number;
  onPrompt: (v: string) => void;
}

export function ScopeData(props: ScopeDataProps) {
  return (
    // A wide table scrolls inside this box, so the page itself never does.
    <div className="scope-data">
      <ScopeBody {...props} />
    </div>
  );
}

function ScopeBody({ scope, object, taskId, refreshKey, onPrompt }: ScopeDataProps) {
  const objects = useObjects(scope);
  const qc = useQueryClient();
  const seenKey = useRef(refreshKey);
  useEffect(() => {
    if (seenKey.current === refreshKey) return;
    seenKey.current = refreshKey;
    void qc.invalidateQueries({ queryKey: ["objects", scope] });
  }, [qc, scope, refreshKey]);
  const selected = object ? objects.data?.items.find((o) => o.name === object) : undefined;

  if (object && isAddress(object))
    return <ValueView key={`${scope}/${object}`} scope={scope} address={object} view={selected} onPrompt={onPrompt} />;
  if (object)
    return (
      <ObjectExplorer key={`${scope}/${object}`} scope={scope} name={object} view={selected} onPrompt={onPrompt} />
    );
  if (objects.isLoading) return <p className="sub">loading…</p>;
  if (objects.isError || !objects.data) return <p className="err">failed to load objects</p>;
  const { items, total } = objects.data;
  const cut = total != null && total > items.length;
  return (
    <>
      {cut && (
        <p className="sub" data-testid="objects-cut">
          Showing {items.length} of {total} rows
        </p>
      )}
      <ObjectsTable
        // shortcut: a task's rows are filtered from the job's one page; past TASK_VALUES_LIMIT rows the server drops task rows first, so add a task filter to GET /objects then.
        objects={taskId === undefined ? items : items.filter((o) => o.task_id === taskId)}
        filters={taskId === undefined}
        emptyText={taskId === undefined ? undefined : cut ? "Not among the rows shown" : "No inputs or result"}
        onPrompt={onPrompt}
      />
    </>
  );
}

// One task input or result: the header comes from its list row, the stored
// JSON from the same query route as an object's rows, as a single cell.
function ValueView({
  scope,
  address,
  view,
  onPrompt,
}: {
  scope: string;
  address: string;
  view: ObjectView | undefined;
  onPrompt: (v: string) => void;
}) {
  const query = useObjectRows({ scope, object: address, order_by: [], limit: 1, offset: 0 });
  const rows = query.data ?? null;
  const columns = rows ? columnNames(rows) : [];
  const scopeText = jobScopeLabel(scope, view?.job_name);
  return (
    <div data-testid="value-view">
      <div className="detail-head">
        <div className="title-row">
          <h2>
            <span className="mono">{view?.label ?? address}</span>
          </h2>
        </div>
        <MetaGrid
          items={[
            { k: "Kind", v: view ? <span className={`badge kind-${view.kind}`}>{view.kind}</span> : "—" },
            { k: "Task", v: address.split(".")[0], mono: true },
            // From the address, so it is right before the list row loads: only an input has a kwarg part.
            { k: "Direction", v: address.includes(".") ? "input" : "result" },
            { k: "Scope", v: scopeText },
            ...(view?.kind === "ref"
              ? [{ k: "Points to", v: <TargetCell o={view} onPrompt={onPrompt} />, mono: true }]
              : []),
          ]}
        />
      </div>
      <Chips chips={[{ label: `← ${scopeText}`, cmd: dataPrompt(scope) }]} onPrompt={onPrompt} />
      {query.isPending && <p className="sub">loading value…</p>}
      {query.isError && <p className="err">{query.error.message}</p>}
      {rows && (
        <ResultsTable
          columns={columns}
          rows={rows.data}
          types={columnTypes(rows)}
          shownIdx={columns.map((_, i) => i)}
          testid="object-rows"
          className="max-h-[70vh]"
        />
      )}
    </div>
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
      <Chips chips={[{ label: `← ${scopeText}`, cmd: dataPrompt(scope) }]} onPrompt={onPrompt} />
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
          <ResultsTable
            columns={columns}
            rows={rows.data}
            types={columnTypes(rows)}
            shownIdx={shownColumnIndices(columns, fields, visibleCols ?? columns)}
            testid="object-rows"
            dimmed={busy}
            // In the page flow, not a height-bounded column: fit the rows up to a cap.
            className="max-h-[70vh]"
          />
        )}
      </div>
    </>
  );
}
