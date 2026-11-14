// `<qv-select-view query>`: a paged results table over one of the dashboard's
// queries, with field and order-by pickers and CSV. It never holds SQL: every
// run asks the host by query name. See docs/dashboard.md, "Widgets".

import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { compileCellTemplate } from "../cells/cellTemplate";
import { parseComplexType } from "../cells/complexCells";
import { renderDefaultCell } from "../cells/defaultCell";
import { MAX_LIMIT, type WidgetReply } from "../dashboard/widgetChannel";
import { FieldPickers, type Field, type OrderCol } from "../presentation/FieldPickers";
import { shownColumnIndices } from "../presentation/presentation";
import { ResultsTable } from "../results/ResultsTable";
import { cellText, columnNames, columnTypes, type Cell, type QueryRows } from "../results/rows";
import { onParamsResults, requestHost, tellHost } from "./frameChannel";

type Page = { limit: number; offset: number; orderBy: OrderCol[] };

// The page part of a request, shared by a run and a CSV download.
function pageMessage(type: "widget-query" | "widget-csv", query: string, p: Page) {
  return { type, query, limit: p.limit, offset: p.offset, order_by: p.orderBy };
}

const inputClass = "glass-input ml-1 w-20 px-2 py-1 text-sm";

// A whole number applied on Enter or on leaving the box, not per keystroke:
// each change is a query. Keyed by its value, so paging resets the text.
function NumberBox({
  label,
  testid,
  value,
  min,
  max = Number.MAX_SAFE_INTEGER,
  onCommit,
}: {
  label: string;
  testid: string;
  value: number;
  min: number;
  max?: number;
  onCommit: (n: number) => void;
}) {
  const [text, setText] = useState(String(value));
  function commit() {
    const n = Math.trunc(Number(text));
    const next = Number.isFinite(n) && text.trim() !== "" ? Math.min(max, Math.max(min, n)) : value;
    setText(String(next));
    if (next !== value) onCommit(next);
  }
  return (
    <label className="text-slate-300">
      {label}
      <input
        type="number"
        value={text}
        min={min}
        onChange={(e) => setText(e.target.value)}
        onBlur={commit}
        onKeyDown={(e) => {
          if (e.key === "Enter") commit();
        }}
        data-testid={testid}
        className={inputClass}
      />
    </label>
  );
}

export function SelectView({
  query,
  limit = 100,
  orderBy = [],
  fields: initialFields = [],
  templates = {},
}: {
  query: string;
  limit?: number;
  // The saved presentation; changes made here last for the visit.
  orderBy?: OrderCol[];
  fields?: string[];
  // Column name → the HTML of its <template column>.
  templates?: Record<string, string>;
}) {
  const [page, setPage] = useState<Page>({ limit, offset: 0, orderBy });
  const [result, setResult] = useState<QueryRows | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [fields, setFields] = useState<Field[]>([]);
  const [visibleCols, setVisibleCols] = useState<string[]>([]);
  // Only the newest run lands: a slow reply must not overwrite a later page.
  const runSeq = useRef(0);
  const firstResult = useRef(true);

  const run = useCallback(
    async (p: Page) => {
      const ticket = ++runSeq.current;
      setBusy(true);
      const reply = await requestHost<Extract<WidgetReply, { type: "widget-result" }>>(
        pageMessage("widget-query", query, p),
      );
      if (ticket !== runSeq.current) return;
      setBusy(false);
      if (!reply.ok) {
        setError(reply.message);
        setResult(null);
        return;
      }
      setError(null);
      const rows: QueryRows = { meta: reply.meta, data: reply.data };
      setResult(rows);
      // The `fields` attribute restricts the table to those columns at first.
      if (firstResult.current && initialFields.length) {
        const cols = columnNames(rows);
        setFields(cols.map((name) => ({ name, type: "" })));
        setVisibleCols(initialFields.filter((f) => cols.includes(f)));
      }
      firstResult.current = false;
    },
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [query],
  );

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    void run(page);
  }, [run, page]);

  // A param change or an edited query: back to the first page.
  useEffect(
    () =>
      onParamsResults((queries) => {
        if (!queries || queries.includes(query)) setPage((p) => ({ ...p, offset: 0 }));
      }),
    [query],
  );

  async function describe() {
    const reply = await requestHost<Extract<WidgetReply, { type: "widget-fields" }>>({
      type: "widget-describe",
      query,
    });
    if (!reply.ok) {
      setError(reply.message);
      return;
    }
    const names = reply.fields.map((f) => f.name);
    // A first describe shows every column; a later one keeps the picks.
    setVisibleCols(fields.length ? visibleCols.filter((c) => names.includes(c)) : names);
    setFields(reply.fields);
  }

  const columns = result ? columnNames(result) : [];
  const colTypes = result ? columnTypes(result) : {};
  // Per-render work kept out of the per-cell path: a page holds up to
  // MAX_LIMIT rows.
  const compiled = useMemo(
    () => Object.fromEntries(Object.entries(templates).map(([c, t]) => [c, compileCellTemplate(t)])),
    [templates],
  );
  const complexTypes = useMemo(
    () => Object.fromEntries((result?.meta ?? []).map((m) => [m.name, parseComplexType(m.type)])),
    [result],
  );
  // Each row's cells as text, made once per result (only when a template needs
  // them), not once per templated cell.
  const rowText = useMemo(
    () =>
      new Map<Cell[], string[]>(
        Object.keys(templates).length && result ? result.data.map((row) => [row, row.map(cellText)]) : [],
      ),
    [result, templates],
  );

  return (
    <section
      data-testid="qv-select-view"
      className="flex max-h-[36rem] flex-col gap-2 rounded-xl bg-slate-950 p-3 text-sm text-slate-200"
    >
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          onClick={() => void describe()}
          data-testid="qv-fields"
          className={`px-3 py-1 text-sm font-medium ${fields.length ? "glass-btn-primary" : "glass-btn"}`}
        >
          Fields
        </button>
        <NumberBox
          label="Limit"
          testid="qv-limit"
          key={`l${page.limit}`}
          value={page.limit}
          min={1}
          max={MAX_LIMIT}
          onCommit={(n) => setPage({ ...page, limit: n })}
        />
        <NumberBox
          label="Offset"
          testid="qv-offset"
          key={`o${page.offset}`}
          value={page.offset}
          min={0}
          onCommit={(n) => setPage({ ...page, offset: n })}
        />
        <button
          type="button"
          onClick={() => setPage({ ...page, offset: Math.max(0, page.offset - page.limit) })}
          disabled={busy || page.offset === 0}
          data-testid="qv-prev"
          className="glass-btn px-3 py-1"
        >
          ← Previous
        </button>
        <button
          type="button"
          onClick={() => setPage({ ...page, offset: page.offset + page.limit })}
          disabled={busy}
          data-testid="qv-next"
          className="glass-btn px-3 py-1"
        >
          Next →
        </button>
        <button
          type="button"
          onClick={() => tellHost(pageMessage("widget-csv", query, page))}
          data-testid="qv-csv"
          className="glass-btn px-3 py-1 font-medium text-emerald-300"
        >
          Download CSV
        </button>
      </div>

      {fields.length > 0 && (
        <FieldPickers
          fields={fields}
          visibleCols={visibleCols}
          orderBy={page.orderBy}
          onVisibleColsChange={setVisibleCols}
          onOrderByChange={(next) => setPage({ ...page, orderBy: next })}
        />
      )}

      {error && (
        <p data-testid="qv-error" className="text-red-300">
          {error}
        </p>
      )}
      {result && (
        <ResultsTable
          columns={columns}
          types={colTypes}
          rows={result.data}
          shownIdx={shownColumnIndices(columns, fields, visibleCols)}
          testid="qv-results"
          dimmed={busy}
          renderCell={(col, value, row) =>
            compiled[col] ? (
              <span
                data-testid={`cell-${col}`}
                dangerouslySetInnerHTML={{
                  __html: compiled[col](cellText(value), rowText.get(row) ?? [], columns),
                }}
              />
            ) : (
              renderDefaultCell(col, value, complexTypes[col] ?? null)
            )
          }
        />
      )}
    </section>
  );
}
