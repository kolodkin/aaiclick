import { useEffect, useMemo, useState } from "react";
import {
  applyParams,
  CellViewModal,
  columnNames,
  columnTypes,
  FieldPickers,
  parseCellViewYaml,
  parseQueryParams,
  presentationForSave,
  renderCell,
  ResultsTable,
  shownColumnIndices,
  type CellViewMap,
  type Field,
  type OrderCol,
  type ParamDef,
  type QueryRows,
} from "@qv/core";
import { useDeleteSavedQuery, useQueryObject, useSavedQueries, useSaveQuery } from "../api/hooks";
import type { ObjectQueryRequest, SavedQuery } from "../api/types";
import { orderColsToPairs, pairsToOrderCols, rowsFromResult } from "../lib/viewer";
import { useToast } from "./Toast";

const NEW_NAME = "::new::";
const MAX_LIMIT = 1000;

function downloadText(filename: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/csv" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// Static `options` params only: `options_sql` needs free SQL, which aaiclick
// does not expose, so such specs are dropped.
function staticParams(cellView: string): ParamDef[] {
  return parseQueryParams(cellView)
    .filter((s) => s.options && s.options.length > 0)
    .map((s) => ({ name: s.name, options: s.options ?? [] }));
}

export function QueryPanel({ scope, object, fields }: { scope: string; object: string; fields: Field[] }) {
  const toast = useToast();
  const run = useQueryObject();
  const saved = useSavedQueries(scope, object);
  const save = useSaveQuery();
  const remove = useDeleteSavedQuery();

  const [where, setWhere] = useState("");
  const [limit, setLimit] = useState(100);
  const [offset, setOffset] = useState(0);
  const [visibleCols, setVisibleCols] = useState<string[]>(() => fields.map((f) => f.name));
  const [orderBy, setOrderBy] = useState<OrderCol[]>([]);
  const [selectedName, setSelectedName] = useState("");
  const [cellView, setCellView] = useState("");
  const [modalOpen, setModalOpen] = useState(false);
  const [paramValues, setParamValues] = useState<Record<string, string>>({});
  const [rows, setRows] = useState<QueryRows | null>(null);
  const [error, setError] = useState<string | null>(null);

  const views = useMemo<CellViewMap>(() => parseCellViewYaml(cellView), [cellView]);
  const paramDefs = useMemo(() => staticParams(cellView), [cellView]);
  const colTypes = useMemo(() => (rows ? columnTypes(rows) : {}), [rows]);

  // Seed each param to its first option, keeping a still-valid pick.
  useEffect(() => {
    setParamValues((prev) => {
      const next: Record<string, string> = {};
      for (const d of paramDefs) next[d.name] = d.options.includes(prev[d.name]) ? prev[d.name] : d.options[0];
      return next;
    });
  }, [paramDefs]);

  // The saved shape of the pickers: an empty selection means "all" (null) on the wire.
  function presentation() {
    const { fields: f, order_by } = presentationForSave(orderBy, visibleCols);
    return { fields: f, order_by: orderColsToPairs(order_by ?? []) };
  }

  const isSaved = (name: string) => saved.data?.items.some((s) => s.name === name) ?? false;

  function request(nextOffset: number, params = paramValues): ObjectQueryRequest {
    return {
      scope,
      object,
      where: where.trim() ? applyParams(where, paramDefs, params) : null,
      ...presentation(),
      limit,
      offset: nextOffset,
      fmt: "json",
    };
  }

  function execute(nextOffset: number, params?: Record<string, string>) {
    setError(null);
    run.mutate(request(nextOffset, params), {
      onSuccess: (data) => {
        setRows(rowsFromResult(data));
        setOffset(nextOffset);
      },
      onError: (e) => setError(e.message),
    });
  }

  function downloadCsv() {
    run.mutate(
      { ...request(offset), fmt: "csv" },
      {
        onSuccess: (data) => downloadText(`${object}.csv`, data.text ?? ""),
        onError: (e) => setError(e.message),
      },
    );
  }

  function applySaved(q: SavedQuery) {
    setWhere(q.where ?? "");
    setOrderBy(pairsToOrderCols(q.order_by));
    setVisibleCols(q.fields?.length ? q.fields : fields.map((f) => f.name));
    setCellView(q.cell_view ?? "");
  }

  function onSelectName(value: string) {
    if (value === NEW_NAME) {
      const name = window.prompt("Save query as (name):", selectedName)?.trim();
      if (name) setSelectedName(name);
      return;
    }
    setSelectedName(value);
    const q = saved.data?.items.find((s) => s.name === value);
    if (q) applySaved(q);
  }

  function persist(cellViewValue = cellView) {
    const name = selectedName.trim();
    if (!name) return;
    save.mutate(
      {
        name,
        body: {
          name,
          scope,
          object,
          where: where.trim() || null,
          ...presentation(),
          cell_view: cellViewValue || null,
        },
      },
      {
        onSuccess: () => {
          setCellView(cellViewValue);
          setModalOpen(false);
          toast(`Saved query '${name}'`);
        },
        onError: (e) => setError(e.message),
      },
    );
  }

  function deleteSaved() {
    const name = selectedName.trim();
    if (!name || !isSaved(name)) return;
    remove.mutate(name, {
      onSuccess: () => {
        setSelectedName("");
        toast(`Deleted query '${name}'`);
      },
      onError: (e) => setError(e.message),
    });
  }

  const columns = rows ? columnNames(rows) : [];
  const shownIdx = shownColumnIndices(columns, fields, visibleCols);
  const busy = run.isPending || save.isPending;

  return (
    <section className="panel viewer-panel" data-testid="query-panel">
      <div className="field inline">
        <select
          data-testid="query-saved-select"
          aria-label="Saved queries"
          value={selectedName}
          onChange={(e) => onSelectName(e.target.value)}
        >
          <option value="">Saved queries…</option>
          <option value={NEW_NAME}>+ New name…</option>
          {selectedName !== "" && !isSaved(selectedName) && <option value={selectedName}>{selectedName}</option>}
          {saved.data?.items.map((s) => (
            <option key={s.name} value={s.name}>
              {s.name}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="query-save"
          disabled={busy || !selectedName.trim()}
          onClick={() => persist()}
        >
          Save
        </button>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="query-delete"
          disabled={busy || !selectedName.trim()}
          onClick={deleteSaved}
        >
          Delete
        </button>
        <button type="button" className="btn btn-sm" data-testid="cell-view-toggle" onClick={() => setModalOpen(true)}>
          Cell view
        </button>
      </div>

      {paramDefs.length > 0 && (
        <div className="field inline" data-testid="query-params">
          {paramDefs.map((def) => (
            <label key={def.name}>
              {def.name}
              <select
                data-testid="param-select"
                data-param={def.name}
                value={paramValues[def.name] ?? def.options[0]}
                onChange={(e) => {
                  const next = { ...paramValues, [def.name]: e.target.value };
                  setParamValues(next);
                  execute(0, next);
                }}
              >
                {def.options.map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            </label>
          ))}
        </div>
      )}

      <div className="field">
        <label htmlFor="query-where">
          where{" "}
          <span className="help">SQL boolean expression over the object's columns; {"{name}"} substitutes a param</span>
        </label>
        <textarea
          id="query-where"
          data-testid="query-where"
          value={where}
          onChange={(e) => setWhere(e.target.value)}
          placeholder="amount > 100 AND status = 'paid'"
        />
      </div>

      <div className="field inline">
        <button
          type="button"
          className="btn btn-primary btn-sm"
          data-testid="query-run"
          disabled={busy}
          onClick={() => execute(offset)}
        >
          Execute
        </button>
        <label>
          Limit{" "}
          <input
            type="number"
            min={1}
            max={MAX_LIMIT}
            value={limit}
            data-testid="query-limit"
            onChange={(e) => setLimit(Math.min(MAX_LIMIT, Math.max(1, Number(e.target.value) || 1)))}
          />
        </label>
        <label>
          Offset{" "}
          <input
            type="number"
            min={0}
            value={offset}
            data-testid="query-offset"
            onChange={(e) => setOffset(Math.max(0, Number(e.target.value) || 0))}
          />
        </label>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="query-prev"
          disabled={busy || offset === 0}
          onClick={() => execute(Math.max(0, offset - limit))}
        >
          ← Previous
        </button>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="query-next"
          disabled={busy}
          onClick={() => execute(offset + limit)}
        >
          Next →
        </button>
        <button type="button" className="btn btn-sm" data-testid="query-csv" disabled={busy} onClick={downloadCsv}>
          Download CSV
        </button>
      </div>

      {fields.length > 0 && (
        <FieldPickers
          fields={fields}
          visibleCols={visibleCols}
          orderBy={orderBy}
          onVisibleColsChange={setVisibleCols}
          onOrderByChange={setOrderBy}
          orderHeaderExtra={
            <button
              type="button"
              data-testid="orderby-run"
              className="btn btn-sm"
              disabled={busy}
              onClick={() => execute(offset)}
            >
              Run
            </button>
          }
        />
      )}

      {modalOpen && (
        <CellViewModal
          initial={cellView}
          onCancel={() => setModalOpen(false)}
          onSave={(value) => {
            if (selectedName.trim()) persist(value);
            else {
              setCellView(value);
              setModalOpen(false);
            }
          }}
          saveDisabled={busy}
        />
      )}

      {error && (
        <p className="err" data-testid="query-error">
          {error}
        </p>
      )}
      {rows && (
        <ResultsTable
          columns={columns}
          rows={rows.data}
          shownIdx={shownIdx}
          testid="query-output"
          renderCell={(col, value, row) => renderCell(col, value, views, row, columns, colTypes)}
        />
      )}
    </section>
  );
}
