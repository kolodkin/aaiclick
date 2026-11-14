// The cell popup: the full value of a result cell too long to read in the grid
// (see ResultsTable). JSON/YAML opens on a collapsible parsed tree with a Raw
// toggle; anything else is wrapped raw text.

import { useMemo, useState } from "react";

import { cellText, isContainer, type Cell } from "../results/rows";
import { detectStructured } from "./structured";
import { defaultCollapsed, treeRows } from "./structuredTree";

function ParsedTree({ data }: { data: unknown }) {
  const [collapsed, setCollapsed] = useState(() => defaultCollapsed(data));
  const rows = useMemo(() => treeRows(data, collapsed), [data, collapsed]);

  function toggle(path: string) {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (!next.delete(path)) next.add(path);
      return next;
    });
  }

  return (
    <div data-testid="cell-data-tree" className="font-mono text-xs leading-relaxed">
      {rows.map((r) => (
        <div
          key={r.path}
          data-testid="cell-data-row"
          className="flex items-baseline gap-1 whitespace-pre-wrap break-all"
          style={{ paddingLeft: `${r.depth}rem` }}
        >
          {r.expandable ? (
            <button
              type="button"
              onClick={() => toggle(r.path)}
              data-testid="cell-data-toggle"
              aria-expanded={!collapsed.has(r.path)}
              className="w-3 shrink-0 text-slate-400 hover:text-slate-200"
            >
              {collapsed.has(r.path) ? "▸" : "▾"}
            </button>
          ) : (
            <span className="w-3 shrink-0" />
          )}
          {r.key !== null && <span className="text-indigo-300">{r.key}:</span>}
          {r.key === null && <span className="text-slate-500">root</span>}
          <span className="text-slate-200">{r.summary}</span>
        </div>
      ))}
    </div>
  );
}

export function CellDataModal({
  column,
  value,
  type,
  onClose,
}: {
  column: string;
  value: Cell;
  // The column's database type. A collection the driver sent as such is
  // labelled by it (`Map`, `Array`, ...), not as the JSON it serializes to.
  type?: string;
  onClose: () => void;
}) {
  const text = cellText(value);
  const native = isContainer(value);
  const structured = useMemo(() => {
    if (native) return { label: type ? type.split("(")[0] : "value", data: value };
    const found = detectStructured(text);
    return found && { label: found.format, data: found.data };
  }, [native, type, value, text]);
  const [raw, setRaw] = useState(false);
  const showParsed = structured !== null && !raw;

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label={`Cell value: ${column}`}
      data-testid="cell-data-modal"
      className="fixed inset-0 z-30 flex items-center justify-center bg-black/50 p-6 backdrop-blur-sm"
      onClick={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div className="glass-popover flex max-h-[80vh] w-full max-w-3xl flex-col p-5">
        <div className="mb-3 flex items-center gap-3">
          <div className="flex min-w-0 flex-1 items-center gap-2">
            <h3 className="min-w-0 truncate text-base font-semibold text-slate-100">{column}</h3>
            {/* A label, not a control: it sits with the title, away from the buttons. */}
            {structured && (
              <span
                data-testid="cell-data-format"
                title={native ? type : undefined}
                className="shrink-0 rounded bg-indigo-500/15 px-1.5 py-0.5 font-mono text-[10px] font-semibold uppercase tracking-wider text-indigo-300"
              >
                {structured.label}
              </span>
            )}
          </div>
          {structured && (
            <>
              <button
                type="button"
                onClick={() => setRaw((r) => !r)}
                data-testid="cell-data-raw-toggle"
                className="glass-btn px-3 py-1 text-xs"
              >
                {raw ? "Parsed" : "Raw"}
              </button>
            </>
          )}
          <button type="button" onClick={onClose} data-testid="cell-data-close" className="glass-btn px-3 py-1 text-xs">
            Close
          </button>
        </div>
        <div className="min-h-0 flex-1 overflow-auto rounded-lg border border-white/10 bg-black/20 p-3">
          {showParsed ? (
            <ParsedTree data={structured.data} />
          ) : (
            <pre data-testid="cell-data-raw" className="whitespace-pre-wrap break-all font-mono text-xs text-slate-200">
              {text}
            </pre>
          )}
        </div>
      </div>
    </div>
  );
}
