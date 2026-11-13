// The results grid shared by the query panel and the explorer: a sticky-header
// table over result rows, restricted to the visible columns (see
// shownColumnIndices in presentation.ts). It fills the rest of its flex-column
// parent (at least 12rem) and scrolls inside, both ways.
//
// The browser fits each column to its widest cell, so a short table fits on
// screen; a cap on every cell (see cellWidth.ts) keeps one long-text column from
// stretching the grid. A value past the cap scrolls inside its own cell and gets
// a button, at the cell's left edge, opening it in CellDataModal.

import { useMemo, useState } from "react";

import { CellDataModal } from "../cells/CellDataModal";
import { detectFormat } from "../cells/structured";
import { CELL_MAX_WIDTH, isOverflowing } from "./cellWidth";
import { cellText, type Cell } from "./rows";

type Opened = { column: string; value: Cell; type?: string };

type RenderCell = (col: string, value: Cell, row: Cell[]) => React.ReactNode;

function BodyCell({
  col,
  type,
  value,
  row,
  renderCell,
  onOpen,
}: {
  col: string;
  type?: string;
  value: Cell;
  row: Cell[];
  renderCell?: RenderCell;
  onOpen: (opened: Opened) => void;
}) {
  const text = cellText(value);
  const overflowing = isOverflowing(text);
  // Detection only names the format for the glyph, so it is skipped for values
  // that fit and memoised so re-rendering doesn't re-parse.
  const format = useMemo(() => (isOverflowing(text) ? detectFormat(text) : null), [text]);

  return (
    <td className="border-b border-white/5 px-2 py-1 align-top font-mono text-slate-200">
      <div className="flex items-start gap-1">
        {overflowing && (
          <button
            type="button"
            onClick={() => onOpen({ column: col, value, type })}
            data-testid="cell-expand"
            data-col={col}
            aria-label={`Open ${col} value`}
            title={format ? `Open parsed ${format}` : "Open full value"}
            className="w-6 shrink-0 rounded text-center text-xs leading-5 text-slate-400 hover:bg-white/10 hover:text-indigo-200"
          >
            {format ? "{ }" : "⤢"}
          </button>
        )}
        {/* The cell's own scroller: a long value scrolls here, not the grid. */}
        <div style={{ maxWidth: CELL_MAX_WIDTH }} className="cell-scroll min-w-0 overflow-x-auto whitespace-pre">
          {renderCell ? renderCell(col, value, row) : text}
        </div>
      </div>
    </td>
  );
}

export function ResultsTable({
  columns,
  rows,
  shownIdx,
  testid,
  renderCell,
  dimmed = false,
  types = {},
}: {
  columns: string[];
  rows: Cell[][];
  // Column name → database type, from the result's meta.
  types?: Record<string, string>;
  shownIdx: number[];
  testid: string;
  // Cell content; defaults to plain text (the query panel plugs in cell views).
  renderCell?: RenderCell;
  // Faded while a re-run is in flight.
  dimmed?: boolean;
}) {
  const [open, setOpen] = useState<Opened | null>(null);
  return (
    <div
      data-testid={testid}
      className={`min-h-0 flex-[1_1_12rem] overflow-auto rounded-xl border border-white/10 ${
        dimmed ? "opacity-50 transition-opacity" : ""
      }`}
    >
      <table className="min-w-full border-collapse text-left font-mono text-sm">
        <thead className="sticky top-0 bg-[rgba(16,20,36,0.62)] backdrop-blur-lg">
          <tr>
            {shownIdx.map((i) => (
              <th
                key={i}
                title={columns[i]}
                className="border-b border-white/10 px-2 py-2 font-semibold text-slate-200"
              >
                <div style={{ maxWidth: CELL_MAX_WIDTH }} className="truncate">
                  {columns[i]}
                </div>
              </th>
            ))}
            {/* Takes the slack when the columns are narrower than the panel, so
                they stay fitted while the header and row rules span it. A `td`,
                so the `th`s stay one per column. */}
            <td aria-hidden className="w-full border-b border-white/10" />
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="odd:bg-transparent even:bg-white/[0.03]">
              {shownIdx.map((j) => (
                <BodyCell
                  key={j}
                  col={columns[j]}
                  type={types[columns[j]]}
                  value={row[j]}
                  row={row}
                  renderCell={renderCell}
                  onOpen={setOpen}
                />
              ))}
              <td aria-hidden className="border-b border-white/5" />
            </tr>
          ))}
        </tbody>
      </table>
      {open && <CellDataModal column={open.column} value={open.value} type={open.type} onClose={() => setOpen(null)} />}
    </div>
  );
}
