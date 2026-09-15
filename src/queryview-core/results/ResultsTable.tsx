// The results grid shared by the query panel and the explorer: a sticky-header
// table over result rows, restricted to the visible columns (see
// shownColumnIndices in presentation.ts).

import { cellText, type Cell } from './rows'

export function ResultsTable({
  columns,
  rows,
  shownIdx,
  testid,
  renderCell,
}: {
  columns: string[]
  rows: Cell[][]
  shownIdx: number[]
  testid: string
  // Cell content; defaults to plain text (the query panel plugs in cell views).
  renderCell?: (col: string, value: Cell, row: Cell[]) => React.ReactNode
}) {
  return (
    <div
      data-testid={testid}
      className="max-h-[70vh] overflow-auto rounded-xl border border-white/10"
    >
      <table className="min-w-full border-collapse text-left text-sm">
        <thead className="sticky top-0 bg-[rgba(16,20,36,0.62)] backdrop-blur-lg">
          <tr>
            {shownIdx.map((i) => (
              <th
                key={i}
                className="border-b border-white/10 px-3 py-2 font-semibold text-slate-200"
              >
                {columns[i]}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="odd:bg-transparent even:bg-white/[0.03]">
              {shownIdx.map((j) => (
                <td
                  key={j}
                  className="whitespace-pre border-b border-white/5 px-3 py-1 font-mono text-slate-200"
                >
                  {renderCell ? renderCell(columns[j], row[j], row) : cellText(row[j])}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
