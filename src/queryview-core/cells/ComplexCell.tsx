import { useState } from 'react'
import { cellText, type Cell } from '../results/rows'
import { complexCellItems, PREVIEW_COUNT, type ComplexType } from './complexCells'

// Built-in default view for an Array/Map/Tuple result cell: a plain vertical
// list of items, collapsed to the first PREVIEW_COUNT with an expander. See
// docs/query.md ("Default views for complex types"). Values are plain text
// nodes (React-escaped), so DB content can't inject markup.
export function ComplexCell({
  type,
  value,
  col,
}: {
  type: ComplexType | null
  value: Cell
  col: string
}) {
  const [expanded, setExpanded] = useState(false)
  const items = complexCellItems(type, value)
  // Empty collection or scalar: fall back to the plain text.
  if (items.length === 0) return <span data-testid={`cell-${col}`}>{cellText(value)}</span>

  const collapsible = items.length > PREVIEW_COUNT
  const shown = expanded ? items : items.slice(0, PREVIEW_COUNT)
  const hidden = items.length - PREVIEW_COUNT

  return (
    <div data-testid={`cell-${col}`} className="flex flex-col gap-0.5">
      {shown.map((item, i) => (
        <div
          key={i}
          className={item.lines.length > 1 ? 'border-l border-white/15 pl-2' : undefined}
        >
          {item.lines.map((line, j) => (
            <div key={j}>{line}</div>
          ))}
        </div>
      ))}
      {collapsible && (
        <button
          type="button"
          data-testid={`cell-${col}-toggle`}
          onClick={() => setExpanded((e) => !e)}
          className="self-start text-xs text-slate-400 hover:text-slate-200"
        >
          {expanded ? '▾ collapse' : `… (+${hidden} more) ▸`}
        </button>
      )}
    </div>
  )
}
