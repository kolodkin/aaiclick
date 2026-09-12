// Saved cell-view config (the `cell_view` YAML on a predefined query) and the
// cell renderer that applies it: `link` and `custom` views win, otherwise
// complex ClickHouse types get the built-in collapsible view, else plain text.

import { ComplexCell } from './ComplexCell'
import { escapeHtml, substituteCellTemplate } from './cellView'
import { parseComplexType } from './complexCells'
import { parseYamlObject } from '../params/queryParams'
import { cellText, isContainer, type Cell } from '../results/rows'

export type CellView = { type: string; value: string }
export type CellViewMap = Record<string, CellView>

// Parse the saved cell_view YAML into a map. A parse error or entry without
// string {type, value} is dropped — broken config falls through to plain text
// rather than blanking the table.
export function parseCellViewYaml(text: string | null | undefined): CellViewMap {
  const doc = parseYamlObject(text)
  if (!doc) return {}
  const out: CellViewMap = {}
  for (const [k, v] of Object.entries(doc)) {
    // `params` is reserved for query-parameter dropdowns (see queryParams.ts).
    if (k === 'params') continue
    if (v && typeof v === 'object' && !Array.isArray(v)) {
      const o = v as Record<string, unknown>
      if (typeof o.type === 'string' && typeof o.value === 'string') {
        out[k] = { type: o.type, value: o.value }
      }
    }
  }
  return out
}

export function renderCell(
  colName: string,
  value: Cell,
  views: CellViewMap,
  row: Cell[],
  columns: string[],
  colTypes: Record<string, string>,
): React.ReactNode {
  const view = views[colName]
  // An explicit cell_view entry wins (and is the opt-out from a default view).
  if (!view) {
    const complex = parseComplexType(colTypes[colName] ?? '')
    if (complex || isContainer(value)) return <ComplexCell type={complex} value={value} col={colName} />
    return cellText(value)
  }
  const raw = cellText(value)
  const rowText = row.map(cellText)
  const testid = `cell-${colName}`
  if (view.type === 'link') {
    const href = substituteCellTemplate(view.value, raw, rowText, columns, encodeURIComponent)
    let scheme: string
    try {
      scheme = new URL(href).protocol
    } catch {
      return raw
    }
    if (scheme !== 'http:' && scheme !== 'https:') return raw
    return (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        data-testid={testid}
        className="text-indigo-300 underline hover:text-indigo-200"
      >
        {raw}
      </a>
    )
  }
  if (view.type === 'custom') {
    const html = substituteCellTemplate(view.value, raw, rowText, columns, escapeHtml)
    // Cell value is HTML-escaped above so DB content is inert; template HTML is
    // trusted (whoever saves a predefined query can inject markup — see docs/query.md).
    return <span data-testid={testid} dangerouslySetInnerHTML={{ __html: html }} />
  }
  return raw
}
