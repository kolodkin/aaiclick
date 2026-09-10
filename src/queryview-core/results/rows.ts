// The query-result contract: ClickHouse's JSONCompact shape, `{meta, data}`,
// produced by every backend driver. Values are JSON as the driver emitted
// them — 64-bit integers and decimals arrive as strings so nothing rounds.

export type Cell = null | boolean | number | string | Cell[] | { [key: string]: Cell }

export type ColumnMeta = { name: string; type: string }

export type QueryRows = { meta: ColumnMeta[]; data: Cell[][] }

export const EMPTY_ROWS: QueryRows = { meta: [], data: [] }

export function columnNames(rows: QueryRows): string[] {
  return rows.meta.map((c) => c.name)
}

export function columnTypes(rows: QueryRows): Record<string, string> {
  const out: Record<string, string> = {}
  for (const c of rows.meta) out[c.name] = c.type
  return out
}

export function isContainer(v: Cell): v is Cell[] | { [key: string]: Cell } {
  return typeof v === 'object' && v !== null
}

// A cell as plain text: null is empty, scalars stringify, containers are JSON.
export function cellText(v: Cell): string {
  if (v === null) return ''
  if (isContainer(v)) return JSON.stringify(v)
  return String(v)
}
