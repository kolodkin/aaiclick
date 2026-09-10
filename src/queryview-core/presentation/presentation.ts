import type { Field, OrderCol } from './FieldPickers'

// Indices of the result columns to render. A column the describe didn't cover
// always shows, so a stale field list can't blank the table. Shared by the
// query panel and the explorer.
export function shownColumnIndices(
  columns: string[],
  fields: Field[],
  visibleCols: string[],
): number[] {
  const fieldNames = new Set(fields.map((f) => f.name))
  const visible = new Set(visibleCols)
  return columns
    .map((_, i) => i)
    .filter((i) => !fieldNames.has(columns[i]) || visible.has(columns[i]))
}

// Saved presentation payload: empty selections persist as null ("no selection"),
// matching the backend's nullable columns.
export function presentationForSave(
  orderBy: OrderCol[],
  visibleCols: string[],
): { order_by: OrderCol[] | null; fields: string[] | null } {
  return {
    order_by: orderBy.length ? orderBy : null,
    fields: visibleCols.length ? visibleCols : null,
  }
}
