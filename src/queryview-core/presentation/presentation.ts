import type { Field } from "./FieldPickers";

// Indices of the result columns to render. A column the describe didn't cover
// always shows, so a stale field list can't blank the table. Shared by the
// dashboard widgets and the explorer.
export function shownColumnIndices(columns: string[], fields: Field[], visibleCols: string[]): number[] {
  const fieldNames = new Set(fields.map((f) => f.name));
  const visible = new Set(visibleCols);
  return columns.map((_, i) => i).filter((i) => !fieldNames.has(columns[i]) || visible.has(columns[i]));
}
