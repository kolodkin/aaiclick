// Cell-view templating: turn a saved `value` template into rendered output by
// substituting placeholders against a single result row. Used by both the
// `link` and `custom` cell views (see docs/query.md).

export function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;')
}

// Substitute cell-template placeholders against a result row.
//
// Two placeholder forms are recognized:
//   {cell}        -> this cell's own raw value
//   {row.<col>}   -> the value of column <col> in the same row
//
// Each substituted value is passed through `encode` (URL-encoding for link
// hrefs, HTML-escaping for custom HTML) so DB content stays inert. Substitution
// is a single pass, so a value that itself looks like a placeholder is encoded
// as literal text rather than re-resolved. An unknown {row.<col>} (no matching
// column) is left untouched.
export function substituteCellTemplate(
  template: string,
  raw: string,
  row: string[],
  columns: string[],
  encode: (s: string) => string,
): string {
  return template.replace(/\{cell\}|\{row\.([^}]+)\}/g, (match, col) => {
    if (col === undefined) return encode(raw) // {cell}
    const idx = columns.indexOf(col)
    if (idx === -1) return match // unknown column: leave the placeholder as-is
    return encode(row[idx] ?? '')
  })
}
