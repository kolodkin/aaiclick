// The document a dashboard iframe renders: results prologue + agent HTML.

// Column-oriented results map: {query_name: {column_name: values[]}}.
export type DashboardResults = Record<string, Record<string, unknown[]>>

// Build the iframe document: a prologue exposing results as `window.queries`,
// then the agent-authored HTML. JSON `<` is escaped so an embedded `</script>`
// in result data can't break out of the prologue script.
export function buildSrcDoc(html: string, results: DashboardResults): string {
  const safeJson = JSON.stringify(results).replace(/</g, '\\u003c')
  return `<script>window.queries = ${safeJson};</script>\n${html}`
}
