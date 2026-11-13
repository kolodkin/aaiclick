// The document a dashboard iframe renders: a prologue + the agent HTML.

import type { ResolvedParam } from "./params";

// Column-oriented results map: {query_name: {column_name: values[]}}.
export type DashboardResults = Record<string, Record<string, unknown[]>>;

// The in-iframe half of the params channel. The page sends values only; the
// host substitutes them into the dashboard's own SQL and posts results back, so
// the page re-renders without a reload and never handles SQL.
const SET_PARAMS_HELPER = `
window.setParams = function (values) {
  parent.postMessage({ type: 'set-params', values: values }, '*');
};
window.addEventListener('message', function (e) {
  var d = e.data;
  if (!d || d.type !== 'params-results') return;
  // A failed run keeps the previous results on screen and reports the message.
  if (d.results) window.queries = d.results;
  if (d.params && d.params.length) window.params = d.params;
  if (typeof window.onQueryResults === 'function') {
    window.onQueryResults(window.queries, window.params, d.ok ? null : d.message);
  }
});`;

// Build the iframe document: a prologue exposing results as `window.queries`,
// the resolved selectors as `window.params` and `window.setParams`, then the
// agent-authored HTML. JSON `<` is escaped so an embedded `</script>` in result
// data can't break out of the prologue script.
export function buildSrcDoc(html: string, results: DashboardResults, params: ResolvedParam[] = []): string {
  const safe = (v: unknown) => JSON.stringify(v).replace(/</g, "\\u003c");
  return (
    `<script>window.queries = ${safe(results)};\nwindow.params = ${safe(params)};` +
    `${SET_PARAMS_HELPER}\n</script>\n${html}`
  );
}
