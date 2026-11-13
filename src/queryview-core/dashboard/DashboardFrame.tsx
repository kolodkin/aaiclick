// Sandboxed rendering of an agent-authored dashboard: the HTML runs in an
// iframe with the query results exposed as `window.queries` and its selectors
// as `window.params`. Built once per load: later param changes are answered
// over postMessage, so the page keeps its state instead of reloading.

import { useMemo, type Ref } from "react";

import type { ResolvedParam } from "./params";
import { buildSrcDoc, type DashboardResults } from "./srcDoc";

export function DashboardFrame({
  html,
  results,
  params = [],
  ref,
  className = "h-[78vh] w-full rounded-xl border border-white/10 bg-white",
}: {
  html: string;
  results: DashboardResults;
  params?: ResolvedParam[];
  // The host answers set-params messages only from this frame.
  ref?: Ref<HTMLIFrameElement>;
  className?: string;
}) {
  // Deliberately keyed on the load, not on params: a re-run posts new params
  // into the live frame rather than rebuilding it.
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const srcDoc = useMemo(() => buildSrcDoc(html, results, params), [html, results]);
  return (
    <iframe
      ref={ref}
      title="dashboard"
      data-testid="dashboard-frame"
      sandbox="allow-scripts"
      srcDoc={srcDoc}
      className={className}
    />
  );
}
