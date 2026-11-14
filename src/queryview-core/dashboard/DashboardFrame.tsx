// Sandboxed rendering of an agent-authored dashboard: the HTML runs in an
// iframe with the query results exposed as `window.queries` and its selectors
// as `window.params`. Built once per load: later param changes are answered
// over postMessage, so the page keeps its state instead of reloading.

import { useMemo, type Ref } from "react";

import type { ResolvedParam } from "./params";
import { buildSrcDoc, type DashboardResults, type QvTheme } from "./srcDoc";

export function DashboardFrame({
  html,
  results,
  params = [],
  theme,
  ref,
  className = "h-[78vh] w-full rounded-xl border border-white/10 bg-white",
}: {
  html: string;
  results: DashboardResults;
  params?: ResolvedParam[];
  // --qv-* overrides for the frame, which the host page's variables don't reach.
  theme?: QvTheme;
  // The host answers set-params messages only from this frame.
  ref?: Ref<HTMLIFrameElement>;
  className?: string;
}) {
  // Deliberately keyed on the load, not on params: a re-run posts new params
  // into the live frame rather than rebuilding it. The theme is keyed by value
  // so an inline object doesn't reload the frame on every render.
  const themeKey = JSON.stringify(theme);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const srcDoc = useMemo(() => buildSrcDoc(html, results, params, theme), [html, results, themeKey]);
  return (
    <iframe
      ref={ref}
      title="dashboard"
      data-testid="dashboard-frame"
      // No allow-same-origin: the page gets no session and no /api. Popups
      // escape the sandbox so a clicked link in a cell opens a normal tab.
      sandbox="allow-scripts allow-popups allow-popups-to-escape-sandbox"
      srcDoc={srcDoc}
      className={className}
    />
  );
}
