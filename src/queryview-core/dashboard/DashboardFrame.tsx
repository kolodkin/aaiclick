// Sandboxed rendering of an agent-authored dashboard: the HTML runs in an
// iframe with the query results exposed as `window.queries`.

import { useMemo } from 'react'

import { buildSrcDoc, type DashboardResults } from './srcDoc'

export function DashboardFrame({
  html,
  results,
  className = 'h-[78vh] w-full rounded-xl border border-white/10 bg-white',
}: {
  html: string
  results: DashboardResults
  className?: string
}) {
  const srcDoc = useMemo(() => buildSrcDoc(html, results), [html, results])
  return (
    <iframe
      title="dashboard"
      data-testid="dashboard-frame"
      sandbox="allow-scripts"
      srcDoc={srcDoc}
      className={className}
    />
  )
}
