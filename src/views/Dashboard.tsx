// See docs/designs/ui.md — "Dashboard (`@dashboard [name]`)".
import { useEffect, useRef } from "react";
import { useQuery } from "@tanstack/react-query";
import {
  applyToQueries,
  DashboardFrame,
  parseDashboardParams,
  parseParamsRequest,
  resolveParams,
  type QueryRunner,
} from "@qv/core";
import { runQueries, useDashboard, useDashboards } from "../api/hooks";
import type { Dashboard as DashboardModel } from "../api/types";
import { Chips } from "../components/Chips";

// ClickHouse's identifier quote, for `identifier` params.
const QUOTE = "`";

// aaiclick has no free-SQL endpoint, so `options_sql` params are dropped
// before they resolve and this is never called.
const noSql: QueryRunner = async () => ({ ok: false, message: "options_sql is not supported" });

// Resolve the dashboard's selectors, substitute them into each panel's `where`
// and run the panels whose selectors are all chosen.
async function runPanels(d: DashboardModel, values: Record<string, string>) {
  const declared = parseDashboardParams(d.params).filter((p) => !p.optionsSql);
  const params = await resolveParams(declared, values, noSql, QUOTE);
  const wheres = applyToQueries(
    Object.fromEntries(Object.entries(d.queries).map(([panel, q]) => [panel, q.where ?? ""])),
    declared,
    params,
    QUOTE,
  );
  const queries = Object.fromEntries(
    Object.entries(wheres).map(([panel, where]) => [panel, { ...d.queries[panel], where: where || null }]),
  );
  const results = Object.keys(queries).length ? (await runQueries(d.scope, queries)).results : {};
  return { results, params };
}

export function Dashboard({ name, onPrompt }: { name: string | null; onPrompt: (v: string) => void }) {
  const list = useDashboards();
  const dashboard = useDashboard(name ?? "");
  const d = dashboard.data;
  const run = useQuery({
    queryKey: ["dashboard-results", name, d?.updated_at],
    queryFn: () => runPanels(d!, {}),
    enabled: !!d,
    refetchInterval: false,
  });
  const frameRef = useRef<HTMLIFrameElement>(null);

  // Answer the page's setParams: values only, and only from this dashboard's
  // own frame, so the page never issues a query of its own.
  useEffect(() => {
    if (!d) return;
    async function onMessage(e: MessageEvent) {
      const frame = frameRef.current;
      if (!frame || !d || e.source !== frame.contentWindow) return;
      const values = parseParamsRequest(e.data);
      if (!values) return;
      let answer: Record<string, unknown>;
      try {
        answer = { type: "params-results", ok: true, ...(await runPanels(d, values)) };
      } catch (err) {
        const message = err instanceof Error ? err.message : "Failed to run queries.";
        answer = { type: "params-results", ok: false, message, params: [] };
      }
      frame.contentWindow?.postMessage(answer, "*");
    }
    window.addEventListener("message", onMessage);
    return () => window.removeEventListener("message", onMessage);
  }, [d]);

  return (
    <>
      <h2>Dashboard</h2>
      <p className="sub">Agent-authored HTML over object queries, rendered in a sandbox</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@data", cmd: "@data" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="field inline">
        <select
          data-testid="dashboard-select"
          aria-label="Dashboards"
          value={name ?? ""}
          onChange={(e) => onPrompt(e.target.value ? `@dashboard ${e.target.value}` : "@dashboard")}
        >
          <option value="">Select a dashboard…</option>
          {name && !list.data?.items.some((i) => i.name === name) && <option value={name}>{name}</option>}
          {list.data?.items.map((i) => (
            <option key={i.name} value={i.name}>
              {i.name}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="dashboard-refresh"
          disabled={!name || run.isFetching}
          onClick={() => run.refetch()}
        >
          Refresh
        </button>
      </div>
      {!name && (
        <p className="sub" data-testid="dashboard-empty">
          {list.data?.items.length
            ? "Pick a dashboard to view it."
            : "No dashboards yet. Save one through the API, MCP, or `view dashboards save`."}
        </p>
      )}
      {(dashboard.isError || run.isError) && (
        <p className="err" data-testid="dashboard-error">
          {dashboard.error?.message ?? run.error?.message}
        </p>
      )}
      {name && run.isPending && !dashboard.isError && !run.isError && (
        <p className="sub" data-testid="dashboard-loading">
          Running queries…
        </p>
      )}
      {d && run.data && (
        <DashboardFrame ref={frameRef} html={d.html} results={run.data.results} params={run.data.params} />
      )}
    </>
  );
}
