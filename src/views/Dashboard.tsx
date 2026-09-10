import { DashboardFrame } from "@qv/core";
import { useDashboard, useDashboards, useRunDashboard, useSaveDashboard } from "../api/hooks";
import { Chips } from "../components/Chips";

export function Dashboard({ name, onPrompt }: { name: string | null; onPrompt: (v: string) => void }) {
  const list = useDashboards();
  const dashboard = useDashboard(name ?? "");
  const results = useRunDashboard(name ?? "");
  const save = useSaveDashboard();

  return (
    <>
      <h2>Dashboard</h2>
      <p className="sub">Agent-authored HTML over saved object queries, rendered in a sandbox</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@data", cmd: "@data" },
          { label: "@query", cmd: "@query" },
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
          {name && !list.data?.items.some((d) => d.name === name) && <option value={name}>{name}</option>}
          {list.data?.items.map((d) => (
            <option key={d.name} value={d.name}>
              {d.name}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="dashboard-refresh"
          disabled={!name || results.isFetching}
          onClick={() => results.refetch()}
        >
          Refresh
        </button>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="dashboard-save"
          disabled={!dashboard.data || save.isPending}
          onClick={() => {
            const d = dashboard.data;
            if (d) save.mutate({ name: d.name, body: { name: d.name, scope: d.scope, html: d.html, queries: d.queries } });
          }}
        >
          Save
        </button>
      </div>
      {!name && (
        <p className="sub" data-testid="dashboard-empty">
          {list.data?.items.length
            ? "Pick a dashboard to view it."
            : "No dashboards yet. Save one through the API, MCP, or `view dashboards save`."}
        </p>
      )}
      {(dashboard.isError || results.isError) && (
        <p className="err" data-testid="dashboard-error">
          {dashboard.error?.message ?? results.error?.message}
        </p>
      )}
      {name && results.isPending && !results.isError && (
        <p className="sub" data-testid="dashboard-loading">
          Running queries…
        </p>
      )}
      {dashboard.data && results.data && <DashboardFrame html={dashboard.data.html} results={results.data.results} />}
    </>
  );
}
