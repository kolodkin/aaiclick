import { useRegisteredJobs } from "../api/hooks";
import { AdminButton } from "../components/AdminButton";
import { EnabledToggle } from "../components/EnabledToggle";

export function Registered({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useRegisteredJobs();
  return (
    <>
      <div className="chips">
        <span className="chip" onClick={() => onPrompt("@jobs")}>
          ← @jobs
        </span>
        <div className="spacer" />
        <AdminButton className="btn btn-primary btn-sm" onClick={() => onPrompt("register")}>
          + Register new job
        </AdminButton>
      </div>
      <h2>Registered jobs</h2>
      <p className="sub">Run on demand, or on a cron schedule via the background scheduler.</p>
      {isLoading && <p className="sub">loading…</p>}
      {isError && <p className="err">failed to load registered jobs</p>}
      {data && (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Entrypoint</th>
              <th>Enabled</th>
              <th>Schedule</th>
              <th>Next run</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((r) => (
              <tr key={r.id}>
                <td>
                  <span className="name-link mono">{r.name}</span>
                </td>
                <td className="mono">{r.entrypoint}</td>
                <td>
                  <EnabledToggle name={r.name} enabled={r.enabled} />
                </td>
                <td className="mono">{r.schedule ?? "manual"}</td>
                <td>{r.next_run_at ?? "—"}</td>
                <td>
                  <div className="row-actions">
                    <AdminButton className="btn btn-primary btn-sm" onClick={() => onPrompt(`run ${r.name}`)}>
                      Run
                    </AdminButton>
                    <AdminButton className="btn btn-sm" onClick={() => onPrompt(`run ${r.name} ?`)}>
                      Run…
                    </AdminButton>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
