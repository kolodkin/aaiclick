import { useDeferredValue, useState } from "react";
import { useAudit } from "../api/hooks";
import { useAuth } from "../components/Auth";
import { Chips } from "../components/Chips";
import { relativeTime } from "../lib/format";

function statusClass(status: number): string {
  if (status >= 500) return "b-FAILED";
  if (status >= 400) return "b-CANCELLED";
  return "b-COMPLETED";
}

export function Audit({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { me } = useAuth();
  const [username, setUsername] = useState("");
  const [method, setMethod] = useState("");
  const [path, setPath] = useState("");
  // Deferred: each keystroke would otherwise fire a COUNT + SELECT over a
  // table that grows with every request the server handles.
  const query = useDeferredValue({ username, method, path, limit: 200 });
  const { data, isLoading, isError, error } = useAudit(query);

  return (
    <>
      <Chips chips={[{ label: "← home", cmd: "" }]} onPrompt={onPrompt} />
      <h2>Audit log</h2>
      <p className="sub">Newest first. Which requests are recorded follows AAICLICK_AUDIT_LOG (writes / all / off).</p>
      {!me?.superadmin && <p className="err">Requires the superadmin flag.</p>}
      <div className="chips">
        <input className="fake-prompt" id="audit-username" placeholder="username" value={username} onChange={(e) => setUsername(e.target.value)} />
        <input className="fake-prompt" id="audit-method" placeholder="method (POST)" value={method} onChange={(e) => setMethod(e.target.value)} />
        <input className="fake-prompt" id="audit-path" placeholder="path prefix (/api/v0/jobs)" value={path} onChange={(e) => setPath(e.target.value)} />
      </div>
      {isLoading && <p className="sub">loading…</p>}
      {isError && <p className="err">{error.message}</p>}
      {data && (
        <table>
          <thead>
            <tr>
              <th>When</th>
              <th>User</th>
              <th>Kind</th>
              <th>Tenant</th>
              <th>Request</th>
              <th>Status</th>
              <th>Duration</th>
              <th>Client</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((e) => (
              <tr key={e.id}>
                <td title={e.at}>{relativeTime(e.at)}</td>
                <td className="mono">{e.username ?? (e.user_id ? `#${e.user_id}` : "—")}</td>
                <td>{e.auth_kind}</td>
                <td className="mono">{e.tenant_id ?? "—"}</td>
                <td className="mono">
                  {e.method} {e.path}
                  {e.action ? ` · ${e.action}` : ""}
                </td>
                <td>
                  <span className={`badge ${statusClass(e.status)}`}>{e.status}</span>
                </td>
                <td>{e.duration_ms} ms</td>
                <td className="mono">{e.client_ip ?? "—"}</td>
              </tr>
            ))}
            {data.items.length === 0 && (
              <tr>
                <td colSpan={8} className="sub">
                  No entries.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </>
  );
}
