import { useState } from "react";
import { useApiTokens, useCreateApiToken, useRevokeApiToken } from "../api/hooks";
import type { ApiTokenCreated } from "../api/types";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";
import { relativeTime } from "../lib/format";

const DEFAULT_EXPIRY_DAYS = "90";

function expiresAt(days: string): string | null {
  const n = Number(days);
  if (!days.trim() || !Number.isFinite(n) || n <= 0) return null;
  return new Date(Date.now() + n * 86_400_000).toISOString();
}

// The raw secret is shown exactly once, right after minting. It never comes
// back from the list endpoint, so the panel stays until the user dismisses it.
function NewToken({ token, onDone }: { token: ApiTokenCreated; onDone: () => void }) {
  const toast = useToast();
  const copy = () => {
    void navigator.clipboard?.writeText(token.token).then(() => toast("Token copied"));
  };
  return (
    <Panel className="confirm info">
      <h2>Token created</h2>
      <p className="sub">Copy it now — it cannot be retrieved again.</p>
      <p className="mono" id="new-token-secret" style={{ overflowWrap: "anywhere" }}>
        {token.token}
      </p>
      <div className="form-actions">
        <button className="btn btn-primary" onClick={copy}>
          Copy
        </button>
        <button className="btn" onClick={onDone}>
          Done
        </button>
      </div>
    </Panel>
  );
}

export function Tokens({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { data, isLoading, isError, error } = useApiTokens();
  const create = useCreateApiToken();
  const revoke = useRevokeApiToken();
  const toast = useToast();
  const [name, setName] = useState("");
  const [scope, setScope] = useState<"read" | "write">("read");
  const [days, setDays] = useState(DEFAULT_EXPIRY_DAYS);
  const [created, setCreated] = useState<ApiTokenCreated | null>(null);

  const onCreate = () => {
    create.mutate(
      { name, scope, expires_at: expiresAt(days) },
      {
        onSuccess: (t) => {
          setCreated(t);
          setName("");
        },
        onError: (e) => toast(`Create failed: ${e.message}`),
      },
    );
  };

  return (
    <>
      <Chips chips={[{ label: "← home", cmd: "" }]} onPrompt={onPrompt} />
      <h2>API tokens</h2>
      <p className="sub">
        Long-lived bearer credentials for unattended clients. Send one as <code className="mono">Authorization: Bearer aaic_…</code>.
      </p>
      {isError && <p className="err">{error.message}</p>}
      {created && <NewToken token={created} onDone={() => setCreated(null)} />}
      <Panel>
        <h2>New token</h2>
        <div className="field">
          <label>
            Name <span className="help">— what will use it, e.g. ci-deploy</span>
          </label>
          <input id="token-name" type="text" value={name} onChange={(e) => setName(e.target.value)} placeholder="ci-deploy" />
        </div>
        <div className="field">
          <label>
            Scope <span className="help">— read: GET only; write: everything your roles allow</span>
          </label>
          <select id="token-scope" value={scope} onChange={(e) => setScope(e.target.value as "read" | "write")}>
            <option value="read">read</option>
            <option value="write">write</option>
          </select>
        </div>
        <div className="field">
          <label>
            Expires in days <span className="help">— blank for never</span>
          </label>
          <input id="token-days" type="text" value={days} onChange={(e) => setDays(e.target.value)} placeholder="90" />
        </div>
        <div className="form-actions">
          <button id="token-create" className="btn btn-primary" disabled={create.isPending || !name.trim()} onClick={onCreate}>
            Create token
          </button>
        </div>
      </Panel>
      {isLoading && <p className="sub">loading…</p>}
      {data && (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Prefix</th>
              <th>Scope</th>
              <th>Expires</th>
              <th>Last used</th>
              <th>Status</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((t) => (
              <tr key={t.id}>
                <td className="mono">{t.name}</td>
                <td className="mono">{t.prefix}…</td>
                <td>
                  <span className="badge b-PENDING">{t.scope}</span>
                </td>
                <td>{t.expires_at ? new Date(t.expires_at).toLocaleDateString() : "never"}</td>
                <td>{relativeTime(t.last_used_at)}</td>
                <td>{t.revoked_at ? <span className="badge b-CANCELLED">revoked</span> : <span className="badge b-COMPLETED">active</span>}</td>
                <td>
                  <button
                    className="btn btn-danger btn-sm"
                    disabled={t.revoked_at !== null || revoke.isPending}
                    onClick={() =>
                      revoke.mutate(t.id, {
                        onSuccess: () => toast(`Revoked ${t.name}`),
                        onError: (e) => toast(`Revoke failed: ${e.message}`),
                      })
                    }
                  >
                    Revoke
                  </button>
                </td>
              </tr>
            ))}
            {data.items.length === 0 && (
              <tr>
                <td colSpan={7} className="sub">
                  No tokens yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </>
  );
}
