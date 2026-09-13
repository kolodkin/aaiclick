import { useState } from "react";
import { useInviteUser } from "../api/hooks";
import type { PasswordResetLinkView } from "../api/types";
import { useAuth } from "../components/Auth";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { SecretPanel } from "../components/SecretPanel";
import { useToast } from "../components/Toast";
import { getActiveTenantId } from "../lib/auth";

type Grant = "viewer" | "admin" | "superadmin";

const GRANT_HELP: Record<Grant, string> = {
  viewer: "reads, plus saved queries and dashboards",
  admin: "everything in this tenant — jobs, objects, memberships",
  superadmin: "the whole instance, every tenant",
};

export function Invite({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { me } = useAuth();
  const invite = useInviteUser();
  const toast = useToast();
  const [username, setUsername] = useState("");
  const [email, setEmail] = useState("");
  const [grant, setGrant] = useState<Grant>("viewer");
  const [link, setLink] = useState<PasswordResetLinkView | null>(null);

  // A tenant admin may grant at most `admin`, and only where they already act;
  // the server enforces this, the picker just avoids offering a certain 403.
  const grants: Grant[] = me?.superadmin ? ["viewer", "admin", "superadmin"] : ["viewer", "admin"];

  const submit = () =>
    invite.mutate(
      {
        username,
        email: email || null,
        superadmin: grant === "superadmin",
        // An instance invite names no tenant; a tenant invite acts where the
        // inviter already is.
        tenant_id: grant === "superadmin" ? null : getActiveTenantId(),
        role: grant === "superadmin" ? null : grant,
      },
      {
        onSuccess: (res) => {
          setLink(res.link);
          toast(`Invited ${res.user.username}`);
          setUsername("");
          setEmail("");
        },
        onError: (e) => toast(`Invite failed: ${e.message}`),
      },
    );

  return (
    <>
      <Chips chips={[{ label: "← home", cmd: "" }]} onPrompt={onPrompt} />
      <h2>Invite a user</h2>
      <p className="sub">
        Creates the account with no password and mints a one-time link. They set their own password by redeeming it —
        until then the account grants nothing.
      </p>
      {link && (
        <SecretPanel
          title="One-time link"
          hint={`Valid until ${new Date(link.expires_at).toLocaleString()} — single use. Hand it over out of band.`}
          value={link.url ?? link.token}
          onDone={() => setLink(null)}
        />
      )}
      <Panel>
        <div className="field">
          <label>Username</label>
          <input id="invite-name" type="text" value={username} onChange={(e) => setUsername(e.target.value)} />
        </div>
        <div className="field">
          <label>
            Email <span className="help">— optional</span>
          </label>
          <input id="invite-email" type="text" value={email} onChange={(e) => setEmail(e.target.value)} />
        </div>
        <div className="field">
          <label>
            Grants <span className="help">— {GRANT_HELP[grant]}</span>
          </label>
          <select id="invite-grant" value={grant} onChange={(e) => setGrant(e.target.value as Grant)}>
            {grants.map((g) => (
              <option key={g} value={g}>
                {g}
              </option>
            ))}
          </select>
        </div>
        <div className="form-actions">
          <button
            id="invite-send"
            className="btn btn-primary"
            disabled={invite.isPending || !username.trim()}
            onClick={submit}
          >
            Send invite
          </button>
        </div>
      </Panel>
    </>
  );
}
