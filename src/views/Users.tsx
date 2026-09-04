import { useState } from "react";
import {
  useCreateResetLink,
  useCreateUser,
  useResetUserMfa,
  useSetDisabled,
  useSetSuperadmin,
  useSetUserEmail,
  useSetUserPassword,
  useUsers,
} from "../api/hooks";
import type { PasswordResetLinkView, UserView } from "../api/types";
import { useAuth } from "../components/Auth";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";
import { relativeTime } from "../lib/format";

function CreateUserForm() {
  const create = useCreateUser();
  const toast = useToast();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [email, setEmail] = useState("");
  const [superadmin, setSuperadmin] = useState(false);

  const submit = () =>
    create.mutate(
      { username, password: password || null, email: email || null, superadmin },
      {
        onSuccess: (u) => {
          toast(`Created ${u.username}`);
          setUsername("");
          setPassword("");
          setEmail("");
          setSuperadmin(false);
        },
        onError: (e) => toast(`Create failed: ${e.message}`),
      },
    );

  return (
    <Panel>
      <h2>New user</h2>
      <div className="field">
        <label>Username</label>
        <input id="user-name" type="text" value={username} onChange={(e) => setUsername(e.target.value)} />
      </div>
      <div className="field">
        <label>
          Password <span className="help">— leave blank for SSO-only, or mint a reset link afterwards</span>
        </label>
        <input id="user-password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
      </div>
      <div className="field">
        <label>
          Email <span className="help">— optional; used for password-reset mail</span>
        </label>
        <input id="user-email" type="text" value={email} onChange={(e) => setEmail(e.target.value)} />
      </div>
      <div className="field inline">
        <input id="user-superadmin" type="checkbox" checked={superadmin} onChange={(e) => setSuperadmin(e.target.checked)} />
        <label htmlFor="user-superadmin">Superadmin</label>
      </div>
      <div className="form-actions">
        <button id="user-create" className="btn btn-primary" disabled={create.isPending || !username.trim()} onClick={submit}>
          Create user
        </button>
      </div>
    </Panel>
  );
}

function UserRow({ user, self, onResetLink }: { user: UserView; self: boolean; onResetLink: (l: PasswordResetLinkView) => void }) {
  const toast = useToast();
  const resetLink = useCreateResetLink();
  const setSuperadmin = useSetSuperadmin();
  const setDisabled = useSetDisabled();
  const setPassword = useSetUserPassword();
  const setEmail = useSetUserEmail();
  const resetMfa = useResetUserMfa();
  const fail = (what: string) => (e: Error) => toast(`${what} failed: ${e.message}`);

  const onPassword = () => {
    const password = window.prompt(`New password for ${user.username}:`);
    if (password) setPassword.mutate({ id: user.id, password }, { onSuccess: () => toast("Password set"), onError: fail("Set password") });
  };
  const onEmail = () => {
    const email = window.prompt(`Email for ${user.username} (blank to clear):`, user.email ?? "");
    if (email !== null) setEmail.mutate({ id: user.id, email: email || null }, { onError: fail("Set email") });
  };

  return (
    <tr>
      <td className="mono">{user.username}</td>
      <td className="mono">{user.email ?? "—"}</td>
      <td>
        <button
          className={`toggle ${user.superadmin ? "on" : "off"}`}
          disabled={self || setSuperadmin.isPending}
          title={self ? "You cannot change your own superadmin flag" : undefined}
          onClick={() => setSuperadmin.mutate({ id: user.id, superadmin: !user.superadmin }, { onError: fail("Superadmin") })}
        >
          <span className="switch" />
          {user.superadmin ? "superadmin" : "user"}
        </button>
      </td>
      <td>
        <button
          className={`toggle ${user.disabled ? "off" : "on"}`}
          disabled={self || setDisabled.isPending}
          title={self ? "You cannot disable yourself" : undefined}
          onClick={() => setDisabled.mutate({ id: user.id, disabled: !user.disabled }, { onError: fail("Disable") })}
        >
          <span className="switch" />
          {user.disabled ? "disabled" : "enabled"}
        </button>
      </td>
      <td>
        {user.mfa_enabled && <span className="badge b-COMPLETED">mfa</span>} {user.sso_linked && <span className="badge b-RUNNING">sso</span>}{" "}
        {!user.has_password && <span className="badge b-PENDING">no password</span>}
      </td>
      <td>{relativeTime(user.created_at)}</td>
      <td>
        <div className="row-actions">
          <button className="btn btn-sm" onClick={onPassword}>
            Set password
          </button>
          <button className="btn btn-sm" onClick={onEmail}>
            Email
          </button>
          <button className="btn btn-sm" disabled={resetLink.isPending} onClick={() => resetLink.mutate(user.id, { onSuccess: onResetLink, onError: fail("Reset link") })}>
            Reset link
          </button>
          {user.mfa_enabled && (
            <button className="btn btn-sm" onClick={() => resetMfa.mutate(user.id, { onSuccess: () => toast("MFA reset"), onError: fail("Reset MFA") })}>
              Reset MFA
            </button>
          )}
        </div>
      </td>
    </tr>
  );
}

// A minted reset link is shown once; hand it to the user out of band.
function ResetLinkPanel({ link, onDone }: { link: PasswordResetLinkView; onDone: () => void }) {
  const toast = useToast();
  const value = link.url ?? link.token;
  return (
    <Panel className="confirm info">
      <h2>Password reset link</h2>
      <p className="sub">Valid until {new Date(link.expires_at).toLocaleString()} — single use.</p>
      <p className="mono" id="reset-link-value" style={{ overflowWrap: "anywhere" }}>
        {value}
      </p>
      <div className="form-actions">
        <button className="btn btn-primary" onClick={() => void navigator.clipboard?.writeText(value).then(() => toast("Copied"))}>
          Copy
        </button>
        <button className="btn" onClick={onDone}>
          Done
        </button>
      </div>
    </Panel>
  );
}

export function Users({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { me } = useAuth();
  const { data, isLoading, isError, error } = useUsers();
  const [link, setLink] = useState<PasswordResetLinkView | null>(null);
  return (
    <>
      <Chips chips={[{ label: "← home", cmd: "" }]} onPrompt={onPrompt} />
      <h2>Users</h2>
      <p className="sub">Instance-level accounts. Tenant memberships are managed with the CLI (`aaiclick member …`).</p>
      {!me?.superadmin && <p className="err">Requires the superadmin flag.</p>}
      {link && <ResetLinkPanel link={link} onDone={() => setLink(null)} />}
      {me?.superadmin && <CreateUserForm />}
      {isLoading && <p className="sub">loading…</p>}
      {isError && <p className="err">{error.message}</p>}
      {data && (
        <table>
          <thead>
            <tr>
              <th>Username</th>
              <th>Email</th>
              <th>Role</th>
              <th>Status</th>
              <th>Flags</th>
              <th>Created</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((u) => (
              <UserRow key={u.id} user={u} self={u.id === me?.id} onResetLink={setLink} />
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
