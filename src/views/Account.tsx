import { useState } from "react";
import { useChangePassword, useMfaDisable, useMfaEnable, useMfaSetup } from "../api/hooks";
import type { MfaSetupView } from "../api/types";
import { useAuth } from "../components/Auth";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

function PasswordForm() {
  const change = useChangePassword();
  const { signOut } = useAuth();
  const toast = useToast();
  const [current, setCurrent] = useState("");
  const [next, setNext] = useState("");
  const [again, setAgain] = useState("");

  const submit = () => {
    if (next !== again) {
      toast("New passwords do not match");
      return;
    }
    change.mutate(
      { current_password: current, new_password: next },
      {
        // The server revokes every session on a password change, this one
        // included — sign out cleanly instead of waiting for the next 401.
        onSuccess: () => {
          toast("Password changed — sign in again");
          void signOut();
        },
        onError: (e) => toast(`Change failed: ${e.message}`),
      },
    );
  };

  return (
    <Panel>
      <h2>Change password</h2>
      <p className="sub">All your sessions are signed out afterwards, including this one.</p>
      <div className="field">
        <label>Current password</label>
        <input id="pw-current" type="password" value={current} onChange={(e) => setCurrent(e.target.value)} />
      </div>
      <div className="field">
        <label>New password</label>
        <input id="pw-new" type="password" value={next} onChange={(e) => setNext(e.target.value)} />
      </div>
      <div className="field">
        <label>Repeat new password</label>
        <input id="pw-again" type="password" value={again} onChange={(e) => setAgain(e.target.value)} />
      </div>
      <div className="form-actions">
        <button id="pw-submit" className="btn btn-primary" disabled={change.isPending || !current || !next} onClick={submit}>
          Change password
        </button>
      </div>
    </Panel>
  );
}

function MfaPanel() {
  const { me, refresh } = useAuth();
  const toast = useToast();
  const setup = useMfaSetup();
  const enable = useMfaEnable();
  const disable = useMfaDisable();
  const [pending, setPending] = useState<MfaSetupView | null>(null);
  const [code, setCode] = useState("");
  const [password, setPassword] = useState("");

  const onSetup = () =>
    setup.mutate(undefined, { onSuccess: setPending, onError: (e) => toast(`Setup failed: ${e.message}`) });
  const onEnable = () =>
    enable.mutate(code, {
      onSuccess: () => {
        toast("MFA enabled — other sessions were signed out");
        setPending(null);
        setCode("");
        void refresh();
      },
      onError: (e) => toast(`Enable failed: ${e.message}`),
    });
  const onDisable = () =>
    disable.mutate(
      { password, code },
      {
        onSuccess: () => {
          toast("MFA disabled");
          setCode("");
          setPassword("");
          void refresh();
        },
        onError: (e) => toast(`Disable failed: ${e.message}`),
      },
    );

  if (me?.mfa_enabled) {
    return (
      <Panel>
        <h2>
          Multi-factor auth <span className="badge b-COMPLETED">enabled</span>
        </h2>
        <p className="sub">Turning it off needs your password and a current code.</p>
        <div className="field">
          <label>Password</label>
          <input id="mfa-password" type="password" value={password} onChange={(e) => setPassword(e.target.value)} />
        </div>
        <div className="field">
          <label>Authenticator code</label>
          <input id="mfa-code" type="text" value={code} onChange={(e) => setCode(e.target.value)} />
        </div>
        <div className="form-actions">
          <button id="mfa-disable" className="btn btn-danger" disabled={disable.isPending || !password || !code} onClick={onDisable}>
            Disable MFA
          </button>
        </div>
      </Panel>
    );
  }

  return (
    <Panel>
      <h2>
        Multi-factor auth <span className="badge b-PENDING">off</span>
      </h2>
      {!pending && (
        <>
          <p className="sub">Add a time-based code from an authenticator app to every password login.</p>
          <div className="form-actions">
            <button id="mfa-setup" className="btn btn-primary" disabled={setup.isPending} onClick={onSetup}>
              Set up MFA
            </button>
          </div>
        </>
      )}
      {pending && (
        <>
          <p className="sub">Add this secret to your authenticator app, then confirm with a code it shows.</p>
          <p className="mono" id="mfa-secret" style={{ overflowWrap: "anywhere" }}>
            {pending.secret}
          </p>
          <p className="mono" style={{ overflowWrap: "anywhere", fontSize: 11 }}>
            {pending.otpauth_uri}
          </p>
          <div className="field">
            <label>Authenticator code</label>
            <input id="mfa-code" type="text" value={code} autoComplete="one-time-code" onChange={(e) => setCode(e.target.value)} />
          </div>
          <div className="form-actions">
            <button id="mfa-enable" className="btn btn-primary" disabled={enable.isPending || !code} onClick={onEnable}>
              Enable MFA
            </button>
            <button className="btn" onClick={() => setPending(null)}>
              Cancel
            </button>
          </div>
        </>
      )}
    </Panel>
  );
}

export function Account({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { me } = useAuth();
  return (
    <>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@tokens", cmd: "@tokens" },
        ]}
        onPrompt={onPrompt}
      />
      <h2>Account</h2>
      <p className="sub">
        Signed in as <span className="mono">{me?.username ?? "(local mode — no user)"}</span>
        {me?.superadmin ? " · superadmin" : ""}
      </p>
      {me?.username ? (
        <>
          <PasswordForm />
          <MfaPanel />
        </>
      ) : (
        <p className="sub">Auth is disabled in local mode; there is no account to manage.</p>
      )}
    </>
  );
}
