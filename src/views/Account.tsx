import { useState } from "react";
import { useChangePassword } from "../api/hooks";
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
      {me?.username ? <PasswordForm /> : <p className="sub">Auth is disabled in local mode; there is no account to manage.</p>}
    </>
  );
}
