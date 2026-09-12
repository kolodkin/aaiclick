import { useState } from "react";
import { Panel } from "../components/Panel";
import { redeemPasswordReset } from "../lib/auth";

// Opened from a reset link (`?p=reset <token>`) — no session exists yet.
export function ResetPassword({ token, onDone }: { token: string; onDone: () => void }) {
  const [password, setPassword] = useState("");
  const [again, setAgain] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [done, setDone] = useState(false);
  const [busy, setBusy] = useState(false);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (password !== again) {
      setError("Passwords do not match");
      return;
    }
    setBusy(true);
    setError(null);
    try {
      await redeemPasswordReset(token, password);
      setDone(true);
    } catch {
      setError("This reset link is invalid, expired, or already used");
    } finally {
      setBusy(false);
    }
  };

  return (
    <main>
      <div className="content" id="content">
        <Panel>
          <h2>Choose a new password</h2>
          {done ? (
            <>
              <p className="sub">Password updated. Sign in with it now.</p>
              <div className="form-actions">
                <button id="reset-done" className="btn btn-primary" onClick={onDone}>
                  Go to sign in
                </button>
              </div>
            </>
          ) : (
            <form onSubmit={submit}>
              <div className="field">
                <label>New password</label>
                <input id="reset-password" type="password" value={password} autoFocus onChange={(e) => setPassword(e.target.value)} />
              </div>
              <div className="field">
                <label>Repeat new password</label>
                <input id="reset-again" type="password" value={again} onChange={(e) => setAgain(e.target.value)} />
              </div>
              {error && <p className="err">{error}</p>}
              <div className="form-actions">
                <button id="reset-submit" className="btn btn-primary" type="submit" disabled={busy || !password}>
                  Set password
                </button>
              </div>
            </form>
          )}
        </Panel>
      </div>
    </main>
  );
}
