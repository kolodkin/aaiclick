import { useEffect, useState } from "react";
import { Panel } from "../components/Panel";
import { useAuth } from "../components/Auth";
import { fetchOidcConfig, login, LoginError, startOidcLogin, type OidcConfig } from "../lib/auth";

export function Login() {
  const { refresh } = useAuth();
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [totp, setTotp] = useState("");
  const [needsCode, setNeedsCode] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [oidc, setOidc] = useState<OidcConfig | null>(null);

  useEffect(() => {
    void fetchOidcConfig().then(setOidc);
  }, []);

  const sso = async () => {
    setBusy(true);
    try {
      await startOidcLogin();
    } catch {
      setError("SSO is not available right now");
      setBusy(false);
    }
  };

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      await login(username, password, needsCode ? totp : undefined);
      await refresh();
    } catch (err) {
      if (err instanceof LoginError && err.code === "mfa_required") {
        setNeedsCode(true);
      } else {
        setError(needsCode ? "Invalid username, password, or code" : "Invalid username or password");
      }
    } finally {
      setBusy(false);
    }
  };

  return (
    <main>
      <div className="content" id="content">
        <Panel>
          <h2>Sign in</h2>
          <p className="sub">Enter your aaiclick credentials.</p>
          <form onSubmit={submit}>
            <div className="field">
              <label>Username</label>
              <input
                id="login-username"
                type="text"
                value={username}
                autoFocus
                onChange={(e) => setUsername(e.target.value)}
              />
            </div>
            <div className="field">
              <label>Password</label>
              <input
                id="login-password"
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
              />
            </div>
            {needsCode && (
              <div className="field">
                <label>
                  Authenticator code <span className="help">— six digits from your app</span>
                </label>
                <input
                  id="login-totp"
                  type="text"
                  value={totp}
                  autoFocus
                  autoComplete="one-time-code"
                  onChange={(e) => setTotp(e.target.value)}
                />
              </div>
            )}
            {error && <p className="err">{error}</p>}
            <div className="form-actions">
              <button id="login-submit" className="btn btn-primary" type="submit" disabled={busy}>
                {busy ? "Signing in…" : "Sign in"}
              </button>
              {oidc?.enabled && (
                <button id="login-sso" className="btn" type="button" disabled={busy} onClick={() => void sso()}>
                  Sign in with {oidc.label}
                </button>
              )}
            </div>
          </form>
        </Panel>
      </div>
    </main>
  );
}
