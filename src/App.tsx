// See docs/designs/ui.md.
import { useCallback, useEffect, useState } from "react";
import { LiveUpdates } from "./api/events";
import { useAuth } from "./components/Auth";
import { Header } from "./components/Header";
import { Chips } from "./components/Chips";
import { useCompletions } from "./components/commands";
import { parsePrompt, promptFromUrl, PUBLIC_ROUTES, pushPromptToUrl, suggestCommands, type Route } from "./prompt";
import {
  Account,
  Audit,
  CancelConfirm,
  Dashboard,
  Data,
  Home,
  JobDetail,
  Jobs,
  Query,
  Registered,
  RegisterForm,
  RunConfirm,
  RunForm,
  Sandbox,
  SandboxFile,
  TaskDetail,
  ToggleConfirm,
  Tokens,
  Invite,
  Users,
} from "./views";
import { Login } from "./views/Login";
import { ResetPassword } from "./views/ResetPassword";

function UnknownCommand({ raw, onPrompt }: { raw: string; onPrompt: (v: string) => void }) {
  // Argument commands (trailing space) would just land back here, so offer only complete ones.
  const commands = useCompletions()
    .filter((c) => !c.disabled && !c.text.endsWith(" "))
    .map((c) => c.text);
  return (
    <>
      <h2>Unknown command</h2>
      <p className="sub mono">{raw}</p>
      <Chips chips={suggestCommands(raw, commands).map((c) => ({ label: c, cmd: c }))} onPrompt={onPrompt} />
    </>
  );
}

function renderRoute(route: Route, onPrompt: (v: string) => void) {
  switch (route.kind) {
    case "home":
      return <Home onPrompt={onPrompt} />;
    case "jobs":
      return <Jobs onPrompt={onPrompt} />;
    case "registered":
      return <Registered onPrompt={onPrompt} />;
    case "register":
      return <RegisterForm name={route.name} onPrompt={onPrompt} />;
    case "job":
      return <JobDetail name={route.name} view={route.view} onPrompt={onPrompt} />;
    case "task":
      return <TaskDetail id={route.id} onPrompt={onPrompt} />;
    case "run-confirm":
      return <RunConfirm name={route.name} onPrompt={onPrompt} />;
    case "run-form":
      return <RunForm name={route.name} onPrompt={onPrompt} />;
    case "cancel-confirm":
      return <CancelConfirm refId={route.ref} onPrompt={onPrompt} />;
    case "toggle-confirm":
      return <ToggleConfirm name={route.name} enabled={route.enabled} onPrompt={onPrompt} />;
    case "tokens":
      return <Tokens onPrompt={onPrompt} />;
    case "account":
      return <Account onPrompt={onPrompt} />;
    case "users":
      return <Users onPrompt={onPrompt} />;
    case "invite":
      return <Invite onPrompt={onPrompt} />;
    case "audit":
      return <Audit onPrompt={onPrompt} />;
    case "reset":
      return <ResetPassword token={route.token} onDone={() => onPrompt("")} />;
    case "data":
      return <Data scope={route.scope} object={route.object} onPrompt={onPrompt} />;
    case "query":
      return <Query scope={route.scope} object={route.object} onPrompt={onPrompt} />;
    case "dashboard":
      return <Dashboard name={route.name} onPrompt={onPrompt} />;
    case "sandbox":
      return <Sandbox onPrompt={onPrompt} />;
    case "sandbox-file":
      return <SandboxFile id={route.id} onPrompt={onPrompt} />;
    case "unknown":
      return <UnknownCommand raw={route.raw} onPrompt={onPrompt} />;
  }
}

export function App() {
  const { me, ready } = useAuth();
  const [prompt, setPrompt] = useState(promptFromUrl);
  // Bumped on every commit, so the header drops its draft even when the prompt value is unchanged.
  const [commits, setCommits] = useState(0);

  // Stable identity: Header calls this on every keystroke, and JobGraph's node
  // memo depends on it — a fresh function per render rebuilds every node object.
  const onPrompt = useCallback((value: string) => {
    setPrompt(value);
    setCommits((n) => n + 1);
    pushPromptToUrl(value);
  }, []);

  useEffect(() => {
    const onPop = () => setPrompt(promptFromUrl());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  // Wait for the initial /auth/me probe; when auth is disabled the server
  // returns a synthetic admin so `me` is set and no login wall appears.
  if (!ready) return null;
  const route = parsePrompt(prompt);
  if (!me && !PUBLIC_ROUTES.has(route.kind)) return <Login />;
  if (!me) return renderRoute(route, onPrompt);

  return (
    <>
      <LiveUpdates />
      <Header prompt={prompt} commits={commits} onPrompt={onPrompt} />
      <main>
        <div className="content" id="content">
          {renderRoute(route, onPrompt)}
        </div>
      </main>
    </>
  );
}
