import { useCallback, useEffect, useState } from "react";
import { useAuth } from "./components/Auth";
import { Header } from "./components/Header";
import { parsePrompt, promptFromUrl, PUBLIC_ROUTES, pushPromptToUrl, type Route } from "./prompt";
import {
  Account,
  AllGallery,
  Audit,
  CancelConfirm,
  Home,
  JobDetail,
  Jobs,
  Registered,
  RegisterForm,
  RunConfirm,
  RunForm,
  TaskDetail,
  Tokens,
  Users,
} from "./views";
import { Login } from "./views/Login";
import { ResetPassword } from "./views/ResetPassword";

function renderRoute(route: Route, onPrompt: (v: string) => void) {
  switch (route.kind) {
    case "home":
      return <Home onPrompt={onPrompt} />;
    case "all":
      return <AllGallery onPrompt={onPrompt} />;
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
    case "tokens":
      return <Tokens onPrompt={onPrompt} />;
    case "account":
      return <Account onPrompt={onPrompt} />;
    case "users":
      return <Users onPrompt={onPrompt} />;
    case "audit":
      return <Audit onPrompt={onPrompt} />;
    case "reset":
      return <ResetPassword token={route.token} onDone={() => onPrompt("")} />;
    case "unknown":
      return (
        <>
          <h2>Unknown command</h2>
          <p className="sub mono">{route.raw}</p>
          <div className="chips">
            <span className="chip" onClick={() => onPrompt("")}>
              help
            </span>
            <span className="chip" onClick={() => onPrompt("@jobs")}>
              @jobs
            </span>
          </div>
        </>
      );
  }
}

export function App() {
  const { me, ready } = useAuth();
  const [prompt, setPrompt] = useState(promptFromUrl);

  // Stable identity: Header calls this on every keystroke, and JobGraph's node
  // memo depends on it — a fresh function per render rebuilds every node object.
  const onPrompt = useCallback((value: string) => {
    setPrompt(value);
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
      <Header prompt={prompt} onPrompt={onPrompt} />
      <main>
        <div className="content" id="content">
          {renderRoute(route, onPrompt)}
        </div>
      </main>
    </>
  );
}
