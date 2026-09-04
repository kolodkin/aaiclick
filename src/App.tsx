import { useCallback, useEffect, useState } from "react";
import { useAuth } from "./components/Auth";
import { Header } from "./components/Header";
import { parsePrompt, promptFromUrl, pushPromptToUrl } from "./prompt";
import {
  AllGallery,
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
} from "./views";
import { Login } from "./views/Login";

function renderRoute(prompt: string, onPrompt: (v: string) => void) {
  const route = parsePrompt(prompt);
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
  if (!me) return <Login />;

  return (
    <>
      <Header prompt={prompt} onPrompt={onPrompt} />
      <main>
        <div className="content" id="content">
          {renderRoute(prompt, onPrompt)}
        </div>
      </main>
    </>
  );
}
