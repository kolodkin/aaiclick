import { useAuth } from "../components/Auth";

interface Cmd {
  code: string;
  desc: string;
  cmd: string;
}

const NAVIGATE: Cmd[] = [
  { code: "@jobs", desc: "List all jobs, newest first.", cmd: "@jobs" },
  { code: "@registered", desc: "Registered jobs — run, register, enable/disable.", cmd: "@registered" },
  { code: "@job <name>", desc: "Job detail — header + tasks table.", cmd: "@job nyc_taxi_pipeline" },
  { code: "@task <id>", desc: "Task detail — status bar + live logs.", cmd: "@task 1" },
  { code: "@all", desc: "Every screen on one page.", cmd: "@all" },
];

const ACTIONS: Cmd[] = [
  { code: "run <name>", desc: "Run a registered job with defaults. Add ? to edit params.", cmd: "run nyc_taxi_pipeline" },
  { code: "cancel <name|id>", desc: "Cancel a pending/running job.", cmd: "cancel nyc_taxi_pipeline" },
  { code: "register", desc: "Register a new job (entrypoint, schedule, kwargs).", cmd: "register" },
  { code: "enable / disable <name>", desc: "Toggle a registered job on/off via @registered.", cmd: "@registered" },
];

const ACCOUNT: Cmd[] = [
  { code: "@account", desc: "Change your password, set up multi-factor auth.", cmd: "@account" },
  { code: "@tokens", desc: "API tokens for CLI / SDK / MCP clients — create, revoke.", cmd: "@tokens" },
];

const ADMIN: Cmd[] = [
  { code: "@users", desc: "Manage users — create, superadmin, disable, reset password.", cmd: "@users" },
];

function CmdList({ items, onPrompt }: { items: Cmd[]; onPrompt: (v: string) => void }) {
  return (
    <div className="cmd-list">
      {items.map((c) => (
        <div key={c.code} className="cmd" onClick={() => onPrompt(c.cmd)}>
          <code>{c.code}</code>
          <span className="desc">{c.desc}</span>
        </div>
      ))}
    </div>
  );
}

export function Home({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { me } = useAuth();
  return (
    <>
      <h2>aaiclick</h2>
      <p className="sub">Prompt-driven operator dashboard. Type a command above, or click one below.</p>
      <div className="group-label">Navigate</div>
      <CmdList items={NAVIGATE} onPrompt={onPrompt} />
      <div className="group-label">Actions</div>
      <CmdList items={ACTIONS} onPrompt={onPrompt} />
      <div className="group-label">Account</div>
      <CmdList items={ACCOUNT} onPrompt={onPrompt} />
      {me?.superadmin && (
        <>
          <div className="group-label">Administration</div>
          <CmdList items={ADMIN} onPrompt={onPrompt} />
        </>
      )}
    </>
  );
}
