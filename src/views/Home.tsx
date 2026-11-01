// See docs/designs/ui.md — "Home (empty prompt)".
import { useSandboxConfig } from "../api/hooks";
import { useAuth } from "../components/Auth";

interface Cmd {
  code: string;
  desc: string;
  cmd: string;
}

const SANDBOX: Cmd = { code: "@sandbox", desc: "Submit a single-file job and run it once.", cmd: "@sandbox" };

const NAVIGATE: Cmd[] = [
  { code: "@jobs", desc: "List all jobs, newest first.", cmd: "@jobs" },
  { code: "@registered", desc: "Registered jobs — run, register, enable/disable.", cmd: "@registered" },
  { code: "@job <name>", desc: "Job detail — header + tasks table.", cmd: "@jobs" },
  { code: "@task <id>", desc: "Task detail — status bar + live logs.", cmd: "@task 1" },
  {
    code: "@data [job <ref>] [<object>]",
    desc: "Objects of a scope — persistent or one job — and their rows.",
    cmd: "@data",
  },
  {
    code: "@query [job <ref>] [<object>]",
    desc: "Filter, order, page, and save a query over one object.",
    cmd: "@query",
  },
  { code: "@dashboard [name]", desc: "Render a saved dashboard over object queries.", cmd: "@dashboard" },
  { code: "@all", desc: "Every screen on one page.", cmd: "@all" },
];

const ACTIONS: Cmd[] = [
  {
    code: "run <name>",
    desc: "Run a registered job, or any module.function entrypoint. Add ? to edit params.",
    cmd: "@registered",
  },
  { code: "cancel <name|id>", desc: "Cancel a pending/running job.", cmd: "@jobs" },
  { code: "register", desc: "Register a new job (entrypoint, schedule, kwargs).", cmd: "register" },
  { code: "enable / disable <name>", desc: "Toggle a registered job on/off via @registered.", cmd: "@registered" },
];

const ACCOUNT: Cmd[] = [
  { code: "@account", desc: "Change your password, set up multi-factor auth.", cmd: "@account" },
  { code: "@tokens", desc: "API tokens for CLI / SDK / MCP clients — create, revoke.", cmd: "@tokens" },
];

const ADMIN: Cmd[] = [
  { code: "@users", desc: "Manage users — create, set role, disable, reset password.", cmd: "@users" },
  { code: "@audit", desc: "Request audit log — who called what, when.", cmd: "@audit" },
];

function CmdList({
  items,
  onPrompt,
  disabled = false,
}: {
  items: Cmd[];
  onPrompt: (v: string) => void;
  disabled?: boolean;
}) {
  return (
    <div className="cmd-list">
      {items.map((c) => (
        <div
          key={c.code}
          className={disabled ? "cmd disabled" : "cmd"}
          aria-disabled={disabled}
          onClick={disabled ? undefined : () => onPrompt(c.cmd)}
        >
          <code>{c.code}</code>
          <span className="desc">{c.desc}</span>
        </div>
      ))}
    </div>
  );
}

export function Home({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { me } = useAuth();
  const sandbox = useSandboxConfig();
  const navigate = sandbox.data?.enabled ? [...NAVIGATE, SANDBOX] : NAVIGATE;
  // Local mode (auth disabled): /auth/me returns a synthetic admin with no user row.
  const local = me !== null && me.id === null;
  const distOnly = local && <span className="dist-only"> (Distributed mode only)</span>;
  return (
    <>
      <h2>aaiclick</h2>
      <p className="sub">Prompt-driven operator dashboard. Type a command above, or click one below.</p>
      <div className="group-label">Navigate</div>
      <CmdList items={navigate} onPrompt={onPrompt} />
      <div className="group-label">Actions</div>
      <CmdList items={ACTIONS} onPrompt={onPrompt} />
      <div className="group-label">Account{distOnly}</div>
      <CmdList items={ACCOUNT} onPrompt={onPrompt} disabled={local} />
      {me?.role === "admin" && (
        <>
          <div className="group-label">Administration{distOnly}</div>
          <CmdList items={ADMIN} onPrompt={onPrompt} disabled={local} />
        </>
      )}
    </>
  );
}
