// See docs/designs/ui.md — "Home (empty prompt)".
// Single command registry for Home, prompt autocomplete and unknown-command chips.
import { useSandboxConfig } from "../api/hooks";
import { useAuth } from "./Auth";

export interface Cmd {
  /** Syntax shown on Home. */
  code: string;
  desc: string;
  /** Prompt emitted when the Home row is clicked. */
  cmd: string;
  /** Autocomplete text; a trailing space leaves room for the argument. Omit to skip. */
  complete?: string;
}

const NAVIGATE: Cmd[] = [
  { code: "@jobs", desc: "List all jobs, newest first.", cmd: "@jobs", complete: "@jobs" },
  {
    code: "@registered",
    desc: "Registered jobs — run, register, enable/disable.",
    cmd: "@registered",
    complete: "@registered",
  },
  { code: "@job <name>", desc: "Job detail — header + tasks table.", cmd: "@jobs", complete: "@job " },
  { code: "@task <id>", desc: "Task detail — status bar + live logs.", cmd: "@task 1", complete: "@task " },
  {
    code: "@data [persistent | job <ref>] [<object>]",
    desc: "Objects of every scope, persistent, or one job — and their rows.",
    cmd: "@data",
    complete: "@data",
  },
  {
    code: "@query [persistent | job <ref>] [<object>]",
    desc: "Filter, order, page, and save a query over one object.",
    cmd: "@query",
    complete: "@query",
  },
  {
    code: "@dashboard [name]",
    desc: "Render a saved dashboard over object queries.",
    cmd: "@dashboard",
    complete: "@dashboard",
  },
];

const SANDBOX: Cmd = {
  code: "@sandbox",
  desc: "Submit a single-file job and run it once.",
  cmd: "@sandbox",
  complete: "@sandbox",
};

const ACTIONS: Cmd[] = [
  {
    code: "run <name>",
    desc: "Run a registered job, or any module.function entrypoint. Add ? to edit params.",
    cmd: "@registered",
    complete: "run ",
  },
  { code: "cancel <name|id>", desc: "Cancel a pending/running job.", cmd: "@jobs", complete: "cancel " },
  {
    code: "register",
    desc: "Register a new job (entrypoint, schedule, kwargs).",
    cmd: "register",
    complete: "register",
  },
  { code: "enable / disable <name>", desc: "Toggle a registered job on/off via @registered.", cmd: "@registered" },
];

const ACCOUNT: Cmd[] = [
  {
    code: "@account",
    desc: "Change your password, set up multi-factor auth.",
    cmd: "@account",
    complete: "@account",
  },
  {
    code: "@tokens",
    desc: "API tokens for CLI / SDK / MCP clients — create, revoke.",
    cmd: "@tokens",
    complete: "@tokens",
  },
];

const ADMIN: Cmd[] = [
  {
    code: "@users",
    desc: "Manage users — create, set role, disable, reset password.",
    cmd: "@users",
    complete: "@users",
  },
  {
    code: "@invite",
    desc: "Create an account with a one-time set-password link.",
    cmd: "@invite",
    complete: "@invite",
  },
  { code: "@audit", desc: "Request audit log — who called what, when.", cmd: "@audit", complete: "@audit" },
];

export const ALL_COMMANDS: Cmd[] = [...NAVIGATE, SANDBOX, ...ACTIONS, ...ACCOUNT, ...ADMIN];

export interface CommandGroups {
  navigate: Cmd[];
  actions: Cmd[];
  /** Need real user accounts — disabled in local mode. */
  account: Cmd[];
  /** Empty for non-admins; also disabled in local mode. */
  admin: Cmd[];
  /** Auth disabled: /auth/me returns a synthetic admin with no user row. */
  local: boolean;
}

export function useCommandGroups(): CommandGroups {
  const { me } = useAuth();
  const sandbox = useSandboxConfig();
  return {
    navigate: sandbox.data?.enabled ? [...NAVIGATE, SANDBOX] : NAVIGATE,
    actions: ACTIONS,
    account: ACCOUNT,
    admin: me?.role === "admin" ? ADMIN : [],
    local: me !== null && me.id === null,
  };
}

export interface Completion {
  text: string;
  disabled: boolean;
}

// Autocomplete entries, `help` first; disabled ones only work in distributed mode.
export function useCompletions(): Completion[] {
  const g = useCommandGroups();
  const entry = (disabled: boolean) => (c: Cmd) => (c.complete ? [{ text: c.complete, disabled }] : []);
  return [
    { text: "help", disabled: false },
    ...[...g.navigate, ...g.actions].flatMap(entry(false)),
    ...[...g.account, ...g.admin].flatMap(entry(g.local)),
  ];
}
