import { useSandboxConfig } from "../api/hooks";
import { ADMIN_COMMANDS, ARG_COMMANDS, BARE_COMMANDS, DIST_ONLY_COMMANDS } from "../prompt";
import { useAuth } from "./Auth";

interface HeaderProps {
  prompt: string;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, onPrompt }: HeaderProps) {
  const { me, signOut } = useAuth();
  const sandbox = useSandboxConfig();
  // Local mode (auth disabled): /auth/me returns a synthetic admin with no user row.
  const local = me !== null && me.id === null;
  const commands = [
    ...BARE_COMMANDS,
    ...ARG_COMMANDS,
    ...(sandbox.data?.enabled ? ["@sandbox"] : []),
    ...(me?.role === "admin" ? ADMIN_COMMANDS : []),
  ];
  return (
    <header>
      <button type="button" className="logo" id="header-home" onClick={() => onPrompt("")}>
        aai<span>click</span>
      </button>
      <div className="prompt-wrap">
        <input
          id="prompt"
          value={prompt}
          onChange={(e) => onPrompt(e.target.value)}
          placeholder="Type a command…  try @jobs, run <registered job>, or help"
          autoComplete="off"
          spellCheck={false}
          list="prompt-commands"
        />
        <datalist id="prompt-commands">
          {/* Native datalist options can't be grayed out; a label marks them instead. */}
          {commands.map((c) => (
            <option
              key={c}
              value={c}
              label={local && DIST_ONLY_COMMANDS.has(c) ? `${c} (Distributed mode only)` : undefined}
            />
          ))}
        </datalist>
      </div>
      {me?.username ? (
        <div className="hint user-menu">
          <span className="name-link" id="header-username" onClick={() => onPrompt("@account")}>
            {me.username}
          </span>
          <button id="header-signout" className="btn btn-sm" onClick={() => void signOut()}>
            Sign out
          </button>
        </div>
      ) : (
        <div className="hint">prompt drives the view ↑</div>
      )}
    </header>
  );
}
