import { useAuth } from "./Auth";
import { useCompletions } from "./commands";

interface HeaderProps {
  prompt: string;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, onPrompt }: HeaderProps) {
  const { me, signOut } = useAuth();
  const completions = useCompletions();
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
          {completions.map((c) => (
            <option key={c.text} value={c.text} label={c.disabled ? `${c.text} (Distributed mode only)` : undefined} />
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
