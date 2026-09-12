import { useAuth } from "./Auth";

interface HeaderProps {
  prompt: string;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, onPrompt }: HeaderProps) {
  const { me, signOut } = useAuth();
  return (
    <header>
      <div className="logo">
        aai<span>click</span>
      </div>
      <div className="prompt-wrap">
        <input
          id="prompt"
          value={prompt}
          onChange={(e) => onPrompt(e.target.value)}
          placeholder="Type a command…  try @jobs, @registered, or run nyc_taxi_pipeline"
          autoComplete="off"
          spellCheck={false}
        />
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
