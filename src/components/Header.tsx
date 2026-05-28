interface HeaderProps {
  prompt: string;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, onPrompt }: HeaderProps) {
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
      <div className="hint">prompt drives the view ↑</div>
    </header>
  );
}
