interface Chip {
  label: string;
  cmd: string;
}

export function Chips({ chips, onPrompt, children }: { chips: Chip[]; onPrompt: (v: string) => void; children?: React.ReactNode }) {
  return (
    <div className="chips">
      {chips.map((c) => (
        <span key={c.label} className="chip" onClick={() => onPrompt(c.cmd)}>
          {c.label}
        </span>
      ))}
      {children}
    </div>
  );
}
