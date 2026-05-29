interface Chip {
  label: string;
  cmd: string;
}

export function Chips({ chips, onPrompt }: { chips: Chip[]; onPrompt: (v: string) => void }) {
  return (
    <div className="chips">
      {chips.map((c) => (
        <span key={c.label} className="chip" onClick={() => onPrompt(c.cmd)}>
          {c.label}
        </span>
      ))}
    </div>
  );
}
