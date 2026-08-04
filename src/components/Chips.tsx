interface Chip {
  label: string;
  /** Prompt to emit on click. Omit when `onClick` drives local state instead. */
  cmd?: string;
  onClick?: () => void;
  active?: boolean;
  testId?: string;
}

export function Chips({ chips, onPrompt }: { chips: Chip[]; onPrompt?: (v: string) => void }) {
  return (
    <div className="chips">
      {chips.map((c) => (
        <span
          key={c.label}
          className={`chip${c.active ? " chip-active" : ""}`}
          data-testid={c.testId}
          onClick={() => (c.onClick ? c.onClick() : c.cmd && onPrompt?.(c.cmd))}
        >
          {c.label}
        </span>
      ))}
    </div>
  );
}
