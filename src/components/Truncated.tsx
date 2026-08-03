import { useState } from "react";

const DEFAULT_MAX = 50;

/**
 * Long value truncated from the *start*, keeping the tail, with click-to-expand.
 *
 * A fully-qualified entrypoint carries its meaning at the end — the module and
 * function name — while the leading package path repeats across every row, so
 * dropping the head loses the least. Click (or Enter/Space) toggles the full
 * value; the untruncated text is always available as a tooltip.
 */
export function Truncated({ text, max = DEFAULT_MAX }: { text: string; max?: number }) {
  const [expanded, setExpanded] = useState(false);

  if (text.length <= max) return <span className="mono">{text}</span>;

  const toggle = () => setExpanded((v) => !v);

  return (
    <span
      className={`mono truncated${expanded ? " is-expanded" : ""}`}
      title={expanded ? text : `${text} — click to expand`}
      role="button"
      tabIndex={0}
      aria-expanded={expanded}
      data-testid="truncated"
      onClick={(e) => {
        // Rows in TasksTable are clickable; expanding must not also navigate.
        e.stopPropagation();
        toggle();
      }}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          e.stopPropagation();
          toggle();
        }
      }}
    >
      {/* -1 leaves room for the ellipsis so the rendered length stays at `max`. */}
      {expanded ? text : `…${text.slice(-(max - 1))}`}
    </span>
  );
}
