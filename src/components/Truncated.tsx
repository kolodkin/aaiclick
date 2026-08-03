import { useState } from "react";

const DEFAULT_MAX = 50;

/**
 * Long value shown on one line with its *head* elided, plus a visible
 * show full / show less toggle.
 *
 * A fully-qualified entrypoint carries its meaning at the end — the module and
 * function name — while the leading package path repeats across every row, so
 * dropping the head loses the least. The elision is done in CSS
 * (`direction: rtl` + `text-overflow: ellipsis`) rather than by slicing the
 * string, so it always fits the column exactly instead of guessing a character
 * count and still wrapping.
 *
 * `max` decides only whether a value is long enough to bother truncating.
 */
export function Truncated({ text, max = DEFAULT_MAX }: { text: string; max?: number }) {
  const [expanded, setExpanded] = useState(false);

  if (text.length <= max) return <span className="mono">{text}</span>;

  return (
    <span className="truncated-wrap">
      <span className={`mono truncated${expanded ? " is-expanded" : ""}`} title={text} data-testid="truncated">
        {text}
      </span>
      <button
        type="button"
        className="truncated-toggle"
        aria-expanded={expanded}
        data-testid="truncated-toggle"
        onClick={(e) => {
          // Rows in TasksTable navigate on click; expanding must not also leave.
          e.stopPropagation();
          setExpanded((v) => !v);
        }}
      >
        {expanded ? "show less" : "show full"}
      </button>
    </span>
  );
}
