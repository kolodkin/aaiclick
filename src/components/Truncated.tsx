import { useLayoutEffect, useRef, useState } from "react";

import { useToast } from "./Toast";

const DEFAULT_MAX = 50;

const ICON_PROPS = {
  width: 14,
  height: 14,
  viewBox: "0 0 16 16",
  fill: "none",
  stroke: "currentColor",
  strokeWidth: 1.5,
  strokeLinecap: "round",
  strokeLinejoin: "round",
  "aria-hidden": true,
} as const;

function ExpandIcon({ expanded }: { expanded: boolean }) {
  return (
    <svg {...ICON_PROPS}>
      {expanded ? <path d="M4 10l4-4 4 4" /> : <path d="M4 6l4 4 4-4" />}
    </svg>
  );
}

function CopyIcon() {
  return (
    <svg {...ICON_PROPS}>
      <rect x="5.5" y="5.5" width="8" height="8" rx="1.5" />
      <path d="M10.5 3.5v-.5a1 1 0 0 0-1-1h-6a1 1 0 0 0-1 1v6a1 1 0 0 0 1 1h.5" />
    </svg>
  );
}

/**
 * Long value shown on one line with its *head* elided, plus icon buttons to
 * show it in full and to copy it.
 *
 * A fully-qualified entrypoint carries its meaning at the end — the module and
 * function name — while the leading package path repeats across every row, so
 * dropping the head loses the least. The elision is done in CSS
 * (`direction: rtl` + `text-overflow: ellipsis`) rather than by slicing the
 * string, so it always fits the column exactly instead of guessing a character
 * count and still wrapping.
 *
 * The expand toggle appears only while the value is actually clipped (or
 * expanded), so a wide column never offers a button that does nothing. `max`
 * decides only whether a value is long enough to be worth measuring.
 */
export function Truncated({ text, max = DEFAULT_MAX }: { text: string; max?: number }) {
  const [expanded, setExpanded] = useState(false);
  const toast = useToast();
  const [clipped, setClipped] = useState(false);
  const valueRef = useRef<HTMLSpanElement>(null);
  const long = text.length > max;

  useLayoutEffect(() => {
    const el = valueRef.current;
    if (!long || expanded || !el) return;
    const measure = () => setClipped(el.scrollWidth > el.clientWidth);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [long, expanded, text]);

  return (
    <span className="truncated-wrap">
      <span
        className={`mono truncated${long && !expanded ? " is-collapsed" : ""}${expanded ? " is-expanded" : ""}`}
        title={text}
        ref={valueRef}
        data-testid="truncated"
      >
        {text}
      </span>
      {/* Rows in TasksTable navigate on click; these buttons must not also leave. */}
      <span className="truncated-actions">
        {long && (clipped || expanded) && (
          <button
            type="button"
            className="icon-btn"
            aria-expanded={expanded}
            aria-label={expanded ? "Show less" : "Show all"}
            title={expanded ? "Show less" : "Show all"}
            data-testid="truncated-toggle"
            onClick={(e) => {
              e.stopPropagation();
              setExpanded((v) => !v);
            }}
          >
            <ExpandIcon expanded={expanded} />
          </button>
        )}
        <button
          type="button"
          className="icon-btn"
          aria-label="Copy"
          title="Copy"
          data-testid="truncated-copy"
          onClick={(e) => {
            e.stopPropagation();
            void navigator.clipboard?.writeText(text).then(() => toast("Copied"));
          }}
        >
          <CopyIcon />
        </button>
      </span>
    </span>
  );
}
