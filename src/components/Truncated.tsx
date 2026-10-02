import { useLayoutEffect, useRef, useState } from "react";

import { useCopy } from "./Toast";

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
 * One-line value with its *head* elided, plus show-all and copy icon buttons.
 *
 * An entrypoint carries its meaning at the end (module and function), while
 * the package prefix repeats across rows, so dropping the head loses least.
 * CSS does the elision (`direction: rtl` + `text-overflow: ellipsis`), so it
 * fits the column exactly instead of guessing a character count.
 *
 * The show-all toggle appears only while the value is clipped (or expanded).
 */
export function Truncated({ text }: { text: string }) {
  const [expanded, setExpanded] = useState(false);
  const [clipped, setClipped] = useState(false);
  const valueRef = useRef<HTMLSpanElement>(null);
  const copy = useCopy();

  useLayoutEffect(() => {
    const el = valueRef.current;
    if (expanded || !el) return;
    const measure = () => setClipped(el.scrollWidth > el.clientWidth);
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    return () => observer.disconnect();
  }, [expanded, text]);

  return (
    <span className="truncated-wrap">
      <span
        className={`mono truncated${expanded ? "" : " is-collapsed"}`}
        title={text}
        ref={valueRef}
        data-testid="truncated"
      >
        {text}
      </span>
      {/* Rows in TasksTable navigate on click; these buttons must not also leave. */}
      <span className="truncated-actions">
        {(clipped || expanded) && (
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
            copy(text);
          }}
        >
          <CopyIcon />
        </button>
      </span>
    </span>
  );
}
