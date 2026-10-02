import { useLayoutEffect, useRef, useState } from "react";
import { useToast } from "./Toast";

/**
 * Long value shown on one line with its *head* elided, plus a visible
 * show full / show less toggle and an optional copy button.
 *
 * A fully-qualified entrypoint carries its meaning at the end — the module and
 * function name — while the leading package path repeats across every row, so
 * dropping the head loses the least. The elision is done in CSS
 * (`direction: rtl` + `text-overflow: ellipsis`) rather than by slicing the
 * string, so it always fits the column exactly instead of guessing a character
 * count and still wrapping.
 *
 * The toggle appears only while the value is actually clipped, measured
 * against the rendered width, so a value that fits its column shows no toggle.
 */
export function Truncated({ text, copyable = false }: { text: string; copyable?: boolean }) {
  const [expanded, setExpanded] = useState(false);
  const [clipped, setClipped] = useState(false);
  const ref = useRef<HTMLSpanElement>(null);
  const toast = useToast();

  useLayoutEffect(() => {
    const el = ref.current;
    if (!el || expanded) return;
    const measure = () => setClipped(el.scrollWidth > el.clientWidth);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, [text, expanded]);

  return (
    <span className="truncated-wrap">
      <span className="truncated-line">
        <span
          ref={ref}
          className={`mono truncated${expanded ? " is-expanded" : ""}`}
          title={text}
          data-testid="truncated"
        >
          {text}
        </span>
        {copyable && (
          <button
            type="button"
            className="truncated-copy"
            aria-label="Copy"
            title="Copy"
            data-testid="truncated-copy"
            onClick={(e) => {
              e.stopPropagation();
              void navigator.clipboard?.writeText(text).then(() => toast("Copied"));
            }}
          >
            <svg viewBox="0 0 16 16" width="13" height="13" aria-hidden="true">
              <rect x="5" y="5" width="9" height="9" rx="1.5" fill="none" stroke="currentColor" strokeWidth="1.4" />
              <path d="M11 5V3.5A1.5 1.5 0 0 0 9.5 2h-6A1.5 1.5 0 0 0 2 3.5v6A1.5 1.5 0 0 0 3.5 11H5" fill="none" stroke="currentColor" strokeWidth="1.4" />
            </svg>
          </button>
        )}
      </span>
      {(clipped || expanded) && (
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
      )}
    </span>
  );
}
