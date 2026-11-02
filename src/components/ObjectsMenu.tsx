import { useEffect, useRef } from "react";
import type { MouseEvent } from "react";
import { useObjects } from "../api/hooks";
import { dataPrompt, scopeKey } from "../lib/viewer";
import { promptHref } from "../prompt";

// A job's objects as a small popup of links: a plain click switches the view,
// while ctrl/middle-click, long-press or the ↗ open the object in a new tab.
// Ids are 64-bit snowflakes carried as strings — never parse them.
export function ObjectsMenu({ jobId, onPrompt }: { jobId: string; onPrompt: (v: string) => void }) {
  const scope = scopeKey(jobId);
  const { data } = useObjects(scope);
  const ref = useRef<HTMLDetailsElement>(null);

  // Close on a click anywhere outside the popup.
  useEffect(() => {
    const close = (e: PointerEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) ref.current.open = false;
    };
    document.addEventListener("pointerdown", close);
    return () => document.removeEventListener("pointerdown", close);
  }, []);

  const items = data?.items ?? [];
  if (items.length === 0) return null;

  const go = (e: MouseEvent, cmd: string) => {
    if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    e.preventDefault();
    if (ref.current) ref.current.open = false;
    onPrompt(cmd);
  };

  return (
    <details className="objects-menu" ref={ref} data-testid="job-objects">
      <summary className="btn btn-sm">Objects ({items.length}) ▾</summary>
      <ul role="menu">
        {items.map((o) => {
          const cmd = dataPrompt(scope, o.name);
          return (
            <li key={o.name} role="none">
              <a role="menuitem" className="mono" href={promptHref(cmd)} onClick={(e) => go(e, cmd)}>
                {o.name}
              </a>
              <a
                className="new-tab"
                href={promptHref(cmd)}
                target="_blank"
                rel="noopener"
                aria-label={`Open ${o.name} in a new tab`}
                title="Open in a new tab"
              >
                ↗
              </a>
            </li>
          );
        })}
      </ul>
    </details>
  );
}
