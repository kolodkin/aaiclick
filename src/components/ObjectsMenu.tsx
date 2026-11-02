import { useEffect, useRef, useState } from "react";
import type { MouseEvent } from "react";
import { useObjects } from "../api/hooks";
import { dataPrompt, scopeKey } from "../lib/viewer";
import { promptHref } from "../prompt";

const NEW_TAB_KEY = "aaiclick.objects.newTab";

// A job's objects as a small popup of links. The "new tab" toggle picks where a
// plain click opens one; ctrl/middle-click and long-press always offer a new tab.
// Ids are 64-bit snowflakes carried as strings — never parse them.
export function ObjectsMenu({ jobId, onPrompt }: { jobId: string; onPrompt: (v: string) => void }) {
  const scope = scopeKey(jobId);
  const { data } = useObjects(scope);
  const ref = useRef<HTMLDetailsElement>(null);
  const [newTab, setNewTab] = useState(() => localStorage.getItem(NEW_TAB_KEY) === "1");
  const toggleNewTab = () => {
    localStorage.setItem(NEW_TAB_KEY, newTab ? "0" : "1");
    setNewTab(!newTab);
  };

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
    if (newTab || e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    e.preventDefault();
    if (ref.current) ref.current.open = false;
    onPrompt(cmd);
  };

  return (
    <details className="objects-menu" ref={ref} data-testid="job-objects">
      <summary className="btn btn-sm">Objects ({items.length}) ▾</summary>
      <div className="objects-popup">
        <button
          type="button"
          className={`chip${newTab ? " chip-active" : ""}`}
          aria-pressed={newTab}
          data-testid="objects-new-tab"
          onClick={toggleNewTab}
        >
          ↗ new tab
        </button>
        <ul role="menu">
          {items.map((o) => {
            const cmd = dataPrompt(scope, o.name);
            return (
              <li key={o.name} role="none">
                <a
                  role="menuitem"
                  className="mono"
                  href={promptHref(cmd)}
                  target={newTab ? "_blank" : undefined}
                  rel={newTab ? "noopener" : undefined}
                  onClick={(e) => go(e, cmd)}
                >
                  {o.name}
                </a>
              </li>
            );
          })}
        </ul>
      </div>
    </details>
  );
}
