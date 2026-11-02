import { useEffect, useRef, useState } from "react";
import type { MouseEvent } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useObjects } from "../api/hooks";
import { dataPrompt, scopeKey } from "../lib/viewer";
import { promptHref } from "../prompt";

const NEW_TAB_KEY = "aaiclick.objects.newTab";
// Short lists read at a glance; only longer ones get a filter box.
const FILTER_MIN = 6;

// A job's objects as a small popup of links. The "new tab" toggle picks where a
// plain click opens one; ctrl/middle-click and long-press always offer a new tab.
// Ids are 64-bit snowflakes carried as strings — never parse them.
// `refreshKey` changes when the job may have new objects (a task finished); the
// list refetches then rather than on every stream signal for any job.
export function ObjectsMenu({
  jobId,
  refreshKey,
  onPrompt,
}: {
  jobId: string;
  refreshKey: string | number;
  onPrompt: (v: string) => void;
}) {
  const scope = scopeKey(jobId);
  const { data } = useObjects(scope);
  const qc = useQueryClient();
  const seenKey = useRef(refreshKey);
  useEffect(() => {
    if (seenKey.current === refreshKey) return;
    seenKey.current = refreshKey;
    void qc.invalidateQueries({ queryKey: ["objects", scope] });
  }, [qc, scope, refreshKey]);
  const ref = useRef<HTMLDetailsElement>(null);
  const [newTab, setNewTab] = useState(() => localStorage.getItem(NEW_TAB_KEY) === "1");
  const [filter, setFilter] = useState("");
  const [open, setOpen] = useState(false);
  const toggleNewTab = () => {
    localStorage.setItem(NEW_TAB_KEY, newTab ? "0" : "1");
    setNewTab(!newTab);
  };

  const close = () => {
    if (ref.current) ref.current.open = false;
  };

  // While open, close on a click outside the popup or on Escape.
  useEffect(() => {
    if (!open) return;
    const onPointer = (e: PointerEvent) => {
      if (!ref.current?.contains(e.target as Node)) close();
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") close();
    };
    document.addEventListener("pointerdown", onPointer);
    document.addEventListener("keydown", onKey);
    return () => {
      document.removeEventListener("pointerdown", onPointer);
      document.removeEventListener("keydown", onKey);
    };
  }, [open]);

  const items = data?.items ?? [];
  if (items.length === 0) return null;
  const total = data?.total ?? items.length;
  const filterable = items.length >= FILTER_MIN;
  const needle = filterable ? filter.toLowerCase() : "";
  const allCmd = dataPrompt(scope);
  const shown = items.filter((o) => o.name.toLowerCase().includes(needle));

  // Middle-click never reaches onClick; the browser opens the href natively.
  const go = (e: MouseEvent, cmd: string) => {
    if (newTab || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
    e.preventDefault();
    close();
    onPrompt(cmd);
  };

  return (
    <details
      className="objects-menu"
      ref={ref}
      data-testid="job-objects"
      onToggle={(e) => {
        setOpen(e.currentTarget.open);
        setFilter("");
      }}
    >
      <summary className="btn btn-sm">Objects ({total}) ▾</summary>
      <div className="objects-popup">
        <div className="objects-popup-head">
          {filterable && (
            <input
              className="scope-filter mono"
              placeholder="filter objects…"
              aria-label="Filter objects"
              data-testid="objects-filter"
              value={filter}
              onChange={(e) => setFilter(e.target.value)}
            />
          )}
          <button
            type="button"
            className={`chip${newTab ? " chip-active" : ""}`}
            aria-pressed={newTab}
            data-testid="objects-new-tab"
            onClick={toggleNewTab}
          >
            ↗ new tab
          </button>
        </div>
        <ul>
          {shown.map((o) => {
            const cmd = dataPrompt(scope, o.name);
            return (
              <li key={o.name}>
                <a
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
        {shown.length === 0 && <p className="sub objects-empty">no match</p>}
        {total > items.length && (
          <a className="objects-more" href={promptHref(allCmd)} onClick={(e) => go(e, allCmd)}>
            all {total} in @data →
          </a>
        )}
      </div>
    </details>
  );
}
