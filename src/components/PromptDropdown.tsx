// See docs/designs/ui.md — "Prompt dropdown".
import { useInfiniteQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { relativeTime } from "../lib/format";
import type { Completion } from "./commands";
import { matchSource, type SourceItem } from "./promptSources";
import { StatusBadge } from "./StatusBadge";

const DEBOUNCE_MS = 200;
// Rows of a picker may be this old; reopening the list reuses them instead of refetching every page.
const STALE_MS = 30_000;

function useDebounced<T>(value: T, ms: number): T {
  const [v, setV] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setV(value), ms);
    return () => clearTimeout(t);
  }, [value, ms]);
  return v;
}

function commandItems(draft: string, completions: Completion[]): SourceItem[] {
  const hits = completions.filter((c) => c.text.startsWith(draft) && c.text !== draft);
  return hits.map((c) => ({
    id: c.text,
    label: c.text,
    detail: c.disabled ? "Distributed mode only" : undefined,
    insert: c.text,
  }));
}

// The rows for `draft`: the command's values once a source matches, else the matching commands.
export function usePromptItems(draft: string, open: boolean, sandbox: boolean, completions: Completion[]) {
  const debounced = useDebounced(draft, DEBOUNCE_MS);
  const source = useMemo(() => matchSource(debounced, { sandbox }), [debounced, sandbox]);
  const query = useInfiniteQuery({
    queryKey: ["prompt-source", source?.key, source?.text],
    queryFn: ({ pageParam }) => source!.fetchPage(pageParam),
    initialPageParam: 0,
    getNextPageParam: (last, pages) => (last.more ? pages.reduce((n, p) => n + p.items.length, 0) : undefined),
    enabled: open && source !== null,
    staleTime: STALE_MS,
    // A picker, not a live view: the fallback poll would refetch every loaded page.
    refetchInterval: false,
  });
  const items = useMemo(() => {
    if (!source) return commandItems(draft, completions);
    const rows = query.data?.pages.flatMap((p) => p.items) ?? [];
    const local = source.local;
    return local ? rows.filter((r) => r.label.includes(local)) : rows;
  }, [source, query.data, draft, completions]);
  return { source, items, query };
}

type PromptItems = ReturnType<typeof usePromptItems>;

interface Props {
  list: PromptItems;
  active: number;
  onActive: (i: number) => void;
  onPick: (item: SourceItem) => void;
}

export function PromptDropdown({ list: { source, items, query }, active, onActive, onPick }: Props) {
  const { hasNextPage, isFetchingNextPage, fetchNextPage } = query;

  const sentinel = useRef<HTMLLIElement>(null);
  useEffect(() => {
    const el = sentinel.current;
    if (!el || !hasNextPage) return;
    const io = new IntersectionObserver((entries) => {
      if (entries[0].isIntersecting && !isFetchingNextPage) void fetchNextPage();
    });
    io.observe(el);
    return () => io.disconnect();
  }, [hasNextPage, isFetchingNextPage, fetchNextPage]);

  const activeRef = useRef<HTMLLIElement>(null);
  useEffect(() => {
    activeRef.current?.scrollIntoView({ block: "nearest" });
  }, [active]);

  if (!source && items.length === 0) return null;
  return (
    // mousedown anywhere in the list (rows, scrollbar, status rows) must not blur the prompt and close it
    <ul className="prompt-dropdown" role="listbox" id="prompt-dropdown" onMouseDown={(e) => e.preventDefault()}>
      {items.map((it, i) => (
        <li
          key={`${it.id}-${i}`}
          ref={i === active ? activeRef : undefined}
          role="option"
          aria-selected={i === active}
          className={i === active ? "active" : undefined}
          // mousemove, not mouseenter: keyboard scrolling slides rows under a resting pointer
          onMouseMove={() => i !== active && onActive(i)}
          onClick={() => onPick(it)}
        >
          <span className="mono">{it.label}</span>
          {it.detail && <span className="sub-inline">{it.detail}</span>}
          {it.status && <StatusBadge status={it.status} />}
          {it.at && <span className="sub-inline">{relativeTime(it.at)}</span>}
        </li>
      ))}
      {query.isLoading && <li className="empty">loading…</li>}
      {query.isError && <li className="empty err">failed to load</li>}
      {source && query.isSuccess && items.length === 0 && <li className="empty">no matches</li>}
      {hasNextPage && (
        <li ref={sentinel} className="empty">
          {isFetchingNextPage ? "loading…" : ""}
        </li>
      )}
    </ul>
  );
}
