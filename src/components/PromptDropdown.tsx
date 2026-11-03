// See docs/designs/ui.md — "Prompt dropdown".
import { useInfiniteQuery } from "@tanstack/react-query";
import { useEffect, useMemo, useRef, useState } from "react";
import { relativeTime } from "../lib/format";
import type { Completion } from "./commands";
import { matchSource, type SourceItem } from "./promptSources";
import { StatusBadge } from "./StatusBadge";

const DEBOUNCE_MS = 200;

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

interface Props {
  draft: string;
  sandbox: boolean;
  completions: Completion[];
  active: number;
  onActive: (i: number) => void;
  onPick: (item: SourceItem) => void;
  /** Reports the rows shown, so the prompt's ↑/↓/Enter can act on them. */
  onItems: (items: SourceItem[]) => void;
}

export function PromptDropdown({ draft, sandbox, completions, active, onActive, onPick, onItems }: Props) {
  const debounced = useDebounced(draft, DEBOUNCE_MS);
  const source = useMemo(() => matchSource(debounced, { sandbox }), [debounced, sandbox]);
  const query = useInfiniteQuery({
    queryKey: ["prompt-source", source?.key, source?.text],
    queryFn: ({ pageParam }) => source!.fetchPage(pageParam),
    initialPageParam: 0,
    getNextPageParam: (last, pages) => (last.more ? pages.reduce((n, p) => n + p.items.length, 0) : undefined),
    enabled: source !== null,
    // A picker, not a live view: the fallback poll would refetch every loaded page.
    refetchInterval: false,
  });
  const { hasNextPage, isFetching, isFetchingNextPage, fetchNextPage } = query;

  const items = useMemo(
    () => (source ? (query.data?.pages.flatMap((p) => p.items) ?? []) : commandItems(draft, completions)),
    [source, query.data, draft, completions],
  );
  useEffect(() => {
    onItems(items);
  }, [items, onItems]);

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

  // Arrowing onto the last loaded row asks for the next page; the sentinel may sit below the fold.
  useEffect(() => {
    // Waits out any refetch (live-update invalidation) and re-runs once it settles.
    if (active === items.length - 1 && hasNextPage && !isFetching) void fetchNextPage();
  }, [active, items.length, hasNextPage, isFetching, fetchNextPage]);

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
          onMouseEnter={() => onActive(i)}
          onClick={() => onPick(it)}
        >
          <span className="mono">{it.label}</span>
          {it.detail && <span className="sub-inline">{it.detail}</span>}
          {it.status && <StatusBadge status={it.status} />}
          {it.at && <span className="sub-inline">{relativeTime(it.at)}</span>}
        </li>
      ))}
      {source && query.isLoading && <li className="empty">loading…</li>}
      {source && query.isError && <li className="empty err">failed to load</li>}
      {source && query.isSuccess && items.length === 0 && <li className="empty">no matches</li>}
      {hasNextPage && (
        <li ref={sentinel} className="empty">
          {isFetchingNextPage ? "loading…" : ""}
        </li>
      )}
    </ul>
  );
}
