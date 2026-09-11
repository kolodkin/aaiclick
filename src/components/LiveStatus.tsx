// Inline freshness for any auto-refreshing view: how this view's data is
// kept current, and how current it actually is.
//
// Both halves matter. The mode makes a dead stream visible instead of letting
// it degrade silently to the polling fallback, and the timestamp shows
// whether anything is arriving at all — a stream that is connected but not
// delivering looks identical to an idle one without it.
//
// Each instance reports one query, so pass that query's own `dataUpdatedAt`
// and the mode that query actually runs in. A badge that borrows a
// neighbouring query's timestamp is a badge that can lie.
import { useEffect, useState, useSyncExternalStore } from "react";
import { isLiveConnected, subscribeLive } from "../api/events";

// Re-render cadence for the "Ns ago" text. A display timer only — it issues
// no requests, so it does not reintroduce the polling this replaces.
const TICK_MS = 1000;

// How the query behind this badge is refreshed:
//   stream — invalidated by /events, falling back to polling while it is down
//   poll   — always on its own timer, regardless of the stream (task logs)
//   manual — only refetched by its own mutations (registered jobs)
export type RefreshMode = "stream" | "poll" | "manual";

function sinceLabel(updatedAt: number, now: number): string {
  if (!updatedAt) return "never";
  const seconds = Math.max(0, Math.round((now - updatedAt) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  return minutes < 60 ? `${minutes}m ago` : `${Math.floor(minutes / 60)}h ago`;
}

export function LiveStatus({ updatedAt, mode = "stream" }: { updatedAt: number; mode?: RefreshMode }) {
  const streamUp = useSyncExternalStore(subscribeLive, isLiveConnected);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), TICK_MS);
    return () => clearInterval(timer);
  }, []);

  // Only a stream-backed query goes dark when the stream does; the other two
  // modes are unaffected by it and must not claim otherwise.
  const live = mode === "stream" && streamUp;
  const label = mode === "manual" ? "on change" : live ? "live" : "polling";

  return (
    <span className="live-status" data-testid="live-status" data-mode={live ? "live" : mode}>
      <span className={live ? "live-dot on" : "live-dot"} aria-hidden="true" />
      {label} · updated {sinceLabel(updatedAt, now)}
    </span>
  );
}
