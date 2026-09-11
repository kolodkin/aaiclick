// Inline liveness for auto-refreshing views: how the page is being kept
// current, and how current it actually is.
//
// Both halves matter. The mode makes a dead stream visible instead of letting
// it degrade silently to the 2 s polling fallback, and the timestamp shows
// whether anything is arriving at all — a stream that is connected but not
// delivering looks identical to an idle one without it.
import { useEffect, useState, useSyncExternalStore } from "react";
import { isLiveConnected, subscribeLive } from "../api/events";

// Re-render cadence for the "Ns ago" text. A display timer only — it issues
// no requests, so it does not reintroduce the polling this replaces.
const TICK_MS = 1000;

function sinceLabel(updatedAt: number, now: number): string {
  if (!updatedAt) return "never";
  const seconds = Math.max(0, Math.round((now - updatedAt) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  return minutes < 60 ? `${minutes}m ago` : `${Math.floor(minutes / 60)}h ago`;
}

export function LiveStatus({ updatedAt }: { updatedAt: number }) {
  const live = useSyncExternalStore(subscribeLive, isLiveConnected);
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), TICK_MS);
    return () => clearInterval(timer);
  }, []);

  return (
    <span className="live-status" data-testid="live-status">
      <span className={live ? "live-dot on" : "live-dot"} aria-hidden="true" />
      {live ? "live" : "polling"} · updated {sinceLabel(updatedAt, now)}
    </span>
  );
}
