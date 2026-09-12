// Inline freshness for an auto-refreshing view: how its data is kept current,
// and how current it actually is.
//
// Both halves matter. The mode makes a dead stream visible instead of letting
// it degrade silently to polling; the timestamp separates a stream that is
// connected but delivering nothing from an idle one.
//
// Each instance reports one query — name it, and the cadence is looked up from
// the one place that decides it. Freshness must be that query's own
// `dataUpdatedAt`; a borrowed one would lie.
import { useEffect, useState, useSyncExternalStore } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { isLiveConnected, refreshMode, subscribeLive } from "../api/events";
import { relativeTime } from "../lib/format";

type BadgeState = "live" | "polling" | "manual";

const LABELS: Record<BadgeState, string> = { live: "live", polling: "polling", manual: "on change" };

// Re-render cadence for the age text — a display timer only, it issues no
// requests. Matched to the label's own resolution so it fires once per visible
// change rather than 60 times per minute once the age is counted in minutes.
function tickDelay(ageMs: number): number {
  if (ageMs < 60_000) return 1000;
  return ageMs < 3_600_000 ? 60_000 : 3_600_000;
}

export function LiveStatus({ updatedAt, queryKey }: { updatedAt: number; queryKey: string }) {
  const streamUp = useSyncExternalStore(subscribeLive, isLiveConnected);
  const [now, setNow] = useState(() => Date.now());
  const qc = useQueryClient();

  useEffect(() => {
    // Nothing fetched yet: the label is fixed, so there is nothing to tick.
    if (!updatedAt) return;
    const timer = setTimeout(() => setNow(Date.now()), tickDelay(Math.max(0, now - updatedAt)));
    return () => clearTimeout(timer);
  }, [updatedAt, now]);

  const mode = refreshMode(queryKey);
  // Only a stream-backed query goes dark when the stream does; the other modes
  // are unaffected by it and must not claim otherwise.
  const state: BadgeState = mode === "manual" ? "manual" : mode === "stream" && streamUp ? "live" : "polling";

  return (
    <span className="live-status" data-testid="live-status" data-mode={state}>
      <span className={state === "live" ? "live-dot on" : "live-dot"} aria-hidden="true" />
      {LABELS[state]} · updated {updatedAt ? relativeTime(updatedAt, now) : "never"}
      {/* Nothing refreshes a manual query on its own, so the only thing that
          can act on a stale age is the reader — put the control where the age
          is. Prefix match: compound keys (["object", scope, name]) refresh
          with their list. */}
      {state === "manual" && (
        <button
          type="button"
          className="live-refresh"
          data-testid="live-refresh"
          title="Refresh"
          aria-label="Refresh"
          onClick={() => void qc.invalidateQueries({ queryKey: [queryKey] })}
        >
          ↻
        </button>
      )}
    </span>
  );
}
