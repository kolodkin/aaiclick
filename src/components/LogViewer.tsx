import { memo, useState } from "react";
import type { LogLine, TaskAttempt, TaskStatus } from "../api/types";
import { useTaskLogs } from "../api/hooks";
import { LiveStatus } from "./LiveStatus";
import { logTone } from "../lib/logTone";
import { isTaskStarted, isTerminalTask, statusClass } from "../lib/status";

// Render a captured created_at (ISO string) as HH:MM:SS.mmm for the inline
// timestamp prefix. Kept tiny and dependency-free; the value is informational.
function fmtTs(iso: string): string {
  // Stored timestamps are naive UTC (no offset); parse as UTC, not browser-local.
  const utc = /[zZ]|[+-]\d\d:?\d\d$/.test(iso) ? iso : `${iso}Z`;
  const d = new Date(utc);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(11, 23);
}

// `lines` typically grows by appending; memoising on the array identity (plus
// the timestamp flag) skips the per-line VDOM rebuild when a poll returns the
// same payload. Each line is colored by the keywords in its text (logTone)
// and stderr lines also get their own marker bar.
const LogLines = memo(function LogLines({
  lines,
  showTimestamps,
}: {
  lines: readonly LogLine[];
  showTimestamps: boolean;
}) {
  return (
    <>
      {lines.map((line, i) => (
        <div key={i} className={`log-line tone-${logTone(line.text)} src-${line.stream}`}>
          {showTimestamps && line.created_at && <span className="ts">{fmtTs(line.created_at)} </span>}
          {line.text}
        </div>
      ))}
    </>
  );
});

// One button per run, Airflow's "Task Tries": the number plus a square in the
// run's status color. Rendered only for a retried task — a single run has
// nothing to choose between.
function AttemptPicker({
  attempts,
  current,
  onPick,
}: {
  attempts: readonly TaskAttempt[];
  current: number | null | undefined;
  onPick: (attempt: number) => void;
}) {
  return (
    <div className="log-tries" role="group" aria-label="Task tries">
      <span>Task tries</span>
      {attempts.map((a) => (
        <button
          key={a.attempt}
          type="button"
          className={`log-try${a.attempt === current ? " on" : ""}`}
          aria-pressed={a.attempt === current}
          title={`Attempt ${a.attempt}: ${a.status}`}
          data-testid={`log-try-${a.attempt}`}
          onClick={() => onPick(a.attempt)}
        >
          {a.attempt}
          <span className={`try-sq ${statusClass(a.status)}`} />
        </button>
      ))}
    </div>
  );
}

export function LogViewer({ taskId, status, runs }: { taskId: string; status: TaskStatus; runs: number }) {
  const started = isTaskStarted(status);
  // null follows the latest attempt (and keeps polling it); a number pins an
  // earlier one, whose logs no longer change.
  const [picked, setPicked] = useState<number | null>(null);
  // The tries list always comes from the latest run's (polled) response: a
  // pinned attempt is cached for good, so its own list would miss new runs.
  // Unpinned, both calls share one query key and so one request.
  const latestRun = useTaskLogs(taskId, status, null, runs);
  const { data, isLoading, isError, dataUpdatedAt } = useTaskLogs(taskId, status, picked, runs);
  const [showTimestamps, setShowTimestamps] = useState(false);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  const attempts = latestRun.data?.attempts ?? data?.attempts ?? [];
  const latest = attempts.length;
  const live = picked == null && started && !isTerminalTask(status);
  if (latest === 0 && !started) return <div className="logs sub">Task has not started — no output until it runs.</div>;

  const lines = data?.lines ?? [];
  const empty = !data || !data.available || lines.length === 0;
  // Only a finished attempt can be said to have captured nothing; a running one
  // may simply not have flushed — which keeps "waiting for output…" instead.
  return (
    <div className="logs">
      <div className="logs-toolbar">
        <label>
          <input
            type="checkbox"
            checked={showTimestamps}
            onChange={(e) => setShowTimestamps(e.target.checked)}
          />
          Show timestamps
        </label>
        {latest > 1 && (
          <AttemptPicker
            attempts={attempts}
            current={data?.attempt}
            onPick={(n) => setPicked(n === latest ? null : n)}
          />
        )}
        <div className="spacer" />
        {/* Logs are on their own clock, not the /events stream — say so here
            rather than letting the task's "live" badge above imply otherwise.
            Once the task is terminal nothing more arrives, so the badge goes
            away instead of ticking up an age that will never reset. */}
        {live && <LiveStatus updatedAt={dataUpdatedAt} queryKey="task-logs" />}
      </div>
      {!empty ? (
        <LogLines lines={lines} showTimestamps={showTimestamps} />
      ) : live ? (
        <div className="sub">waiting for output…</div>
      ) : (
        <div className="sub">(no logs captured for this attempt)</div>
      )}
    </div>
  );
}
