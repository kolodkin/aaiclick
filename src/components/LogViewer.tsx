import { memo, useState } from "react";
import type { LogLine } from "../api/types";
import { useTaskLogs } from "../api/hooks";
import { LiveStatus } from "./LiveStatus";
import { isTaskStarted, isTerminalTask } from "../lib/status";
import type { TaskStatus } from "../api/types";

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
// same payload. Each line carries a per-level class (text color by severity;
// raw stdout/stderr arrive as INFO/WARNING) plus a per-stream class so stderr
// lines get their own marker independent of severity — a logging.error record
// shows ERROR red *and* the stderr bar.
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
        <div key={i} data-testid={`log-line-${line.level}`} className={`log-line lvl-${line.level} src-${line.stream}`}>
          {showTimestamps && line.created_at && <span className="ts">{fmtTs(line.created_at)} </span>}
          {line.text}
        </div>
      ))}
    </>
  );
});

export function LogViewer({ taskId, status }: { taskId: string; status: TaskStatus }) {
  // A task that has not started cannot produce output, so there is nothing to
  // poll for yet; the status change that starts it arrives over /events and
  // re-renders this with polling switched on.
  const started = isTaskStarted(status);
  const live = started && !isTerminalTask(status);
  const { data, isLoading, isError, dataUpdatedAt } = useTaskLogs(taskId, live);
  const [showTimestamps, setShowTimestamps] = useState(false);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  const lines = data?.lines ?? [];
  const empty = !data || !data.available || lines.length === 0;
  // Three different empty states, and conflating them misleads: a queued task
  // has produced nothing *yet*, a running one may simply not have flushed, and
  // only a finished one can be said to have captured nothing.
  if (!started) return <div className="logs sub">Task has not started — no output until it runs.</div>;
  // A running task with nothing captured yet is still being polled, so the
  // toolbar stays: "(no logs captured)" on its own reads as a final answer
  // when it is really "none so far".
  if (empty && !live) return <div className="logs">(no logs captured for this task)</div>;
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
        <div className="spacer" />
        {/* Logs are on their own clock, not the /events stream — say so here
            rather than letting the task's "live" badge above imply otherwise.
            Once the task is terminal nothing more arrives, so the badge goes
            away instead of ticking up an age that will never reset. */}
        {live && <LiveStatus updatedAt={dataUpdatedAt} mode="poll" />}
      </div>
      {empty ? (
        <div className="sub">waiting for output…</div>
      ) : (
        <LogLines lines={lines} showTimestamps={showTimestamps} />
      )}
    </div>
  );
}
