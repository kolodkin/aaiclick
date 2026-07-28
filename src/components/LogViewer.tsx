import { memo, useState } from "react";
import type { LogLine } from "../api/types";
import { useTaskLogs } from "../api/hooks";

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

export function LogViewer({ taskId }: { taskId: string }) {
  const { data, isLoading, isError } = useTaskLogs(taskId);
  const [showTimestamps, setShowTimestamps] = useState(false);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  const lines = data?.lines ?? [];
  if (!data || !data.available || lines.length === 0) {
    return <div className="logs">(no logs captured for this task)</div>;
  }
  return (
    <div className="logs">
      <label className="logs-toolbar">
        <input
          type="checkbox"
          checked={showTimestamps}
          onChange={(e) => setShowTimestamps(e.target.checked)}
        />
        Show timestamps
      </label>
      <LogLines lines={lines} showTimestamps={showTimestamps} />
    </div>
  );
}
