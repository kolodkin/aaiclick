import { memo } from "react";
import type { LogLine } from "../api/types";
import { useTaskLogs } from "../api/hooks";

// `lines` typically grows by appending; memoising on the array identity skips
// the per-line VDOM rebuild when a poll returns the same payload, and lets
// React diff incrementally when only the tail changed. stderr lines carry a
// modifier class so the viewer can distinguish them from stdout.
const LogLines = memo(function LogLines({ lines }: { lines: readonly LogLine[] }) {
  return (
    <>
      {lines.map((line, i) => (
        <div key={i} className={line.stream === "stderr" ? "log-line log-stderr" : "log-line"}>
          {line.text}
        </div>
      ))}
    </>
  );
});

export function LogViewer({ taskId }: { taskId: string }) {
  const { data, isLoading, isError } = useTaskLogs(taskId);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  const lines = data?.lines ?? [];
  if (!data || !data.available || lines.length === 0) {
    return <div className="logs">(no logs captured for this task)</div>;
  }
  return (
    <div className="logs">
      <LogLines lines={lines} />
    </div>
  );
}
