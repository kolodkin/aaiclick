import { memo } from "react";
import { useTaskLogs } from "../api/hooks";

// `lines` typically grows by appending; memoising on the array identity skips
// the per-line VDOM rebuild when a poll returns the same payload, and lets
// React diff incrementally when only the tail changed.
const LogLines = memo(function LogLines({ lines }: { lines: readonly string[] }) {
  return (
    <>
      {lines.map((line, i) => (
        <div key={i}>{line}</div>
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
