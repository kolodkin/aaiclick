import { useTaskLogs } from "../api/hooks";

export function LogViewer({ taskId }: { taskId: number }) {
  const { data, isLoading, isError } = useTaskLogs(taskId);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  if (!data || !data.available || data.lines.length === 0) {
    return <div className="logs">(no logs captured for this task)</div>;
  }
  return (
    <div className="logs">
      {data.lines.map((line, i) => (
        <div key={i}>{line}</div>
      ))}
    </div>
  );
}
