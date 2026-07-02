import { useTask } from "../api/hooks";
import { Chips } from "../components/Chips";
import { LogViewer } from "../components/LogViewer";
import { MetaGrid } from "../components/MetaGrid";
import { StatusBadge } from "../components/StatusBadge";
import { durationBetween, relativeTime } from "../lib/format";

export function TaskDetail({ id, onPrompt }: { id: string; onPrompt: (v: string) => void }) {
  const { data: task, isLoading, isError } = useTask(id);

  if (isLoading) return <p className="sub">loading…</p>;
  if (isError || !task)
    return (
      <>
        <h2>Task not found</h2>
        <p className="sub mono">#{id}</p>
        <Chips chips={[{ label: "@jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      </>
    );

  return (
    <>
      <Chips
        chips={[
          { label: "@jobs", cmd: "@jobs" },
          { label: `← @job ${task.job_id}`, cmd: `@job ${task.job_id}` },
        ]}
        onPrompt={onPrompt}
      />
      <div className="detail-head">
        <h2>
          <span className="mono">{task.name}</span> <StatusBadge status={task.status} />
        </h2>
        <MetaGrid
          items={[
            { k: "Task ID", v: `#${task.id}`, mono: true },
            { k: "Job", v: String(task.job_id), mono: true },
            { k: "Entrypoint", v: task.entrypoint, mono: true },
            { k: "Attempt", v: `${task.attempt}/${task.max_retries}`, mono: true },
            { k: "Execution worker", v: task.execution_worker_id == null ? "—" : String(task.execution_worker_id), mono: true },
            { k: "Started", v: relativeTime(task.started_at) },
            { k: "Duration", v: durationBetween(task.started_at, task.completed_at) },
          ]}
        />
        {task.error && <div className="err">{task.error}</div>}
      </div>
      <LogViewer taskId={task.id} />
    </>
  );
}
