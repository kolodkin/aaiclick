import { useJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { JobGraph } from "../components/graph/JobGraph";
import { MetaGrid } from "../components/MetaGrid";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { TasksTable } from "../components/TasksTable";
import { durationMs, relativeTime } from "../lib/format";
import type { JobViewMode } from "../prompt";

export function JobDetail({
  name,
  view,
  onPrompt,
}: {
  name: string;
  view: JobViewMode;
  onPrompt: (v: string) => void;
}) {
  const { data: job, isLoading, isError } = useJob(name);

  if (isLoading) return <p className="sub">loading…</p>;
  if (isError || !job)
    return (
      <>
        <h2>Job not found</h2>
        <p className="sub mono">{name}</p>
        <Chips chips={[{ label: "@jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      </>
    );

  const cancellable = job.status === "RUNNING" || job.status === "PENDING";
  const onCancel = () => onPrompt(`cancel ${job.name}`);

  return (
    <>
      <Chips chips={[{ label: "← @jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      <div className="detail-head">
        <div className="title-row">
          <h2>
            <span className="mono">{job.name}</span> <StatusBadge status={job.status} />
          </h2>
          <div className="spacer" />
          {cancellable && (
            <button className="btn btn-danger btn-sm" onClick={onCancel}>
              Cancel job
            </button>
          )}
        </div>
        <MetaGrid
          items={[
            { k: "Created", v: relativeTime(job.created_at) },
            { k: "Started", v: relativeTime(job.started_at) },
            { k: "Completed", v: relativeTime(job.completed_at) },
            { k: "Duration", v: durationMs(job.duration_ms) },
            { k: "Progress", v: <ProgressBar done={job.completed_tasks} total={job.total_tasks} /> },
          ]}
        />
        {job.error && <div className="err">{job.error}</div>}
      </div>
      <div className="chips">
        <span
          className={`chip${view === "table" ? " chip-active" : ""}`}
          onClick={() => onPrompt(`@job ${job.name}`)}
        >
          Table
        </span>
        <span
          className={`chip${view === "graph" ? " chip-active" : ""}`}
          onClick={() => onPrompt(`@job ${job.name} graph`)}
        >
          Graph
        </span>
      </div>
      {view === "graph" ? (
        <JobGraph refId={job.name} onPrompt={onPrompt} />
      ) : (
        <TasksTable tasks={job.tasks ?? []} onPrompt={onPrompt} />
      )}
    </>
  );
}
