// See docs/designs/ui.md — "Job Detail (`@job <name>`)".
import { useJob, useObjects } from "../api/hooks";
import { AdminButton } from "../components/AdminButton";
import { Chips } from "../components/Chips";
import { LiveStatus } from "../components/LiveStatus";
import { JobGraph } from "../components/graph/JobGraph";
import { MetaGrid } from "../components/MetaGrid";
import { ProgressBar } from "../components/ProgressBar";
import { StatusBadge } from "../components/StatusBadge";
import { TasksTable } from "../components/TasksTable";
import { JobDuration } from "../components/timings/JobDuration";
import { JobTimings } from "../components/timings/JobTimings";
import { durationMs, relativeTime } from "../lib/format";
import { dataPrompt, scopeKey } from "../lib/viewer";
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
  const { data: job, isLoading, isError, dataUpdatedAt } = useJob(name);

  if (isLoading) return <p className="sub">loading…</p>;
  if (isError || !job)
    return (
      <>
        <h2>Job not found</h2>
        <p className="sub mono">{name}</p>
        <Chips chips={[{ label: "@jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      </>
    );

  // Named once: the badge above and the body below must not drift apart.
  const tableView = view === "table";
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
          <JobObjects jobId={String(job.id)} onPrompt={onPrompt} />
          {cancellable && (
            <AdminButton className="btn btn-danger btn-sm" onClick={onCancel}>
              Cancel job
            </AdminButton>
          )}
        </div>
        {/* Other views show their own badge, for their own query. */}
        {tableView && (
          <p className="sub">
            <LiveStatus updatedAt={dataUpdatedAt} queryKey="job" />
          </p>
        )}
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
      <Chips
        chips={[
          { label: "Table", cmd: `@job ${job.name}`, active: view === "table" },
          { label: "Graph", cmd: `@job ${job.name} graph`, active: view === "graph" },
          { label: "Timings", cmd: `@job ${job.name} timings`, active: view === "timings" },
          { label: "Duration", cmd: `@job ${job.name} duration`, active: view === "duration" },
        ]}
        onPrompt={onPrompt}
      />
      {view === "graph" && <JobGraph refId={job.name} onPrompt={onPrompt} />}
      {view === "timings" && <JobTimings refId={job.name} jobStatus={job.status} onPrompt={onPrompt} />}
      {view === "duration" && <JobDuration refId={job.name} jobStatus={job.status} onPrompt={onPrompt} />}
      {tableView && <TasksTable tasks={job.tasks} onPrompt={onPrompt} />}
    </>
  );
}

// Ids are 64-bit snowflakes carried as strings — never parse them.
function JobObjects({ jobId, onPrompt }: { jobId: string; onPrompt: (v: string) => void }) {
  const scope = scopeKey(jobId);
  const { data } = useObjects(scope);
  const items = data?.items ?? [];
  if (items.length === 0) return null;
  return (
    <div className="field">
      <select
        data-testid="job-objects"
        aria-label="Objects"
        value=""
        onChange={(e) => onPrompt(dataPrompt(scope, e.target.value))}
      >
        <option value="">Objects ({items.length})…</option>
        {items.map((o) => (
          <option key={o.name} value={o.name}>
            {o.name}
          </option>
        ))}
      </select>
    </div>
  );
}
