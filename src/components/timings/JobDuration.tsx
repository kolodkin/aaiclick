// See docs/designs/ui.md — "Job Detail (`@job <name>`)", Duration tab.
import { useJobTimings } from "../../api/hooks";
import type { JobStatus } from "../../api/types";
import { parseUtc } from "../../lib/format";
import { statusClass } from "../../lib/status";
import { durationRows, formatRun } from "../../lib/timings";
import { LiveStatus } from "../LiveStatus";
import { useNow } from "./useNow";

export function JobDuration({
  refId,
  jobStatus,
  onPrompt,
}: {
  refId: string;
  jobStatus: JobStatus;
  onPrompt: (v: string) => void;
}) {
  const { data, isLoading, isError, dataUpdatedAt } = useJobTimings(refId);
  const now = useNow(jobStatus === "RUNNING" || jobStatus === "PENDING");

  if (isLoading) return <p className="sub">loading durations…</p>;
  if (isError || !data) return <p className="err">Could not load job timings.</p>;
  const tasks = data.tasks ?? [];

  const rows = durationRows(tasks, now);
  const notStarted = tasks.filter((t) => !t.attempts?.length).length;
  if (rows.length === 0) return <p className="sub">No task has started yet.</p>;
  const longest = Math.max(1, rows[0].ms);
  const startedAt = new Map(
    tasks.flatMap((t) => (t.attempts ?? []).map((a) => [`${t.id}#${a.attempt}`, a.started_at])),
  );

  return (
    <>
      <p className="sub">
        <LiveStatus updatedAt={dataUpdatedAt} queryKey="job-timings" />
      </p>
      <div className="tl" data-testid="job-duration">
        <div className="tl-row tl-head">
          <span>Name</span>
          <span>Run time</span>
          <span className="tl-num">Time</span>
        </div>
        {rows.map((r) => (
          <div
            key={`${r.taskId}#${r.attempt}`}
            className="tl-row clickable"
            data-testid="duration-row"
            title={`${r.status} · started ${parseUtc(startedAt.get(`${r.taskId}#${r.attempt}`) ?? "").toLocaleTimeString()}`}
            onClick={() => onPrompt(`@task ${r.taskId}`)}
          >
            <span className="name-link mono">{r.label}</span>
            <div className="tl-track">
              <div
                className={`tl-bar ${statusClass(r.status)}`}
                style={{ left: 0, width: `${(r.ms / longest) * 100}%` }}
              />
            </div>
            <span className="tl-num mono">{formatRun(r.ms)}</span>
          </div>
        ))}
      </div>
      {notStarted > 0 && (
        <div className="tl-legend">
          <span>{notStarted} task(s) not started</span>
        </div>
      )}
    </>
  );
}
