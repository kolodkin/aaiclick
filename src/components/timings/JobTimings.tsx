// See docs/designs/ui.md — "Job Detail (`@job <name>`)", Timings tab.
import { useJobTimings } from "../../api/hooks";
import type { JobStatus } from "../../api/types";
import { statusClass } from "../../lib/status";
import { type Span, formatRun, parallelCounts, timelineRows } from "../../lib/timings";
import { LiveStatus } from "../LiveStatus";
import { useNow } from "./useNow";

const BUCKETS = 120;

export function JobTimings({
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

  if (isLoading) return <p className="sub">loading timings…</p>;
  if (isError || !data) return <p className="err">Could not load job timings.</p>;
  const tasks = data.tasks ?? [];
  if (tasks.length === 0) return <p className="sub">No tasks yet.</p>;

  const { origin, end, rows } = timelineRows(tasks, now);
  const scale = end - origin;
  const pos = (s: Span) => ({
    left: `${((s.start - origin) / scale) * 100}%`,
    width: `${((s.end - s.start) / scale) * 100}%`,
  });
  const tickStep = niceStep(scale / 8);
  const ticks = Array.from({ length: Math.floor(scale / tickStep) + 1 }, (_, i) => i * tickStep);
  const counts = parallelCounts(rows, origin, end, BUCKETS);
  const peak = Math.max(1, ...counts);

  return (
    <>
      <p className="sub">
        <LiveStatus updatedAt={dataUpdatedAt} queryKey="job-timings" />
      </p>
      <div className="tl" data-testid="job-timings">
        <div className="tl-row tl-head">
          <span>Name</span>
          <div className="tl-track">
            {ticks.map((t) => (
              <span key={t} className="tl-tick" style={{ left: `${(t / scale) * 100}%` }}>
                {formatRun(t).replace(".0s", "s")}
              </span>
            ))}
          </div>
          <span className="tl-num">Total</span>
        </div>
        {rows.map((r) => (
          <div
            key={r.task.id}
            className="tl-row clickable"
            data-testid="timings-row"
            title={hover(r)}
            onClick={() => onPrompt(`@task ${r.task.id}`)}
          >
            <span className="name-link mono">{r.task.name}</span>
            {r.attempts.length === 0 ? (
              <span className="tl-note">not started</span>
            ) : (
              <div className="tl-track">
                {r.blocked && <div className="tl-blocked" style={pos(r.blocked)} />}
                {r.queued && <div className="tl-queued" style={pos(r.queued)} />}
                {r.waits.map((w, i) => (
                  <div key={i} className="tl-queued" style={pos(w)} />
                ))}
                {r.attempts.map((a, i) => (
                  <div
                    key={a.attempt}
                    className={`tl-bar ${statusClass(a.status)}${i < r.attempts.length - 1 ? " tl-old" : ""}`}
                    style={pos(a.span)}
                  />
                ))}
              </div>
            )}
            <span className="tl-num mono">{r.total == null ? "" : formatRun(r.total)}</span>
          </div>
        ))}
        <div className="tl-row tl-par">
          <span className="tl-note">parallel (max {peak})</span>
          <div className="tl-strip">
            {counts.map((c, i) => (
              <i key={i} style={{ height: `${(c / peak) * 100}%` }} />
            ))}
          </div>
          <span />
        </div>
      </div>
      <div className="tl-legend">
        <span>
          <b className="tl-blocked" /> blocked on upstream
        </span>
        <span>
          <b className="tl-queued" /> queued / waiting
        </span>
        <span>faded bar = earlier attempt</span>
      </div>
    </>
  );
}

function hover(r: ReturnType<typeof timelineRows>["rows"][number]): string {
  const lines = [r.task.name];
  const head = [
    r.blocked && `blocked ${formatRun(r.blocked.end - r.blocked.start)}`,
    r.queued && `queued ${formatRun(r.queued.end - r.queued.start)}`,
  ];
  if (head.some(Boolean)) lines.push(head.filter(Boolean).join(" · "));
  r.attempts.forEach((a, i) => {
    const wait = r.waits[i];
    const waited = wait && i < r.attempts.length - 1 ? ` · waited ${formatRun(wait.end - wait.start)}` : "";
    lines.push(`#${a.attempt} ${a.status} ${formatRun(a.span.end - a.span.start)}${waited}`);
  });
  return lines.join("\n");
}

// Round an axis interval up to 1/2/5 × 10^n ms so tick labels stay readable.
function niceStep(raw: number): number {
  const pow = 10 ** Math.floor(Math.log10(Math.max(raw, 1)));
  return [1, 2, 5, 10].map((m) => m * pow).find((s) => s >= raw) ?? raw;
}
