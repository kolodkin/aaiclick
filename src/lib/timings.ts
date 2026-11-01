// Pure layout for the Timings and Duration tabs. See docs/designs/ui.md — "Job Detail".
import type { TaskStatus, TaskTimingView } from "../api/types";
import { durationMs, parseUtc } from "./format";

export type Span = { start: number; end: number };

export type TimelineRow = {
  task: TaskTimingView;
  blocked: Span | null;
  queued: Span | null;
  attempts: { span: Span; status: TaskStatus; attempt: number }[];
  waits: Span[];
  total: number | null;
};

const ms = (iso: string) => parseUtc(iso).getTime();
const endOr = (iso: string | null | undefined, now: number) => (iso ? ms(iso) : now);
// Clock skew can put an end before its start; never draw a negative width.
const span = (start: number, end: number): Span => ({ start, end: Math.max(start, end) });

function row(t: TaskTimingView, now: number): TimelineRow {
  const created = ms(t.created_at);
  const ready = t.ready_at ? ms(t.ready_at) : null;
  const attempts = (t.attempts ?? []).map((a) => ({
    span: span(ms(a.started_at), endOr(a.completed_at, now)),
    status: a.status,
    attempt: a.attempt,
  }));
  const first = attempts[0]?.span.start;
  const last = attempts.at(-1);
  const waits = attempts.slice(1).map((a, i) => span(attempts[i].span.end, a.span.start));
  // A failed last attempt with the task back in PENDING is an open wait until the next run.
  if (last && last.status !== "RUNNING" && t.status === "PENDING") waits.push(span(last.span.end, now));
  return {
    task: t,
    blocked: ready === created ? null : span(created, ready ?? first ?? now),
    queued: ready != null && first != null ? span(ready, first) : null,
    attempts,
    waits,
    total: ready != null && last ? Math.max(0, last.span.end - ready) : null,
  };
}

export function timelineRows(
  tasks: TaskTimingView[],
  now: number,
): { origin: number; end: number; rows: TimelineRow[] } {
  const rows = tasks.map((t) => row(t, now));
  rows.sort((a, b) => (a.attempts[0]?.span.start ?? Infinity) - (b.attempts[0]?.span.start ?? Infinity));
  const starts = rows.map((r) => ms(r.task.created_at));
  const origin = starts.length ? Math.min(...starts) : now;
  const ends = rows.flatMap((r) => [r.blocked, r.queued, ...r.waits, ...r.attempts.map((a) => a.span)]);
  const end = Math.max(origin + 1000, ...ends.map((s) => s?.end ?? origin));
  return { origin, end, rows };
}

export function parallelCounts(rows: TimelineRow[], origin: number, end: number, buckets: number): number[] {
  const width = (end - origin) / buckets;
  return Array.from({ length: buckets }, (_, i) => {
    const at = origin + (i + 0.5) * width;
    return rows.reduce((n, r) => n + r.attempts.filter((a) => a.span.start <= at && at < a.span.end).length, 0);
  });
}

export type DurationRow = { taskId: string; label: string; status: TaskStatus; ms: number };

export function durationRows(tasks: TaskTimingView[], now: number): DurationRow[] {
  return tasks
    .flatMap((t) => {
      const attempts = t.attempts ?? [];
      return attempts.map((a) => ({
        taskId: t.id,
        label: attempts.length > 1 ? `${t.name} #${a.attempt}` : t.name,
        status: a.status,
        ms: Math.max(0, endOr(a.completed_at, now) - ms(a.started_at)),
      }));
    })
    .sort((a, b) => b.ms - a.ms);
}

// Sub-minute runs keep a decimal so short tasks still rank visibly apart.
export function formatRun(msValue: number): string {
  return msValue < 60_000 ? `${(msValue / 1000).toFixed(1)}s` : durationMs(msValue);
}
