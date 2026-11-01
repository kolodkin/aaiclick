import { expect, test } from "vitest";
import type { TaskTimingView } from "../api/types";
import { durationRows, formatRun, parallelCounts, timelineRows } from "./timings";

const T0 = Date.parse("2026-11-01T10:00:00Z");
const at = (s: number) => new Date(T0 + s * 1000).toISOString();

function task(id: string, attempts: [number, number | null, string][], ready: number | null = 0): TaskTimingView {
  return {
    id,
    name: id,
    status: "COMPLETED",
    created_at: at(0),
    ready_at: ready == null ? null : at(ready),
    attempts: attempts.map(([s, e, status], i) => ({
      attempt: i + 1,
      status: status as TaskTimingView["status"],
      started_at: at(s),
      completed_at: e == null ? null : at(e),
    })),
  };
}

const FIXTURE = [
  task("never", [], null),
  task("retried", [
    [2, 3, "FAILED"],
    [5, 9, "COMPLETED"],
  ]),
  task("root", [[1, 4, "COMPLETED"]]),
];

test("timeline orders by first start, never-run last", () => {
  const { rows } = timelineRows(FIXTURE, T0 + 10_000);
  expect(rows.map((r) => r.task.id)).toEqual(["root", "retried", "never"]);
  expect(rows[2].attempts).toHaveLength(0);
});

test("retried row has one wait and a failed first attempt", () => {
  const row = timelineRows(FIXTURE, T0 + 10_000).rows[1];
  expect(row.waits).toEqual([{ start: T0 + 3000, end: T0 + 5000 }]);
  expect(row.attempts[0].status).toBe("FAILED");
  expect(row.queued).toEqual({ start: T0, end: T0 + 2000 });
  expect(row.total).toBe(9000);
});

test("running attempt and blocked span end at now", () => {
  const now = T0 + 7000;
  const { rows } = timelineRows([task("run", [[1, null, "RUNNING"]]), task("wait", [], null)], now);
  expect(rows[0].attempts[0].span.end).toBe(now);
  expect(rows[1].blocked).toEqual({ start: T0, end: now });
  expect(rows[1].queued).toBeNull();
});

test("duration ranks attempts, labels retries, clamps skew", () => {
  const rows = durationRows([...FIXTURE, task("skew", [[5, 4, "COMPLETED"]])], T0 + 10_000);
  expect(rows.map((r) => r.label)).toEqual(["retried #2", "root", "retried #1", "skew"]);
  expect(rows[3].ms).toBe(0);
});

test("parallel counts overlapping attempts", () => {
  const { rows, origin, end } = timelineRows(FIXTURE, T0 + 10_000);
  expect(Math.max(...parallelCounts(rows, origin, end, 90))).toBe(2);
});

test("empty job keeps a non-zero axis", () => {
  const { rows, origin, end } = timelineRows([], T0);
  expect(rows).toEqual([]);
  expect(end).toBeGreaterThan(origin);
});

test("formatRun keeps a decimal under a minute", () => {
  expect(formatRun(2100)).toBe("2.1s");
  expect(formatRun(90_000)).toBe("1m 30s");
});
