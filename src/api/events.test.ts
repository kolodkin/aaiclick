import { expect, test } from "vitest";
import { refreshMode } from "./events";

// req: designs/frontend/badge-refresh-modes
test.each([
  ["jobs", "stream"],
  ["job", "stream"],
  ["job-graph", "stream"],
  ["job-timings", "stream"],
  ["task", "stream"],
  ["task-logs", "poll"],
  ["registered-jobs", "manual"],
  ["objects", "manual"],
  ["object", "manual"],
])("a %s badge reports %s", (key, mode) => {
  expect(refreshMode(key)).toBe(mode);
});
