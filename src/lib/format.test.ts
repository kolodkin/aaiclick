import { expect, test } from "vitest";
import { attemptTitle, parseUtc, relativeTime } from "./format";

test("naive timestamps parse as UTC", () => {
  expect(parseUtc("2026-10-31T10:00:00").toISOString()).toBe("2026-10-31T10:00:00.000Z");
  expect(parseUtc("2026-10-31T12:00:00+02:00").toISOString()).toBe("2026-10-31T10:00:00.000Z");
  expect(relativeTime("2026-10-31T10:00:00", Date.parse("2026-10-31T10:01:00Z"))).toBe("1m ago");
});

test("attempt tooltip shows status, start and duration", () => {
  const a = {
    attempt: 2,
    status: "FAILED" as const,
    started_at: "2026-10-31T10:00:00",
    completed_at: "2026-10-31T10:01:05",
  };
  const title = attemptTitle(a);
  expect(title).toMatch(/^Attempt 2: FAILED · started .+ · 1m 05s$/);
  expect(title).toContain(parseUtc(a.started_at).toLocaleString());
});
