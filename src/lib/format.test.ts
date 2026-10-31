import { expect, test } from "vitest";
import { parseUtc, relativeTime } from "./format";

test("naive timestamps parse as UTC", () => {
  expect(parseUtc("2026-10-31T10:00:00").toISOString()).toBe("2026-10-31T10:00:00.000Z");
  expect(parseUtc("2026-10-31T12:00:00+02:00").toISOString()).toBe("2026-10-31T10:00:00.000Z");
  expect(relativeTime("2026-10-31T10:00:00", Date.parse("2026-10-31T10:01:00Z"))).toBe("1m ago");
});
