import { expect, test } from "vitest";
import { logTone } from "./logTone";

test.each([
  ["ValueError: boom", "error"],
  ["Traceback (most recent call last):", "error"],
  ["CRITICAL: disk gone", "error"],
  ["an Exception was raised", "error"],
  ["WARNING: low memory", "warning"],
  ["DeprecationWarning: old api", "warning"],
  ["debug: cache hit", "debug"],
  ["warning: then an error", "error"],
  ["step 3 of 20", "plain"],
])("%s -> %s", (text, tone) => {
  expect(logTone(text)).toBe(tone);
});
