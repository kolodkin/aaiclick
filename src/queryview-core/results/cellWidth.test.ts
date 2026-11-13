import { describe, expect, it } from "vitest";

import { CELL_WIDTH_CH, isOverflowing } from "./cellWidth";

describe("isOverflowing", () => {
  it("is false for a value that fits on one line", () => {
    expect(isOverflowing("")).toBe(false);
    expect(isOverflowing("x".repeat(CELL_WIDTH_CH))).toBe(false);
  });

  it("is true past the cell width", () => {
    expect(isOverflowing("x".repeat(CELL_WIDTH_CH + 1))).toBe(true);
  });

  // `whitespace-pre` renders these as extra rows however narrow they are, and
  // short multi-line YAML is exactly what the parsed view is for.
  it("is true for multi-line text, even when it is short", () => {
    expect(isOverflowing("id: 1\nname: alpha")).toBe(true);
    expect(isOverflowing("a\nb")).toBe(true);
  });
});
