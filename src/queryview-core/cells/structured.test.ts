import { describe, expect, it } from "vitest";

import { MAX_DETECT_CHARS, detectFormat, detectStructured } from "./structured";

describe("detectStructured", () => {
  it("detects a JSON object", () => {
    const got = detectStructured('{"id": 1, "tags": ["a", "b"]}');
    expect(got).toEqual({ format: "json", data: { id: 1, tags: ["a", "b"] } });
  });

  it("detects a JSON array", () => {
    expect(detectStructured("[1, 2, 3]")).toEqual({ format: "json", data: [1, 2, 3] });
  });

  it("rejects JSON scalars, which are not worth a popup", () => {
    expect(detectStructured("123")).toBeNull();
    expect(detectStructured('"quoted"')).toBeNull();
    expect(detectStructured("true")).toBeNull();
    expect(detectStructured("null")).toBeNull();
  });

  it("detects multi-line YAML mappings and sequences", () => {
    expect(detectStructured("id: 1\nname: alpha")).toEqual({
      format: "yaml",
      data: { id: 1, name: "alpha" },
    });
    expect(detectStructured("- one\n- two")).toEqual({ format: "yaml", data: ["one", "two"] });
  });

  // Almost any single-line string is valid YAML, so single-line text is left
  // alone unless it is real JSON.
  it("ignores single-line text that only YAML would claim", () => {
    expect(detectStructured("status: healthy")).toBeNull();
    expect(detectStructured("plain text value")).toBeNull();
    expect(detectStructured("C:\\path\\to\\file")).toBeNull();
  });

  it("ignores prose that happens to span lines", () => {
    expect(detectStructured("a long sentence\nand another one")).toBeNull();
  });

  it("returns null for empty, whitespace and unparseable text", () => {
    expect(detectStructured("")).toBeNull();
    expect(detectStructured("   ")).toBeNull();
    expect(detectStructured('{"unterminated": ')).toBeNull();
  });
});

describe("detectFormat", () => {
  it("names the format without handing back the parsed graph", () => {
    expect(detectFormat('{"id": 1}')).toBe("json");
    expect(detectFormat("id: 1\nname: alpha")).toBe("yaml");
    expect(detectFormat("plain text value")).toBeNull();
  });

  // Parsing a huge value to pick a button glyph would block the grid's first
  // paint; the popup still parses the one cell that gets opened.
  it("gives up past the size cap rather than parsing a huge value", () => {
    const huge = `{"a": "${"x".repeat(MAX_DETECT_CHARS)}"}`;
    expect(detectFormat(huge)).toBeNull();
    expect(detectStructured(huge)).not.toBeNull();
  });
});
