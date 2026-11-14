import { describe, expect, test } from "vitest";
import {
  dataPrompt,
  fieldsFromSchema,
  formatBytes,
  orderColsToPairs,
  scopeKey,
  jobScopeLabel,
  scopeLabel,
} from "./viewer";

describe("scope keys", () => {
  test("persistent and job", () => {
    expect(scopeKey(null)).toBe("persistent");
    expect(scopeKey("nightly_etl")).toBe("job:nightly_etl");
    expect(scopeLabel("persistent")).toBe("Persistent");
    expect(scopeLabel("job:123")).toBe("Job 123");
    expect(scopeLabel("all")).toBe("All scopes");
    expect(jobScopeLabel("job:7523769567175368706", "etl")).toBe("etl #368706");
    expect(jobScopeLabel("job:123", undefined)).toBe("Job 123");
  });

  test.each([
    ["all", undefined, "@data"],
    ["persistent", undefined, "@data persistent"],
    ["persistent", "orders", "@data persistent orders"],
    ["job:7", "sales", "@data job 7 sales"],
  ])("dataPrompt(%s, %s) round-trips as %s", (scope, object, prompt) => {
    expect(dataPrompt(scope, object)).toBe(prompt);
  });
});

describe("order by adapters", () => {
  test("cols to wire pairs", () => {
    const cols = [
      { name: "a", dir: "ASC" as const },
      { name: "b", dir: "DESC" as const },
    ];
    expect(orderColsToPairs(cols)).toEqual([
      ["a", "ASC"],
      ["b", "DESC"],
    ]);
  });
});

describe("fields", () => {
  test("fieldsFromSchema drops aai_id and keeps order", () => {
    const fields = fieldsFromSchema({
      aai_id: { type: "UInt64" },
      id: { type: "Int64" },
      name: { type: "String" },
    });
    expect(fields).toEqual([
      { name: "id", type: "Int64" },
      { name: "name", type: "String" },
    ]);
  });
});

describe("formatBytes", () => {
  test("units", () => {
    expect(formatBytes(null)).toBe("—");
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(2048)).toBe("2.0 KB");
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.0 MB");
  });
});
