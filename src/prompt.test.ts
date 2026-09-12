import { describe, expect, test } from "vitest";
import { parsePrompt } from "./prompt";

describe("viewer routes", () => {
  test.each([
    ["@data", { kind: "data", job: null, object: null }],
    ["@data job nightly_etl", { kind: "data", job: "nightly_etl", object: null }],
    ["@data job 123 orders", { kind: "data", job: "123", object: "orders" }],
    ["@data orders", { kind: "data", job: null, object: "orders" }],
    ["@query", { kind: "query", job: null, object: null }],
    ["@query orders", { kind: "query", job: null, object: "orders" }],
    ["@query job etl orders", { kind: "query", job: "etl", object: "orders" }],
    ["@dashboard", { kind: "dashboard", name: null }],
    ["@dashboard sales", { kind: "dashboard", name: "sales" }],
  ])("%s", (prompt, route) => {
    expect(parsePrompt(prompt)).toEqual(route);
  });

  test("existing routes still parse", () => {
    expect(parsePrompt("@jobs")).toEqual({ kind: "jobs" });
    expect(parsePrompt("@job x graph")).toEqual({ kind: "job", name: "x", view: "graph" });
  });
});
