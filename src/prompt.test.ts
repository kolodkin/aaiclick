import { describe, expect, test } from "vitest";
import { parsePrompt, suggestCommands } from "./prompt";

describe("viewer routes", () => {
  test.each([
    ["@data", { kind: "data", scope: "all", object: null }],
    ["@data persistent", { kind: "data", scope: "persistent", object: null }],
    ["@data persistent orders", { kind: "data", scope: "persistent", object: "orders" }],
    ["@data job nightly_etl", { kind: "data", scope: "job:nightly_etl", object: null }],
    ["@data job 123 orders", { kind: "data", scope: "job:123", object: "orders" }],
    ["@data orders", { kind: "data", scope: "persistent", object: "orders" }],
    ["@query", { kind: "query", scope: "all", object: null }],
    ["@query orders", { kind: "query", scope: "persistent", object: "orders" }],
    ["@query job etl orders", { kind: "query", scope: "job:etl", object: "orders" }],
    ["@dashboard", { kind: "dashboard", name: null }],
    ["@dashboard sales", { kind: "dashboard", name: "sales" }],
  ])("%s", (prompt, route) => {
    expect(parsePrompt(prompt)).toEqual(route);
  });

  test("sandbox routes", () => {
    expect(parsePrompt("@sandbox")).toEqual({ kind: "sandbox" });
    expect(parsePrompt("@sandbox 42")).toEqual({ kind: "sandbox-file", id: "42" });
  });

  test("enable / disable confirm a registered-job toggle", () => {
    expect(parsePrompt("enable etl")).toEqual({ kind: "toggle-confirm", name: "etl", enabled: true });
    expect(parsePrompt("disable etl")).toEqual({ kind: "toggle-confirm", name: "etl", enabled: false });
  });

  test("help shows the home command reference", () => {
    expect(parsePrompt("help")).toEqual({ kind: "home" });
  });

  test("existing routes still parse", () => {
    expect(parsePrompt("@jobs")).toEqual({ kind: "jobs" });
    expect(parsePrompt("@job x graph")).toEqual({ kind: "job", name: "x", view: "graph" });
    expect(parsePrompt("@job x timings")).toEqual({ kind: "job", name: "x", view: "timings" });
    expect(parsePrompt("@job x duration")).toEqual({ kind: "job", name: "x", view: "duration" });
  });
});

describe("suggestCommands", () => {
  const commands = ["help", "@jobs", "@job", "run"];

  test.each([
    ["@", ["@jobs", "@job"]],
    ["@jobs", ["@jobs"]],
    ["bogus", commands],
  ])("%s", (raw, expected) => {
    expect(suggestCommands(raw, commands)).toEqual(expected);
  });
});
