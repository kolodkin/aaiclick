import { describe, expect, test } from "vitest";
import { parsePrompt, suggestCommands } from "./prompt";

describe("viewer routes", () => {
  test.each([
    ["@data", { kind: "data", object: null }],
    ["@data persistent", { kind: "data", object: null }],
    ["@data persistent orders", { kind: "data", object: "orders" }],
    ["@data orders", { kind: "data", object: "orders" }],
    ["@job etl", { kind: "job", name: "etl", view: "table", item: null }],
    ["@job etl graph", { kind: "job", name: "etl", view: "graph", item: null }],
    ["@job etl timings", { kind: "job", name: "etl", view: "timings", item: null }],
    ["@job etl duration", { kind: "job", name: "etl", view: "duration", item: null }],
    ["@job etl data", { kind: "job", name: "etl", view: "data", item: null }],
    ["@job 123 data sales", { kind: "job", name: "123", view: "data", item: "sales" }],
    ["@job etl data graph", { kind: "job", name: "etl", view: "data", item: "graph" }],
    ["@job etl data sales extra", { kind: "job", name: "etl", view: "data", item: "sales" }],
    ["@task 7", { kind: "task", id: "7", view: "log" }],
    ["@task 7 data", { kind: "task", id: "7", view: "data" }],
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
    expect(parsePrompt("unregister etl")).toEqual({ kind: "unregister-confirm", name: "etl" });
  });

  test("help shows the home command reference", () => {
    expect(parsePrompt("help")).toEqual({ kind: "home" });
  });

  test("existing routes still parse", () => {
    expect(parsePrompt("@jobs")).toEqual({ kind: "jobs" });
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
