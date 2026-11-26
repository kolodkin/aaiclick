import { describe, expect, test } from "vitest";
import { parsePrompt, suggestCommands } from "./prompt";

describe("viewer routes", () => {
  test.each([
    ["@data", { kind: "data", object: null }],
    ["@data persistent", { kind: "data", object: null }],
    ["@data persistent orders", { kind: "data", object: "orders" }],
    ["@data orders", { kind: "data", object: "orders" }],
    ["@job 123", { kind: "job", id: "123", view: "table", item: null }],
    ["@job 123 graph", { kind: "job", id: "123", view: "graph", item: null }],
    ["@job 123 timings", { kind: "job", id: "123", view: "timings", item: null }],
    ["@job 123 duration", { kind: "job", id: "123", view: "duration", item: null }],
    ["@job 123 data", { kind: "job", id: "123", view: "data", item: null }],
    ["@job 123 data sales", { kind: "job", id: "123", view: "data", item: "sales" }],
    ["@job 123 data graph", { kind: "job", id: "123", view: "data", item: "graph" }],
    ["@job 123 data sales extra", { kind: "job", id: "123", view: "data", item: "sales" }],
    // Jobs are opened by id only: a name is picked from the dropdown, which inserts the id.
    ["@job etl", { kind: "unknown", raw: "@job etl" }],
    ["@job etl data", { kind: "unknown", raw: "@job etl data" }],
    ["cancel 123", { kind: "cancel-confirm", ref: "123" }],
    ["cancel etl", { kind: "unknown", raw: "cancel etl" }],
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
