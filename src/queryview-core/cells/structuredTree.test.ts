import { describe, expect, it } from "vitest";

import { ROOT_PATH, defaultCollapsed, treeRows } from "./structuredTree";

const data = {
  id: 1,
  tags: ["a", "b"],
  owner: { team: "core", active: true, note: null },
};

// Rows as `depth key summary`, the shape the modal renders.
const shape = (rows: ReturnType<typeof treeRows>) => rows.map((r) => `${r.depth} ${r.key ?? "$"} ${r.summary}`);

describe("treeRows", () => {
  it("walks an object depth-first, containers summarised by size", () => {
    expect(shape(treeRows(data, new Set()))).toEqual([
      "0 $ {3}",
      "1 id 1",
      "1 tags [2]",
      "2 0 a",
      "2 1 b",
      "1 owner {3}",
      "2 team core",
      "2 active true",
      "2 note null",
    ]);
  });

  it("omits the children of a collapsed container", () => {
    const rows = treeRows(data, new Set());
    const tags = rows.find((r) => r.key === "tags")!;
    expect(shape(treeRows(data, new Set([tags.path])))).toEqual([
      "0 $ {3}",
      "1 id 1",
      "1 tags [2]",
      "1 owner {3}",
      "2 team core",
      "2 active true",
      "2 note null",
    ]);
  });

  it("collapsing the root leaves just the root row", () => {
    expect(shape(treeRows(data, new Set([ROOT_PATH])))).toEqual(["0 $ {3}"]);
  });

  it("marks containers expandable and scalars not", () => {
    const rows = treeRows(data, new Set());
    expect(rows.filter((r) => r.expandable).map((r) => r.key ?? "$")).toEqual(["$", "tags", "owner"]);
  });

  it("gives every row a distinct path, even for colliding key names", () => {
    const rows = treeRows({ "a.b": 1, a: { b: 2 } }, new Set());
    expect(new Set(rows.map((r) => r.path)).size).toBe(rows.length);
  });

  it("handles a scalar or empty root", () => {
    expect(shape(treeRows("hello", new Set()))).toEqual(["0 $ hello"]);
    expect(shape(treeRows({}, new Set()))).toEqual(["0 $ {0}"]);
    expect(shape(treeRows([], new Set()))).toEqual(["0 $ [0]"]);
  });
});

describe("defaultCollapsed", () => {
  // Top two levels open, deeper containers folded, so a big document opens
  // readable instead of thousands of lines long. Every deep container is in
  // the set, not just the outermost one, so drilling into `b` reveals `c`
  // still folded rather than dumping its whole subtree.
  it("collapses every container below depth 1", () => {
    const nested = { a: { b: { c: [1] } }, d: [1, 2] };
    const rows = treeRows(nested, new Set());
    const collapsed = defaultCollapsed(nested);
    const keyOf = (p: string) => rows.find((r) => r.path === p)!.key;
    expect([...collapsed].map(keyOf).sort()).toEqual(["b", "c"]);
  });

  it("leaves a shallow document fully expanded", () => {
    expect(defaultCollapsed({ a: 1, b: [1, 2] }).size).toBe(0);
  });
});
