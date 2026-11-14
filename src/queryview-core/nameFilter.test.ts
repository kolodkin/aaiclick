import { expect, test } from "vitest";
import { filterNames } from "./nameFilter";

const names = ["INFORMATION_SCHEMA", "sales_reporting", "sales_2026_09_08", "default", "system"];

test("empty or blank query keeps every name in order", () => {
  expect(filterNames(names, "")).toEqual(names);
  expect(filterNames(names, "   ")).toEqual(names);
});

test("matches a case-insensitive substring anywhere in the name", () => {
  expect(filterNames(names, "reporting")).toEqual(["sales_reporting"]);
  expect(filterNames(names, "SCHEMA")).toEqual(["INFORMATION_SCHEMA"]);
  expect(filterNames(names, "information")).toEqual(["INFORMATION_SCHEMA"]);
});

test("trims the query and keeps original order for several matches", () => {
  expect(filterNames(names, " a ")).toEqual(["INFORMATION_SCHEMA", "sales_reporting", "sales_2026_09_08", "default"]);
});

test("no match yields an empty list", () => {
  expect(filterNames(names, "zzz")).toEqual([]);
});

test("matches objects by the name the accessor picks", () => {
  const fields = [{ name: "asset_id" }, { name: "host_name" }];
  expect(filterNames(fields, "HOST", (f) => f.name)).toEqual([{ name: "host_name" }]);
});
