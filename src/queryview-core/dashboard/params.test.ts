import { describe, expect, test } from "vitest";
import {
  parseDashboardParams,
  placeholdersIn,
  resolveOrder,
  resolveParams,
  missingParams,
  substituteParams,
  applyToQueries,
  parseParamsRequest,
  type DashboardParam,
  type Dialect,
} from "./params";

const CH: Dialect = { quote: "`", backslashEscapes: true };
const PG: Dialect = { quote: '"', backslashEscapes: false };
const sub = (sql: string, params: DashboardParam[], values: Record<string, string>) =>
  substituteParams(sql, params, values, CH);

describe("parseDashboardParams", () => {
  test("parses a static options param", () => {
    expect(parseDashboardParams([{ name: "region", options: ["eu", "us"] }])).toEqual([
      { name: "region", kind: "value", options: ["eu", "us"], autoSelect: true },
    ]);
  });

  test("parses an options_sql param", () => {
    expect(
      parseDashboardParams([{ name: "table", kind: "identifier", options_sql: "SELECT name FROM system.tables" }]),
    ).toEqual([
      {
        name: "table",
        kind: "identifier",
        optionsSql: "SELECT name FROM system.tables",
        autoSelect: true,
      },
    ]);
  });

  test("parses a dimension param, which needs no options", () => {
    expect(parseDashboardParams([{ name: "category", kind: "dimension" }])).toEqual([
      { name: "category", kind: "dimension", autoSelect: true },
    ]);
  });

  test("drops an entry declaring both options and options_sql", () => {
    expect(parseDashboardParams([{ name: "x", options: ["a"], options_sql: "SELECT 1" }])).toEqual([]);
  });

  test("drops an entry with an unknown kind", () => {
    expect(parseDashboardParams([{ name: "x", kind: "sql", options: ["a"] }])).toEqual([]);
  });

  test("drops a nameless entry", () => {
    expect(parseDashboardParams([{ options: ["a"] }])).toEqual([]);
  });

  test("returns nothing for a non-array", () => {
    expect(parseDashboardParams({ name: "x" })).toEqual([]);
  });
});

describe("substituteParams", () => {
  const params = parseDashboardParams([
    { name: "table", kind: "identifier", options_sql: "SELECT 1" },
    { name: "field", kind: "identifier", options_sql: "SELECT 1" },
    { name: "region", options: ["eu"] },
    { name: "category", kind: "dimension" },
  ]);

  test("substitutes an identifier as a quoted identifier, not a string literal", () => {
    expect(sub("SELECT {field} FROM {table}", params, { field: "city", table: "events" })).toBe(
      "SELECT `city` FROM `events`",
    );
  });

  test("escapes backslashes only where the dialect does", () => {
    const user = parseDashboardParams([{ name: "user", options: ["CORP\\alice"] }]);
    expect(sub("WHERE u = {user}", user, { user: "CORP\\alice" })).toBe("WHERE u = 'CORP\\\\alice'");
    expect(substituteParams("WHERE u = {user}", user, { user: "CORP\\alice" }, PG)).toBe("WHERE u = 'CORP\\alice'");
  });

  test("rejects a backslash in an identifier only where the dialect escapes", () => {
    expect(() => sub("SELECT {field}", params, { field: "a\\" })).toThrow(/invalid identifier/);
    expect(substituteParams("SELECT {field}", params, { field: "a\\" }, PG)).toBe('SELECT "a\\"');
  });

  test("substitutes a value as a quoted literal", () => {
    expect(sub("WHERE region = {region}", params, { region: "eu" })).toBe("WHERE region = 'eu'");
  });

  test("escapes quotes in a value literal", () => {
    expect(sub("WHERE region = {region}", params, { region: "o'brien" })).toBe("WHERE region = 'o''brien'");
  });

  test("rejects an identifier containing a backtick", () => {
    expect(() => sub("SELECT {field}", params, { field: "a`b" })).toThrow(/identifier/i);
  });

  test("substitutes a checked dimension as its column", () => {
    expect(sub("GROUP BY {category}", params, { category: "on" })).toBe("GROUP BY `category`");
  });

  test("substitutes an unchecked dimension as '' so the query shape is unchanged", () => {
    expect(sub("GROUP BY {category}", params, { category: "" })).toBe("GROUP BY ''");
  });

  test("casts an identifier to a literal where SQL wants a string", () => {
    // system.columns.table is compared against a string, but the same param is
    // an identifier in `FROM {table}`.
    expect(sub("WHERE table = {table:literal} FROM {table}", params, { table: "events" })).toBe(
      "WHERE table = 'events' FROM `events`",
    );
  });

  test("casts a value to an identifier", () => {
    expect(sub("SELECT {region:identifier}", params, { region: "eu" })).toBe("SELECT `eu`");
  });

  test("rejects an unknown cast", () => {
    expect(() => sub("SELECT {field:sql}", params, { field: "city" })).toThrow(/cast/i);
  });

  test("leaves an unknown placeholder untouched", () => {
    expect(sub("SELECT {nope}", params, {})).toBe("SELECT {nope}");
  });

  test("throws when a declared param has no value", () => {
    expect(() => sub("SELECT {field}", params, {})).toThrow(/field/);
  });
});

describe("placeholdersIn", () => {
  test("finds the placeholders a query depends on", () => {
    expect(placeholdersIn("SELECT name FROM system.columns WHERE table = {table} AND x = {y}")).toEqual(["table", "y"]);
  });

  test("finds a cast placeholder by its param name", () => {
    expect(placeholdersIn("WHERE table = {table:literal}")).toEqual(["table"]);
  });

  test("finds none in a plain query", () => {
    expect(placeholdersIn("SELECT 1")).toEqual([]);
  });
});

describe("resolveOrder", () => {
  test("orders a dependent param after the one its options_sql references", () => {
    const params = parseDashboardParams([
      {
        name: "field",
        kind: "identifier",
        options_sql: "SELECT name FROM system.columns WHERE table = {table}",
      },
      { name: "table", kind: "identifier", options_sql: "SELECT name FROM system.tables" },
    ]);
    expect(resolveOrder(params).map((p) => p.name)).toEqual(["table", "field"]);
  });

  test("keeps declaration order when nothing depends on anything", () => {
    const params = parseDashboardParams([
      { name: "a", options: ["1"] },
      { name: "b", options: ["2"] },
    ]);
    expect(resolveOrder(params).map((p) => p.name)).toEqual(["a", "b"]);
  });

  test("throws on a dependency cycle rather than looping", () => {
    const params = parseDashboardParams([
      { name: "a", kind: "identifier", options_sql: "SELECT {b}" },
      { name: "b", kind: "identifier", options_sql: "SELECT {a}" },
    ]);
    expect(() => resolveOrder(params)).toThrow(/cycle/i);
  });
});

describe("default: none", () => {
  test("parses a param that starts unselected", () => {
    expect(parseDashboardParams([{ name: "table", kind: "identifier", options: ["a"], default: "none" }])).toEqual([
      { name: "table", kind: "identifier", options: ["a"], autoSelect: false },
    ]);
  });

  test("auto-selects by default, as query params do", () => {
    expect(parseDashboardParams([{ name: "table", options: ["a"] }])[0].autoSelect).toBe(true);
  });
});

describe("missingParams", () => {
  const params = parseDashboardParams([
    { name: "table", kind: "identifier", options: ["events"], default: "none" },
    { name: "field", kind: "identifier", options: ["city"], default: "none" },
    { name: "category", kind: "dimension" },
  ]);

  test("names the placeholders a query still needs a value for", () => {
    expect(missingParams("SELECT {field} FROM {table}", params, { table: "events" })).toEqual(["field"]);
  });

  test("is empty once every placeholder has a value", () => {
    expect(missingParams("SELECT {field} FROM {table}", params, { table: "events", field: "city" })).toEqual([]);
  });

  test("never reports a dimension, which is off rather than unset", () => {
    expect(missingParams("GROUP BY {category}", params, {})).toEqual([]);
  });

  test("ignores placeholders that match no param", () => {
    expect(missingParams("SELECT {nope}", params, {})).toEqual([]);
  });
});

describe("resolveParams", () => {
  const params = parseDashboardParams([
    {
      name: "table",
      kind: "identifier",
      default: "none",
      options_sql: "SELECT name FROM system.tables",
    },
    {
      name: "field",
      kind: "identifier",
      default: "none",
      options_sql: "SELECT name FROM system.columns WHERE table = {table:literal}",
    },
  ]);
  const run = async (queries: Record<string, string>) => ({
    ok: true as const,
    results: { o: { name: queries.o.includes("system.tables") ? ["events"] : ["city", "ts"] } },
  });

  test("leaves a dependent param unresolved while its dependency is unset", async () => {
    // The whole point of `default: none`: nothing is chosen, so the field list
    // cannot be looked up yet — and that is not an error.
    const resolved = await resolveParams(params, {}, run, CH);
    expect(resolved).toEqual([
      { name: "table", kind: "identifier", options: ["events"], value: "" },
      { name: "field", kind: "identifier", options: [], value: "" },
    ]);
  });

  test("resolves the dependent options once the dependency has a value", async () => {
    const resolved = await resolveParams(params, { table: "events" }, run, CH);
    expect(resolved[1]).toEqual({
      name: "field",
      kind: "identifier",
      options: ["city", "ts"],
      value: "",
    });
  });

  test("keeps a chosen value that is still a valid option", async () => {
    const resolved = await resolveParams(params, { table: "events", field: "city" }, run, CH);
    expect(resolved[1].value).toBe("city");
  });

  test("drops a chosen value that the new options no longer offer", async () => {
    const resolved = await resolveParams(params, { table: "events", field: "gone" }, run, CH);
    expect(resolved[1].value).toBe("");
  });

  test("auto-selects the first option unless the param opts out", async () => {
    const auto = parseDashboardParams([
      { name: "table", kind: "identifier", options_sql: "SELECT name FROM system.tables" },
    ]);
    expect((await resolveParams(auto, {}, run, CH))[0].value).toBe("events");
  });
});

describe("applyToQueries", () => {
  test("substitutes ready queries and holds back ones waiting on a selector", () => {
    const params: DashboardParam[] = [{ name: "table", kind: "identifier", autoSelect: false, options: ["t"] }];
    const resolved = [{ name: "table", kind: "identifier" as const, options: ["t"], value: "" }];
    const out = applyToQueries({ a: "SELECT 1", b: "SELECT * FROM {table}" }, params, resolved, PG);
    expect(out).toEqual({ a: "SELECT 1" });
    const chosen = [{ ...resolved[0], value: "t" }];
    expect(applyToQueries({ b: "SELECT * FROM {table}" }, params, chosen, PG)).toEqual({
      b: 'SELECT * FROM "t"',
    });
  });
});

describe("parseParamsRequest", () => {
  test("takes values only, booleans as checkbox state", () => {
    expect(parseParamsRequest({ type: "set-params", values: { a: "x", d: true, e: false } })).toEqual({
      a: "x",
      d: "on",
      e: "",
    });
    expect(parseParamsRequest({ type: "set-params", values: { n: null, u: undefined, z: 0 } })).toEqual({
      n: "",
      u: "",
      z: "0",
    });
    expect(parseParamsRequest({ type: "run-queries", queries: {} })).toBeNull();
    expect(parseParamsRequest({ type: "set-params", values: ["x"] })).toBeNull();
  });
});

describe("an unchecked dimension", () => {
  test("stays unchecked for '' and 0", async () => {
    const params = parseDashboardParams([{ name: "by_city", kind: "dimension" }]);
    const run = async () => ({ ok: true as const, results: {} });
    for (const v of ["", "0"]) {
      const resolved = await resolveParams(params, { by_city: v }, run, PG);
      expect(resolved[0].value).toBe("");
    }
    expect(substituteParams("GROUP BY {by_city}", params, { by_city: "0" }, PG)).toBe("GROUP BY ''");
  });
});
