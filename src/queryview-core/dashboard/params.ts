// Dashboard-level selectors, declared in a dashboard's `params` and substituted
// into its queries via `{name}` placeholders:
//
//   value       a literal: {region} -> 'eu'
//   identifier  a table/column: {field} -> "city" (a literal would make
//               `SELECT {field}` select a constant string, not the column)
//   dimension   an optional GROUP BY column: checked -> its own column, else ''
//
// An options_sql may itself reference another param ({table}), so field lists
// can depend on the selected table; resolveOrder sequences those. See
// docs/dashboard.md, "Dashboard parameters".

export type ParamKind = "value" | "identifier" | "dimension";

// A selector with its option list resolved and a value chosen (or left empty) —
// what the page receives as `window.params`.
export type ResolvedParam = {
  name: string;
  kind: ParamKind;
  options: string[];
  value: string;
};

// Runs a map of named SQL and hands back column-oriented results.
export type QueryRunner = (
  queries: Record<string, string>,
) => Promise<{ ok: true; results: Record<string, Record<string, unknown[]>> } | { ok: false; message: string }>;

export type DashboardParam = {
  name: string;
  kind: ParamKind;
  options?: string[];
  optionsSql?: string;
  // Whether the first option is chosen for you. `default: none` leaves the
  // selector empty, holding back queries that need it until a choice is made.
  autoSelect: boolean;
};

const KINDS: ParamKind[] = ["value", "identifier", "dimension"];
const CASTS = ["literal", "identifier"];
// `{name}` substitutes per the param's kind; `{name:literal}` / `{name:identifier}`
// override it for that one spot: a table is an identifier in `FROM {table}` but
// a string in `WHERE table = {table:literal}`.
const PLACEHOLDER = /\{([A-Za-z_][A-Za-z0-9_]*)(?::([A-Za-z_]+))?\}/g;

// Parse the `params` list. Defensive, like parseQueryParams: a malformed entry
// is dropped so a broken declaration costs one selector, not the dashboard.
export function parseDashboardParams(raw: unknown): DashboardParam[] {
  if (!Array.isArray(raw)) return [];
  const out: DashboardParam[] = [];
  for (const entry of raw) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) continue;
    const o = entry as Record<string, unknown>;
    if (typeof o.name !== "string" || o.name === "") continue;
    const kind = (o.kind ?? "value") as ParamKind;
    if (!KINDS.includes(kind)) continue;

    const autoSelect = o.default !== "none";
    const hasOptions = Array.isArray(o.options);
    const sql = typeof o.options_sql === "string" ? o.options_sql.trim() : "";
    if (hasOptions && sql) continue; // ambiguous
    if (sql) {
      out.push({ name: o.name, kind, optionsSql: sql, autoSelect });
    } else if (hasOptions) {
      const options = (o.options as unknown[]).filter((v) => typeof v !== "object").map(String);
      if (options.length) out.push({ name: o.name, kind, options, autoSelect });
    } else if (kind === "dimension") {
      out.push({ name: o.name, kind, autoSelect }); // a checkbox: no option list
    }
  }
  return out;
}

// The `{name}` placeholders a query references, in order of first appearance.
export function placeholdersIn(sql: string): string[] {
  const seen = new Set<string>();
  for (const m of sql.matchAll(PLACEHOLDER)) seen.add(m[1]);
  return [...seen];
}

// How the connection's dialect quotes: its identifier quote and whether
// backslash escapes (see /api/session's ident_quote and backslash_escapes).
// A quoted SQL string literal: single quotes doubled, and backslashes too where
// the dialect reads them as escapes (the connection's backslashEscapes).
function quoteLiteral(value: string, backslashEscapes: boolean): string {
  const v = backslashEscapes ? value.replaceAll("\\", "\\\\") : value;
  return `'${v.replaceAll("'", "''")}'`;
}

export type Dialect = { quote: string; backslashEscapes: boolean };

// A quote character (or a backslash that would escape it) can't be trusted
// through string assembly, and no real name carries one, so reject it.
function quoteIdentifier(name: string, param: string, { quote, backslashEscapes }: Dialect): string {
  if (name.includes(quote) || (backslashEscapes && name.includes("\\")))
    throw new Error(`invalid identifier for {${param}}: ${name}`);
  return `${quote}${name}${quote}`;
}

// A dimension's checkbox state; a page may send 0 for unchecked.
const isChecked = (v?: string): v is string => !!v && v !== "0";

// Replace every declared `{name}`; placeholders with no matching param are left
// alone (they may be literal braces in the SQL).
export function substituteParams(
  sql: string,
  params: DashboardParam[],
  values: Record<string, string>,
  dialect: Dialect,
): string {
  const byName = new Map(params.map((p) => [p.name, p]));
  return sql.replace(PLACEHOLDER, (token, name: string, cast?: string) => {
    const p = byName.get(name);
    if (!p) return token;
    if (cast && !CASTS.includes(cast)) throw new Error(`unknown cast for {${name}}: ${cast}`);
    const raw = values[name];
    if (p.kind === "dimension") {
      // Unchecked: an empty literal keeps the query's shape; the column just
      // contributes nothing to the grouping.
      return isChecked(raw) ? quoteIdentifier(name, name, dialect) : "''";
    }
    if (raw === undefined || raw === "") throw new Error(`no value selected for {${name}}`);
    const asIdentifier = cast ? cast === "identifier" : p.kind === "identifier";
    return asIdentifier ? quoteIdentifier(raw, name, dialect) : quoteLiteral(raw, dialect.backslashEscapes);
  });
}

// Declaration order, except a param whose options_sql references another is
// resolved after it. Throws on a cycle rather than spinning.
export function resolveOrder(params: DashboardParam[]): DashboardParam[] {
  const byName = new Map(params.map((p) => [p.name, p]));
  const ordered: DashboardParam[] = [];
  const done = new Set<string>();
  const visiting = new Set<string>();
  function visit(p: DashboardParam) {
    if (done.has(p.name)) return;
    if (visiting.has(p.name)) throw new Error(`params dependency cycle at ${p.name}`);
    visiting.add(p.name);
    for (const dep of placeholdersIn(p.optionsSql ?? "")) {
      const target = byName.get(dep);
      if (target) visit(target);
    }
    visiting.delete(p.name);
    done.add(p.name);
    ordered.push(p);
  }
  for (const p of params) visit(p);
  return ordered;
}

// The declared params a query still needs a value for. Dimensions never count:
// an unchecked one is off, not unset.
export function missingParams(sql: string, params: DashboardParam[], values: Record<string, string>): string[] {
  const byName = new Map(params.map((p) => [p.name, p]));
  return placeholdersIn(sql).filter((name) => {
    const p = byName.get(name);
    return !!p && p.kind !== "dimension" && !values[name];
  });
}

// Resolve every selector's options in dependency order, keeping the caller's
// values where they are still offered. A param whose options_sql depends on one
// not chosen yet resolves to no options and no value — with `default: none`
// that is the normal opening state, not a failure.
export async function resolveParams(
  params: DashboardParam[],
  values: Record<string, string>,
  run: QueryRunner,
  dialect: Dialect,
): Promise<ResolvedParam[]> {
  const resolved: ResolvedParam[] = [];
  const chosen: Record<string, string> = {};
  for (const p of resolveOrder(params)) {
    let options = p.options ?? [];
    if (p.optionsSql) {
      if (missingParams(p.optionsSql, params, chosen).length) {
        chosen[p.name] = "";
        resolved.push({ name: p.name, kind: p.kind, options: [], value: "" });
        continue;
      }
      const r = await run({ o: substituteParams(p.optionsSql, params, chosen, dialect) });
      if (!r.ok) throw new Error(r.message);
      options = (Object.values(r.results.o ?? {})[0] ?? []).map(String);
    }
    const want = values[p.name];
    const value =
      p.kind === "dimension"
        ? isChecked(want)
          ? want
          : ""
        : want && options.includes(want)
          ? want
          : p.autoSelect
            ? (options[0] ?? "")
            : "";
    chosen[p.name] = value;
    resolved.push({ name: p.name, kind: p.kind, options, value });
  }
  return resolved;
}

// The dashboard's queries with the selections substituted. A query whose
// selectors aren't all chosen yet is left out rather than run half-blank.
export function applyToQueries(
  queries: Record<string, string>,
  declared: DashboardParam[],
  resolved: ResolvedParam[],
  dialect: Dialect,
): Record<string, string> {
  const values = Object.fromEntries(resolved.map((p) => [p.name, p.value]));
  return Object.fromEntries(
    Object.entries(queries)
      .filter(([, sql]) => missingParams(sql, declared, values).length === 0)
      .map(([name, sql]) => [name, substituteParams(sql, declared, values, dialect)]),
  );
}

// A set-params message from the dashboard page: values only, never SQL — the
// SQL lives in the dashboard's queries and is substituted host-side.
export function parseParamsRequest(data: unknown): Record<string, string> | null {
  if (typeof data !== "object" || data === null) return null;
  const msg = data as Record<string, unknown>;
  if (msg.type !== "set-params") return null;
  const values = msg.values;
  if (typeof values !== "object" || values === null || Array.isArray(values)) return null;
  const out: Record<string, string> = {};
  for (const [k, v] of Object.entries(values as Record<string, unknown>)) {
    // Checkboxes arrive as booleans; '' (also from null) reads as unset / unchecked.
    out[k] = typeof v === "boolean" ? (v ? "on" : "") : String(v ?? "");
  }
  return out;
}
