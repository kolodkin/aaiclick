# Viewer Frontend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the `@data`, `@query`, and `@dashboard` modes to the aaiclick SPA on top of the landed viewer backend, rendering rows with QueryView's frontend kernel.

**Architecture:** `src/queryview-core/` is a verbatim copy of QueryView's `frontend/src/core` (React 19, Tailwind 4, `js-yaml`) imported through the `@qv/core` alias and never edited here. aaiclick owns everything around it: React Query hooks over `/api/v0/viewer/*` and `/api/v0/objects`, three views under `src/views/`, a scope tree and objects table under `src/components/`, and three new prompt routes. The wire format is aaiclick's (`OrderBy` is a `[name, dir]` pair, `ColumnSchema` is `{name, type}`); one small adapter module converts to the kernel's `OrderCol` objects and `QueryRows`.

**Tech Stack:** React 19, TypeScript 5, Vite 6, Tailwind 4, TanStack Query 5, `js-yaml` 4, vitest (new dev dependency), Playwright (Python, existing `test_e2e/web/` suite).

**Spec:** `docs/designs/viewer.md` (sections "QueryView Coupling", "Frontend", "Testing").

## Global Constraints

- `src/queryview-core/` is never edited; its `README.md` records the QueryView commit it was copied from (`b69128dcf04319c86ae8b472f86078c978c71aaf` at the time of writing — use `git -C /home/user/queryview rev-parse HEAD`).
- The kernel's Tailwind utility classes and its `glass-*` component classes must be available: the utilities come from Tailwind scanning `src/`, the component classes are ported into `src/styles/queryview-core.css`.
- Prompt grammar from the spec: `@data`, `@data job <ref>`, `@data [job <ref>] <object>`, `@query [job <ref>] [<object>]`, `@dashboard [name]`.
- Scope keys on the wire: `"persistent"` or `"job:<id|name>"` (`aaiclick/viewer/scope.py`).
- `npm run check` (`tsc --noEmit`) and `npm test` (vitest) must pass; `npm run gen-types` must leave `src/api/schema.ts` unchanged (CI fails on drift).
- No `__all__`-style barrel maintenance beyond `src/views/index.tsx`, which already re-exports every view.
- Prettier-style of the existing SPA: double quotes, semicolons, 2-space indent (the kernel copy keeps QueryView's single-quote style — it is a verbatim copy).

## Decisions taken while planning

- **`OrderBy` adapter, not a schema change.** The backend serialises `OrderBy` as a JSON array (`["amount", "DESC"]`); the kernel's `FieldPickers` works with `{name, dir}` objects. `src/lib/viewer.ts` converts in both directions so the backend contract and the kernel copy both stay untouched.
- **`fields` semantics.** Kernel `FieldPickers` toggles client-side visibility; aaiclick sends the visible set as `fields` so the server projects only those columns (smaller payloads). An empty visible set means "all" on the wire (`fields: null`), matching `presentationForSave`.
- **Params are supported without `options_sql`.** The kernel's `parseQueryParams` accepts `options_sql`, which needs free SQL; aaiclick has no free-SQL endpoint, so specs with `optionsSql` are ignored and only static `options` params render. `applyParams` substitutes into the `where` expression.
- **`cell_view` lives on the saved query only.** The `@query` view keeps a draft in component state; Save writes it through `PUT /viewer/queries/{name}`.
- **Dashboards are read and run from the UI; authoring stays with agents/CLI.** The `@dashboard` view has a picker and a Refresh button; there is no HTML editor (spec: "dashboard picker, Save, sandboxed frame" — Save re-persists the loaded dashboard as-is, which the spec keeps for parity with QueryView's push flow; implemented as a no-op-safe re-PUT).
- **Vitest is added to aaiclick** for the kernel's own tests and `prompt.test.ts`. `npm test` runs `vitest run`; CI's lint job gains one step.
- **Job scope in `useObjects`** passes `scope=job&job=<ref>` to `GET /objects`; the query key includes the scope key so switching scopes never shows stale rows.

## File Structure

| File | Responsibility |
|------|----------------|
| `src/queryview-core/**` | verbatim kernel copy + `README.md` with the source commit |
| `src/styles/queryview-core.css` | the `glass-*` component classes the kernel's markup uses |
| `tsconfig.json`, `vite.config.ts`, `vitest.config.ts`, `package.json` | `@qv/core` alias, vitest |
| `src/api/types.ts` | re-exports for the viewer models |
| `src/api/client.ts` | `putJSON`, `deleteJSON` |
| `src/api/hooks.ts` | `useObjects`, `useObject`, `useQueryObject`, `useSavedQueries`, `useSaveQuery`, `useDeleteSavedQuery`, `useDashboards`, `useDashboard`, `useRunDashboard`, `useSaveDashboard` |
| `src/lib/viewer.ts` | scope key helpers, `OrderBy` ↔ `OrderCol`, `ObjectQueryResult` → `QueryRows`, `fieldsFromSchema`, `formatBytes` |
| `src/lib/viewer.test.ts` | unit tests for the adapters |
| `src/prompt.ts`, `src/prompt.test.ts` | the three routes |
| `src/components/ScopeTree.tsx` | Persistent + Jobs (with search) selector |
| `src/components/ObjectsTable.tsx` | objects of the selected scope |
| `src/components/QueryPanel.tsx` | where / limit / offset / params / cell-view modal / saved-query dropdown / CSV |
| `src/views/Data.tsx`, `src/views/Query.tsx`, `src/views/Dashboard.tsx` | the three modes |
| `src/App.tsx`, `src/views/index.tsx`, `src/views/Home.tsx` | routing and help entries |
| `test_e2e/web/test_viewer.py`, `test_e2e/web/seed.py` | Playwright coverage |
| `docs/designs/ui.md`, `docs/designs/frontend.md`, `docs/designs/viewer.md`, `.github/workflows/_test-reusable.yaml` | docs and CI |

---

### Task 1: Kernel copy, `@qv/core` alias, vitest

**Files:**
- Create: `src/queryview-core/**` (copy), `src/queryview-core/README.md`, `src/styles/queryview-core.css`, `vitest.config.ts`
- Modify: `tsconfig.json`, `vite.config.ts`, `package.json`, `src/styles/globals.css`, `.gitignore` (nothing to add — check), `.github/workflows/_test-reusable.yaml`

**Interfaces:**
- Produces: `import { ResultsTable, FieldPickers, … } from "@qv/core"` resolving in tsc, Vite, and vitest; `npm test` running the kernel's tests.

- [ ] **Step 1: Copy the kernel and record the commit**

```bash
cd /home/user/aaiclick
rm -rf src/queryview-core
cp -R /home/user/queryview/frontend/src/core src/queryview-core
QV_SHA=$(git -C /home/user/queryview rev-parse HEAD)
cat > src/queryview-core/README.md <<EOF
# queryview-core

Verbatim copy of QueryView's \`frontend/src/core\` (the backend-agnostic
rendering kernel), imported through the \`@qv/core\` alias.

- Source commit: \`$QV_SHA\`
- Never edit files here. A change goes to QueryView first, then the copy is
  refreshed: \`cp -R <queryview>/frontend/src/core src/queryview-core\`, update
  this commit, run \`npm run check\` and \`npm test\`.
- The \`glass-*\` classes its markup uses live in \`src/styles/queryview-core.css\`.

See \`docs/designs/viewer.md\`, "QueryView Coupling".
EOF
ls src/queryview-core
```

Expected: the `cells/`, `dashboard/`, `params/`, `presentation/`, `results/` folders, `index.ts`, and `README.md`.

- [ ] **Step 2: Add the alias and vitest**

```bash
npm install --save-dev vitest@^3 js-yaml@^4 @types/js-yaml@^4
npm install js-yaml@^4
```

(`js-yaml` is a runtime dependency of the kernel; keep it in `dependencies`.)

`tsconfig.json` — add inside `compilerOptions`:

```json
    "paths": { "@qv/core": ["src/queryview-core/index.ts"] }
```

`vite.config.ts`:

```ts
import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: { "@qv/core": fileURLToPath(new URL("./src/queryview-core/index.ts", import.meta.url)) },
  },
  build: {
    outDir: "aaiclick/server/static",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://localhost:8000",
      "/mcp": "http://localhost:8000",
    },
  },
});
```

`vitest.config.ts` (new):

```ts
import { fileURLToPath, URL } from "node:url";
import { defineConfig } from "vitest/config";

export default defineConfig({
  resolve: {
    alias: { "@qv/core": fileURLToPath(new URL("./src/queryview-core/index.ts", import.meta.url)) },
  },
  test: {
    environment: "node",
    include: ["src/**/*.test.{ts,tsx}"],
  },
});
```

`package.json` scripts — add `"test": "vitest run"`.

`tsconfig.node.json` — change `"include": ["vite.config.ts"]` to `"include": ["vite.config.ts", "vitest.config.ts"]`.

- [ ] **Step 3: Port the kernel's component classes**

`src/styles/queryview-core.css` — copy the `@layer components { … }` block from `/home/user/queryview/frontend/src/index.css` verbatim (the `.glass-panel`, `.glass-popover`, `.glass-chip`, `.glass-input`, `.glass-btn`, `.glass-btn-primary`, `.glass-toggle` rules), preceded by:

```css
/* Component classes used by the queryview-core kernel's markup. Copied from
   QueryView's frontend/src/index.css (@layer components) — refresh together
   with src/queryview-core. */
```

`src/styles/globals.css` — add after `@import "tailwindcss";`:

```css
@import "./queryview-core.css";
```

- [ ] **Step 4: Run the checks**

Run: `npm run check && npm test`
Expected: tsc clean; vitest reports the kernel's suites (`cellView`, `cellViewYaml`, `complexCells`, `srcDoc`, `queryParams`, `presentation`, `rows`) passing.

If tsc reports `verbatimModuleSyntax`/`erasableSyntaxOnly` differences inside `src/queryview-core`, do not edit the copy: the kernel already uses `import type` / `type` modifiers, so the remaining fix is adding the missing compiler option to `tsconfig.json`, never touching the copy.

- [ ] **Step 5: CI**

In `.github/workflows/_test-reusable.yaml`, after the "Type-check the SPA" step in the `lint` job, add:

```yaml
      - name: Run SPA unit tests
        run: npm test
```

- [ ] **Step 6: Commit**

```bash
git add src/queryview-core src/styles tsconfig.json tsconfig.node.json vite.config.ts vitest.config.ts package.json package-lock.json .github/workflows/_test-reusable.yaml
git commit -m "SPA: copy the QueryView kernel behind @qv/core; add vitest"
```

---

### Task 2: API types, client verbs, adapters

**Files:**
- Modify: `src/api/types.ts`, `src/api/client.ts`
- Create: `src/lib/viewer.ts`, `src/lib/viewer.test.ts`

**Interfaces:**
- Produces types `ObjectView`, `ObjectDetail`, `ColumnSchema`, `OrderByPair`, `ObjectQueryRequest`, `ObjectQueryResult`, `SavedQuery`, `SavedQueryBody`, `DashboardSummary`, `Dashboard`, `DashboardBody`, `DashboardResults`; `putJSON<T>(path, body)`, `deleteJSON<T>(path)`; helpers below.

- [ ] **Step 1: Write the failing tests**

`src/lib/viewer.test.ts`:

```ts
import { describe, expect, test } from "vitest";
import {
  formatBytes,
  fieldsFromSchema,
  orderColsToPairs,
  pairsToOrderCols,
  rowsFromResult,
  scopeKey,
  scopeLabel,
} from "./viewer";

describe("scope keys", () => {
  test("persistent and job", () => {
    expect(scopeKey(null)).toBe("persistent");
    expect(scopeKey("nightly_etl")).toBe("job:nightly_etl");
    expect(scopeLabel("persistent")).toBe("Persistent");
    expect(scopeLabel("job:123")).toBe("Job 123");
  });
});

describe("order by adapters", () => {
  test("round trip", () => {
    const cols = [
      { name: "a", dir: "ASC" as const },
      { name: "b", dir: "DESC" as const },
    ];
    expect(orderColsToPairs(cols)).toEqual([
      ["a", "ASC"],
      ["b", "DESC"],
    ]);
    expect(pairsToOrderCols(orderColsToPairs(cols))).toEqual(cols);
    expect(pairsToOrderCols(undefined)).toEqual([]);
  });
});

describe("rows and fields", () => {
  test("rowsFromResult keeps meta and data", () => {
    const rows = rowsFromResult({ meta: [{ name: "n", type: "Int64" }], data: [["1"]], text: null });
    expect(rows.meta).toEqual([{ name: "n", type: "Int64" }]);
    expect(rows.data).toEqual([["1"]]);
  });
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `npm test -- src/lib/viewer.test.ts`
Expected: FAIL, `Cannot find module './viewer'`.

- [ ] **Step 3: Implement**

`src/api/types.ts` — append after `RegisterJobRequest`:

```ts
export type ObjectView = S["ObjectView"];
export type ObjectDetail = S["ObjectDetail"];
export type ColumnInfo = S["ColumnInfo"];
export type ColumnSchema = S["ColumnSchema"];
export type OrderByPair = S["OrderBy"];
export type ObjectQueryRequest = S["ObjectQueryRequest"];
export type ObjectQueryResult = S["ObjectQueryResult"];
export type SavedQuery = S["SavedQuery"];
export type SavedQueryBody = S["SavedQueryBody"];
export type DashboardSummary = S["DashboardSummary"];
export type Dashboard = S["Dashboard"];
export type DashboardBody = S["DashboardBody"];
export type DashboardResults = S["DashboardResults"];
export type Deleted = S["Deleted"];
```

`src/api/client.ts` — append:

```ts
export async function putJSON<T>(path: string, body: unknown): Promise<T> {
  const res = await request(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

export async function deleteJSON<T>(path: string): Promise<T> {
  const res = await request(path, { method: "DELETE" });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}
```

`src/lib/viewer.ts`:

```ts
// Adapters between aaiclick's wire format and the queryview-core kernel.
import type { Field, OrderCol, QueryRows } from "@qv/core";
import type { ColumnInfo, ObjectQueryResult, OrderByPair } from "../api/types";

export const PERSISTENT = "persistent";
export const AAI_ID = "aai_id";

// A scope key as the backend expects it: "persistent" or "job:<id|name>".
export function scopeKey(job: string | null): string {
  return job ? `job:${job}` : PERSISTENT;
}

// The job reference inside a scope key, or null for the persistent scope.
export function jobOfScope(scope: string): string | null {
  return scope.startsWith("job:") ? scope.slice(4) : null;
}

export function scopeLabel(scope: string): string {
  const job = jobOfScope(scope);
  return job ? `Job ${job}` : "Persistent";
}

// The `@data` / `@query` prompt for a scope and optional object.
export function dataPrompt(scope: string, object?: string): string {
  const job = jobOfScope(scope);
  return ["@data", job ? `job ${job}` : "", object ?? ""].filter(Boolean).join(" ");
}

export function queryPrompt(scope: string, object?: string): string {
  const job = jobOfScope(scope);
  return ["@query", job ? `job ${job}` : "", object ?? ""].filter(Boolean).join(" ");
}

// The backend serialises OrderBy as a [name, dir] pair; the kernel wants objects.
export function orderColsToPairs(cols: OrderCol[]): OrderByPair[] {
  return cols.map((c) => [c.name, c.dir]);
}

export function pairsToOrderCols(pairs: OrderByPair[] | null | undefined): OrderCol[] {
  return (pairs ?? []).map(([name, dir]) => ({ name, dir }));
}

// ObjectQueryResult carries the kernel's {meta, data} contract unchanged.
export function rowsFromResult(result: ObjectQueryResult): QueryRows {
  return { meta: result.meta ?? [], data: (result.data ?? []) as QueryRows["data"] };
}

// The Fields picker's input, from the object's registered schema (aai_id hidden,
// like the server's projection).
export function fieldsFromSchema(columns: Record<string, Pick<ColumnInfo, "type">>): Field[] {
  return Object.entries(columns)
    .filter(([name]) => name !== AAI_ID)
    .map(([name, info]) => ({ name, type: info.type }));
}

export function formatBytes(bytes: number | null | undefined): string {
  if (bytes == null) return "—";
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = bytes / 1024;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return `${value.toFixed(1)} ${units[i]}`;
}
```

- [ ] **Step 4: Run tests and typecheck**

Run: `npm test -- src/lib/viewer.test.ts && npm run check`
Expected: PASS, tsc clean.

- [ ] **Step 5: Commit**

```bash
git add src/api/types.ts src/api/client.ts src/lib/viewer.ts src/lib/viewer.test.ts
git commit -m "SPA: viewer types, PUT/DELETE client verbs, kernel adapters"
```

---

### Task 3: Prompt routes

**Files:**
- Modify: `src/prompt.ts`
- Create: `src/prompt.test.ts`

**Interfaces:**
- Produces `Route` members `{ kind: "data"; job: string | null; object: string | null }`, `{ kind: "query"; job: string | null; object: string | null }`, `{ kind: "dashboard"; name: string | null }`.

- [ ] **Step 1: Write the failing tests**

`src/prompt.test.ts`:

```ts
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `npm test -- src/prompt.test.ts`
Expected: FAIL — `@data` parses as `{ kind: "unknown" }`.

- [ ] **Step 3: Implement**

In `src/prompt.ts`, extend the union:

```ts
  | { kind: "data"; job: string | null; object: string | null }
  | { kind: "query"; job: string | null; object: string | null }
  | { kind: "dashboard"; name: string | null }
```

Add above `parsePrompt`:

```ts
// `[job <ref>] [<object>]` after `@data` / `@query`.
function parseScoped(rest: string): { job: string | null; object: string | null } {
  const words = rest.split(/\s+/).filter(Boolean);
  let job: string | null = null;
  if (words[0] === "job" && words[1]) {
    job = words[1];
    words.splice(0, 2);
  }
  return { job, object: words[0] ?? null };
}
```

Add inside `parsePrompt`, before the `@job ` branch (so `@job` and `@jobs` keep their exact matches):

```ts
  if (p === "@data" || p.startsWith("@data ")) return { kind: "data", ...parseScoped(p.slice(5)) };
  if (p === "@query" || p.startsWith("@query ")) return { kind: "query", ...parseScoped(p.slice(6)) };
  if (p === "@dashboard") return { kind: "dashboard", name: null };
  if (p.startsWith("@dashboard ")) return { kind: "dashboard", name: p.slice(11).trim() || null };
```

- [ ] **Step 4: Run tests**

Run: `npm test -- src/prompt.test.ts && npm run check`
Expected: PASS (the `check` step will report `App.tsx` switch is non-exhaustive only if `renderRoute` has an explicit return type — it does not, so it stays green until Task 7 wires the views).

- [ ] **Step 5: Commit**

```bash
git add src/prompt.ts src/prompt.test.ts
git commit -m "SPA: @data, @query, @dashboard prompt routes"
```

---

### Task 4: React Query hooks

**Files:**
- Modify: `src/api/hooks.ts`

**Interfaces:**
- Produces:
  - `useObjects(scope: string)` → `Page<ObjectView>`; `useObject(name)` → `ObjectDetail` (persistent scope only — `GET /objects/{name}` is global-scope; for job scope the header reads from `useObjects`).
  - `useQueryObject()` → mutation `(req: ObjectQueryRequest) => ObjectQueryResult`.
  - `useSavedQueries(scope, object?)`, `useSaveQuery()`, `useDeleteSavedQuery()`.
  - `useDashboards()`, `useDashboard(name)`, `useRunDashboard()`, `useSaveDashboard()`.

- [ ] **Step 1: Implement** (append to `hooks.ts`; extend the type import with `Dashboard, DashboardBody, DashboardResults, DashboardSummary, Deleted, ObjectDetail, ObjectQueryRequest, ObjectQueryResult, ObjectView, SavedQuery, SavedQueryBody` and the client import with `deleteJSON, putJSON`)

```ts
// --- viewer ---------------------------------------------------------------

function objectsPath(scope: string): string {
  const job = scope.startsWith("job:") ? scope.slice(4) : null;
  return job ? `/objects?scope=job&job=${encodeURIComponent(job)}` : "/objects";
}

export function useObjects(scope: string) {
  return useQuery({
    queryKey: ["objects", scope],
    queryFn: () => fetchJSON<Page<ObjectView>>(objectsPath(scope)),
  });
}

export function useObject(name: string) {
  return useQuery({
    queryKey: ["object", name],
    queryFn: () => fetchJSON<ObjectDetail>(`/objects/${encodeURIComponent(name)}`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

// A query is a mutation: it runs on demand (Execute, paging, params) rather
// than polling, and its result is per-panel state.
export function useQueryObject() {
  return useMutation({
    mutationFn: (req: ObjectQueryRequest) => postJSON<ObjectQueryResult>("/viewer/query", req),
  });
}

export function useSavedQueries(scope: string, object?: string) {
  const params = new URLSearchParams({ scope });
  if (object) params.set("object", object);
  return useQuery({
    queryKey: ["saved-queries", scope, object ?? ""],
    queryFn: () => fetchJSON<Page<SavedQuery>>(`/viewer/queries?${params}`),
    refetchInterval: false,
  });
}

export function useSaveQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: SavedQueryBody }) =>
      putJSON<SavedQuery>(`/viewer/queries/${encodeURIComponent(name)}`, body),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["saved-queries"] }),
  });
}

export function useDeleteSavedQuery() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (name: string) => deleteJSON<Deleted>(`/viewer/queries/${encodeURIComponent(name)}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["saved-queries"] }),
  });
}

export function useDashboards() {
  return useQuery({
    queryKey: ["dashboards"],
    queryFn: () => fetchJSON<Page<DashboardSummary>>("/viewer/dashboards"),
    refetchInterval: false,
  });
}

export function useDashboard(name: string) {
  return useQuery({
    queryKey: ["dashboard", name],
    queryFn: () => fetchJSON<Dashboard>(`/viewer/dashboards/${encodeURIComponent(name)}`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

export function useRunDashboard(name: string) {
  return useQuery({
    queryKey: ["dashboard-results", name],
    queryFn: () => postJSON<DashboardResults>(`/viewer/dashboards/${encodeURIComponent(name)}:run`),
    enabled: name.length > 0,
    refetchInterval: false,
  });
}

export function useSaveDashboard() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: DashboardBody }) =>
      putJSON<Dashboard>(`/viewer/dashboards/${encodeURIComponent(name)}`, body),
    onSuccess: (_d, { name }) => {
      qc.invalidateQueries({ queryKey: ["dashboards"] });
      qc.invalidateQueries({ queryKey: ["dashboard", name] });
    },
  });
}
```

- [ ] **Step 2: Typecheck**

Run: `npm run check`
Expected: clean. (`SavedQueryBody` and `DashboardBody` carry `name: string` with a default of `""` in the schema; passing `{ ...body, name: "" }` is unnecessary because the server takes the name from the path — see `aaiclick/server/routers/viewer.py`.)

- [ ] **Step 3: Commit**

```bash
git add src/api/hooks.ts
git commit -m "SPA: viewer hooks for objects, queries, saved queries, dashboards"
```

---

### Task 5: `@data` — scope tree, objects table, object rows

**Files:**
- Create: `src/components/ScopeTree.tsx`, `src/components/ObjectsTable.tsx`, `src/views/Data.tsx`
- Modify: `src/styles/globals.css` (a `.viewer-layout` two-column rule)

**Interfaces:**
- Consumes Task 2 helpers, Task 4 hooks, `@qv/core` `ResultsTable`, `renderCell`, `columnNames`, `columnTypes`.
- Produces `Data({ job, object, onPrompt })`, `ScopeTree({ scope, onScope })`, `ObjectsTable({ objects, scope, onPrompt })`.

- [ ] **Step 1: `ScopeTree`**

```tsx
import { useState } from "react";
import { useJobs } from "../api/hooks";
import { PERSISTENT, scopeKey } from "../lib/viewer";

// Left-hand scope selector: Persistent, then the jobs (newest first) with a
// name filter. Job nodes reuse the jobs list; a click sets the scope key.
export function ScopeTree({ scope, onScope }: { scope: string; onScope: (scope: string) => void }) {
  const { data } = useJobs();
  const [filter, setFilter] = useState("");
  const jobs = (data?.items ?? []).filter((j) => j.name.includes(filter));
  return (
    <nav className="scope-tree" data-testid="scope-tree">
      <div
        className={`scope-node${scope === PERSISTENT ? " is-active" : ""}`}
        data-testid="scope-persistent"
        onClick={() => onScope(PERSISTENT)}
      >
        Persistent
      </div>
      <div className="scope-group">Jobs</div>
      <input
        className="scope-filter mono"
        placeholder="filter jobs…"
        value={filter}
        onChange={(e) => setFilter(e.target.value)}
        aria-label="Filter jobs"
      />
      {jobs.map((j) => {
        const key = scopeKey(String(j.id));
        return (
          <div
            key={j.id}
            className={`scope-node mono${scope === key ? " is-active" : ""}`}
            data-testid="scope-job"
            title={`#${j.id}`}
            onClick={() => onScope(key)}
          >
            {j.name} <span className="sub-inline">#{String(j.id).slice(-6)}</span>
          </div>
        );
      })}
    </nav>
  );
}
```

Note: `JobView.id` is a JSON string (64-bit snowflake); `String(j.id)` keeps it opaque.

- [ ] **Step 2: `ObjectsTable`**

```tsx
import type { ObjectView } from "../api/types";
import { relativeTime } from "../lib/format";
import { dataPrompt, formatBytes, queryPrompt } from "../lib/viewer";

export function ObjectsTable({
  objects,
  scope,
  onPrompt,
}: {
  objects: ObjectView[];
  scope: string;
  onPrompt: (v: string) => void;
}) {
  if (objects.length === 0) return <p className="sub">No objects in this scope</p>;
  return (
    <table data-testid="objects-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Rows</th>
          <th>Size</th>
          <th>Created</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {objects.map((o) => (
          <tr key={o.table} className="clickable" onClick={() => onPrompt(dataPrompt(scope, o.name))}>
            <td>
              <span className="name-link mono">{o.name}</span>
            </td>
            <td className="mono">{o.row_count ?? "—"}</td>
            <td className="mono">{formatBytes(o.size_bytes)}</td>
            <td>{relativeTime(o.created_at)}</td>
            <td className="row-actions">
              <button
                type="button"
                className="btn btn-sm"
                data-testid="object-query"
                onClick={(e) => {
                  e.stopPropagation();
                  onPrompt(queryPrompt(scope, o.name));
                }}
              >
                Query
              </button>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 3: `Data` view**

```tsx
import { useEffect, useMemo } from "react";
import { columnNames, columnTypes, renderCell, ResultsTable } from "@qv/core";
import { useObjects, useQueryObject } from "../api/hooks";
import { Chips } from "../components/Chips";
import { MetaGrid } from "../components/MetaGrid";
import { ObjectsTable } from "../components/ObjectsTable";
import { ScopeTree } from "../components/ScopeTree";
import { relativeTime } from "../lib/format";
import { dataPrompt, formatBytes, queryPrompt, rowsFromResult, scopeKey, scopeLabel } from "../lib/viewer";

const PREVIEW_LIMIT = 100;

export function Data({
  job,
  object,
  onPrompt,
}: {
  job: string | null;
  object: string | null;
  onPrompt: (v: string) => void;
}) {
  const scope = scopeKey(job);
  const objects = useObjects(scope);
  const selected = object ? objects.data?.items.find((o) => o.name === object) : undefined;

  return (
    <>
      <h2>Data</h2>
      <p className="sub">Objects of the selected scope · click a row to preview</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@query", cmd: queryPrompt(scope, object ?? undefined) },
          { label: "@dashboard", cmd: "@dashboard" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="viewer-layout">
        <ScopeTree scope={scope} onScope={(s) => onPrompt(dataPrompt(s))} />
        <div className="viewer-main">
          {objects.isLoading && <p className="sub">loading…</p>}
          {objects.isError && <p className="err">failed to load objects</p>}
          {object ? (
            <ObjectPreview scope={scope} name={object} view={selected} onPrompt={onPrompt} />
          ) : (
            objects.data && <ObjectsTable objects={objects.data.items} scope={scope} onPrompt={onPrompt} />
          )}
        </div>
      </div>
    </>
  );
}

function ObjectPreview({
  scope,
  name,
  view,
  onPrompt,
}: {
  scope: string;
  name: string;
  view: { row_count: number | null; size_bytes: number | null; created_at: string | null } | undefined;
  onPrompt: (v: string) => void;
}) {
  const query = useQueryObject();
  const { mutate } = query;
  useEffect(() => {
    mutate({ scope, object: name, limit: PREVIEW_LIMIT, offset: 0, fmt: "json" });
  }, [mutate, scope, name]);

  const rows = useMemo(() => (query.data ? rowsFromResult(query.data) : null), [query.data]);
  const columns = rows ? columnNames(rows) : [];
  const colTypes = rows ? columnTypes(rows) : {};

  return (
    <>
      <div className="detail-head">
        <div className="title-row">
          <h2>
            <span className="mono">{name}</span>
          </h2>
          <div className="spacer" />
          <button type="button" className="btn btn-sm" data-testid="preview-query" onClick={() => onPrompt(queryPrompt(scope, name))}>
            Query
          </button>
        </div>
        <MetaGrid
          items={[
            { k: "Scope", v: scopeLabel(scope) },
            { k: "Rows", v: view?.row_count ?? "—", mono: true },
            { k: "Size", v: formatBytes(view?.size_bytes) },
            { k: "Created", v: relativeTime(view?.created_at) },
          ]}
        />
      </div>
      <Chips chips={[{ label: `← ${scopeLabel(scope)}`, cmd: dataPrompt(scope) }]} onPrompt={onPrompt} />
      {query.isPending && <p className="sub">loading rows…</p>}
      {query.isError && <p className="err">{query.error.message}</p>}
      {rows && (
        <ResultsTable
          columns={columns}
          rows={rows.data}
          shownIdx={columns.map((_, i) => i)}
          testid="object-rows"
          renderCell={(col, value, row) => renderCell(col, value, {}, row, columns, colTypes)}
        />
      )}
    </>
  );
}
```

- [ ] **Step 4: Layout CSS** — append to `src/styles/globals.css`:

```css
/* ---- viewer (@data / @query) ---- */
.viewer-layout { display: grid; grid-template-columns: 220px 1fr; gap: 18px; align-items: start; }
.viewer-main { min-width: 0; }
.scope-tree { background: var(--panel); border: 1px solid var(--border); border-radius: 14px;
  backdrop-filter: var(--glass-blur); -webkit-backdrop-filter: var(--glass-blur);
  box-shadow: var(--hi), var(--shadow); padding: 10px; font-size: 13px; }
.scope-group { font-size: 11px; text-transform: uppercase; letter-spacing: .05em; color: var(--muted);
  font-weight: 700; margin: 12px 6px 6px; }
.scope-node { padding: 6px 8px; border-radius: 8px; cursor: pointer; overflow: hidden;
  text-overflow: ellipsis; white-space: nowrap; }
.scope-node:hover { background: rgba(91,157,255,.12); }
.scope-node.is-active { background: var(--accent-soft); color: var(--accent); }
.scope-node .sub-inline { color: var(--muted); font-size: 11px; }
.scope-filter { width: 100%; margin: 0 0 6px; padding: 5px 8px; border: 1px solid var(--border);
  border-radius: 8px; background: rgba(255,255,255,.05); color: var(--text); font-size: 12px; }
.scope-filter:focus { outline: none; border-color: var(--accent); }
```

- [ ] **Step 5: Typecheck**

Run: `npm run check`
Expected: clean. The view is not wired into `App.tsx` yet (Task 7).

- [ ] **Step 6: Commit**

```bash
git add src/components/ScopeTree.tsx src/components/ObjectsTable.tsx src/views/Data.tsx src/styles/globals.css
git commit -m "SPA: @data mode — scope tree, objects table, object preview"
```

---

### Task 6: `@query` — the query panel

**Files:**
- Create: `src/components/QueryPanel.tsx`, `src/views/Query.tsx`

**Interfaces:**
- Consumes `@qv/core`: `CellViewModal`, `FieldPickers`, `ResultsTable`, `applyParams`, `parseCellViewYaml`, `parseQueryParams`, `presentationForSave`, `renderCell`, `shownColumnIndices`, `columnNames`, `columnTypes`, types `Field`, `OrderCol`, `ParamDef`, `QueryRows`, `CellViewMap`.
- Produces `Query({ job, object, onPrompt })`, `QueryPanel({ scope, object, fields })`.

- [ ] **Step 1: `QueryPanel`**

```tsx
import { useEffect, useMemo, useState } from "react";
import {
  applyParams,
  CellViewModal,
  columnNames,
  columnTypes,
  FieldPickers,
  parseCellViewYaml,
  parseQueryParams,
  presentationForSave,
  renderCell,
  ResultsTable,
  shownColumnIndices,
  type CellViewMap,
  type Field,
  type OrderCol,
  type ParamDef,
  type QueryRows,
} from "@qv/core";
import { useDeleteSavedQuery, useQueryObject, useSavedQueries, useSaveQuery } from "../api/hooks";
import type { ObjectQueryRequest, SavedQuery } from "../api/types";
import { useToast } from "./Toast";
import { orderColsToPairs, pairsToOrderCols, rowsFromResult } from "../lib/viewer";

const NEW_NAME = "::new::";

function downloadText(filename: string, text: string) {
  const url = URL.createObjectURL(new Blob([text], { type: "text/csv" }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}

// Static `options` params only: `options_sql` needs free SQL, which aaiclick
// does not expose, so such specs are dropped.
function staticParams(cellView: string): ParamDef[] {
  return parseQueryParams(cellView)
    .filter((s) => s.options && s.options.length > 0)
    .map((s) => ({ name: s.name, options: s.options ?? [] }));
}

export function QueryPanel({ scope, object, fields }: { scope: string; object: string; fields: Field[] }) {
  const toast = useToast();
  const run = useQueryObject();
  const saved = useSavedQueries(scope, object);
  const save = useSaveQuery();
  const remove = useDeleteSavedQuery();

  const [where, setWhere] = useState("");
  const [limit, setLimit] = useState(100);
  const [offset, setOffset] = useState(0);
  const [visibleCols, setVisibleCols] = useState<string[]>(() => fields.map((f) => f.name));
  const [orderBy, setOrderBy] = useState<OrderCol[]>([]);
  const [selectedName, setSelectedName] = useState("");
  const [cellView, setCellView] = useState("");
  const [modalOpen, setModalOpen] = useState(false);
  const [paramValues, setParamValues] = useState<Record<string, string>>({});
  const [rows, setRows] = useState<QueryRows | null>(null);
  const [error, setError] = useState<string | null>(null);

  const views = useMemo<CellViewMap>(() => parseCellViewYaml(cellView), [cellView]);
  const paramDefs = useMemo(() => staticParams(cellView), [cellView]);
  const colTypes = useMemo(() => (rows ? columnTypes(rows) : {}), [rows]);

  // Seed each param to its first option, keeping a still-valid pick.
  useEffect(() => {
    setParamValues((prev) => {
      const next: Record<string, string> = {};
      for (const d of paramDefs) next[d.name] = d.options.includes(prev[d.name]) ? prev[d.name] : d.options[0];
      return next;
    });
  }, [paramDefs]);

  function request(nextOffset: number, params = paramValues): ObjectQueryRequest {
    const { fields: f, order_by } = presentationForSave(orderBy, visibleCols);
    return {
      scope,
      object,
      where: where.trim() ? applyParams(where, paramDefs, params) : null,
      fields: f,
      order_by: orderColsToPairs(order_by ?? []),
      limit,
      offset: nextOffset,
      fmt: "json",
    };
  }

  function execute(nextOffset: number, params?: Record<string, string>) {
    setError(null);
    run.mutate(request(nextOffset, params), {
      onSuccess: (data) => {
        setRows(rowsFromResult(data));
        setOffset(nextOffset);
      },
      onError: (e) => setError(e.message),
    });
  }

  function downloadCsv() {
    run.mutate(
      { ...request(offset), fmt: "csv" },
      {
        onSuccess: (data) => downloadText(`${object}.csv`, data.text ?? ""),
        onError: (e) => setError(e.message),
      },
    );
  }

  function applySaved(q: SavedQuery) {
    setWhere(q.where ?? "");
    setOrderBy(pairsToOrderCols(q.order_by));
    setVisibleCols(q.fields?.length ? q.fields : fields.map((f) => f.name));
    setCellView(q.cell_view ?? "");
  }

  function onSelectName(value: string) {
    if (value === NEW_NAME) {
      const name = window.prompt("Save query as (name):", selectedName)?.trim();
      if (name) setSelectedName(name);
      return;
    }
    setSelectedName(value);
    const q = saved.data?.items.find((s) => s.name === value);
    if (q) applySaved(q);
  }

  function persist(cellViewValue = cellView) {
    const name = selectedName.trim();
    if (!name) return;
    save.mutate(
      {
        name,
        body: {
          name,
          scope,
          object,
          where: where.trim() || null,
          fields: presentationForSave(orderBy, visibleCols).fields,
          order_by: orderColsToPairs(orderBy),
          cell_view: cellViewValue || null,
        },
      },
      {
        onSuccess: () => {
          setCellView(cellViewValue);
          setModalOpen(false);
          toast(`Saved query '${name}'`);
        },
        onError: (e) => setError(e.message),
      },
    );
  }

  function deleteSaved() {
    const name = selectedName.trim();
    if (!name || !saved.data?.items.some((s) => s.name === name)) return;
    remove.mutate(name, {
      onSuccess: () => {
        setSelectedName("");
        toast(`Deleted query '${name}'`);
      },
      onError: (e) => setError(e.message),
    });
  }

  const columns = rows ? columnNames(rows) : [];
  const shownIdx = shownColumnIndices(columns, fields, visibleCols);
  const busy = run.isPending || save.isPending;

  return (
    <section className="panel viewer-panel" data-testid="query-panel">
      <div className="field inline">
        <select
          data-testid="query-saved-select"
          aria-label="Saved queries"
          value={selectedName}
          onChange={(e) => onSelectName(e.target.value)}
        >
          <option value="">Saved queries…</option>
          <option value={NEW_NAME}>+ New name…</option>
          {selectedName !== "" && !saved.data?.items.some((s) => s.name === selectedName) && (
            <option value={selectedName}>{selectedName}</option>
          )}
          {saved.data?.items.map((s) => (
            <option key={s.name} value={s.name}>
              {s.name}
            </option>
          ))}
        </select>
        <button type="button" className="btn btn-sm" data-testid="query-save" disabled={busy || !selectedName.trim()} onClick={() => persist()}>
          Save
        </button>
        <button type="button" className="btn btn-sm" data-testid="query-delete" disabled={busy || !selectedName.trim()} onClick={deleteSaved}>
          Delete
        </button>
        <button type="button" className="btn btn-sm" data-testid="cell-view-toggle" onClick={() => setModalOpen(true)}>
          Cell view
        </button>
      </div>

      {paramDefs.length > 0 && (
        <div className="field inline" data-testid="query-params">
          {paramDefs.map((def) => (
            <label key={def.name}>
              {def.name}
              <select
                data-testid="param-select"
                data-param={def.name}
                value={paramValues[def.name] ?? def.options[0]}
                onChange={(e) => {
                  const next = { ...paramValues, [def.name]: e.target.value };
                  setParamValues(next);
                  execute(0, next);
                }}
              >
                {def.options.map((opt) => (
                  <option key={opt} value={opt}>
                    {opt}
                  </option>
                ))}
              </select>
            </label>
          ))}
        </div>
      )}

      <div className="field">
        <label htmlFor="query-where">
          where <span className="help">SQL boolean expression over the object's columns; {"{name}"} substitutes a param</span>
        </label>
        <textarea id="query-where" data-testid="query-where" value={where} onChange={(e) => setWhere(e.target.value)} placeholder="amount > 100 AND status = 'paid'" />
      </div>

      <div className="field inline">
        <button type="button" className="btn btn-primary btn-sm" data-testid="query-run" disabled={busy} onClick={() => execute(offset)}>
          Execute
        </button>
        <label>
          Limit <input type="number" min={1} max={1000} value={limit} data-testid="query-limit" onChange={(e) => setLimit(Math.min(1000, Math.max(1, Number(e.target.value) || 1)))} />
        </label>
        <label>
          Offset <input type="number" min={0} value={offset} data-testid="query-offset" onChange={(e) => setOffset(Math.max(0, Number(e.target.value) || 0))} />
        </label>
        <button type="button" className="btn btn-sm" data-testid="query-prev" disabled={busy || offset === 0} onClick={() => execute(Math.max(0, offset - limit))}>
          ← Previous
        </button>
        <button type="button" className="btn btn-sm" data-testid="query-next" disabled={busy} onClick={() => execute(offset + limit)}>
          Next →
        </button>
        <button type="button" className="btn btn-sm" data-testid="query-csv" disabled={busy} onClick={downloadCsv}>
          Download CSV
        </button>
      </div>

      <FieldPickers
        fields={fields}
        visibleCols={visibleCols}
        orderBy={orderBy}
        onVisibleColsChange={setVisibleCols}
        onOrderByChange={setOrderBy}
        orderHeaderExtra={
          <button type="button" data-testid="orderby-run" className="btn btn-sm" disabled={busy} onClick={() => execute(offset)}>
            Run
          </button>
        }
      />

      {modalOpen && (
        <CellViewModal
          initial={cellView}
          onCancel={() => setModalOpen(false)}
          onSave={(value) => {
            if (selectedName.trim()) persist(value);
            else {
              setCellView(value);
              setModalOpen(false);
            }
          }}
          saveDisabled={busy}
        />
      )}

      {error && (
        <p className="err" data-testid="query-error">
          {error}
        </p>
      )}
      {rows && (
        <ResultsTable
          columns={columns}
          rows={rows.data}
          shownIdx={shownIdx}
          testid="query-output"
          renderCell={(col, value, row) => renderCell(col, value, views, row, columns, colTypes)}
        />
      )}
    </section>
  );
}
```

`useToast()` (`src/components/Toast.tsx`) returns a `(message: string) => void` function.

- [ ] **Step 2: `Query` view**

```tsx
import { useMemo } from "react";
import { useObjects, useObject } from "../api/hooks";
import { Chips } from "../components/Chips";
import { QueryPanel } from "../components/QueryPanel";
import { ScopeTree } from "../components/ScopeTree";
import { dataPrompt, fieldsFromSchema, queryPrompt, scopeKey, scopeLabel } from "../lib/viewer";

export function Query({
  job,
  object,
  onPrompt,
}: {
  job: string | null;
  object: string | null;
  onPrompt: (v: string) => void;
}) {
  const scope = scopeKey(job);
  const objects = useObjects(scope);
  // The Fields picker needs column types; `GET /objects/{name}` serves the
  // persistent scope. Job-scoped objects fall back to a name-only field list
  // built from the first result (see QueryPanel's `fields` prop below).
  const detail = useObject(job ? "" : (object ?? ""));
  const fields = useMemo(
    () => (detail.data ? fieldsFromSchema(detail.data.table_schema.columns) : []),
    [detail.data],
  );

  return (
    <>
      <h2>Query</h2>
      <p className="sub">Filter, order, and page through one object · save as a named query</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@data", cmd: dataPrompt(scope, object ?? undefined) },
          { label: "@dashboard", cmd: "@dashboard" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="viewer-layout">
        <ScopeTree scope={scope} onScope={(s) => onPrompt(queryPrompt(s))} />
        <div className="viewer-main">
          <div className="field inline">
            <label htmlFor="query-object">Object</label>
            <select
              id="query-object"
              data-testid="query-object"
              value={object ?? ""}
              onChange={(e) => onPrompt(queryPrompt(scope, e.target.value || undefined))}
            >
              <option value="">Pick an object in {scopeLabel(scope)}…</option>
              {objects.data?.items.map((o) => (
                <option key={o.table} value={o.name}>
                  {o.name}
                </option>
              ))}
            </select>
          </div>
          {object && <QueryPanel key={`${scope}/${object}`} scope={scope} object={object} fields={fields} />}
        </div>
      </div>
    </>
  );
}
```

`key={`${scope}/${object}`}` remounts the panel when the object changes so its state resets.

For job-scoped objects `fields` is `[]`; `shownColumnIndices` then shows every result column (a column outside `fields` always shows), and `FieldPickers` renders no toggles. That matches the spec's "The Fields picker is fed from `useObject`'s schema" — job-scope detail is in `docs/designs/future.md` (add the line in Task 9).

- [ ] **Step 3: Typecheck**

Run: `npm run check`
Expected: clean.

- [ ] **Step 4: Commit**

```bash
git add src/components/QueryPanel.tsx src/views/Query.tsx
git commit -m "SPA: @query mode — query panel over the kernel's pickers and cell views"
```

---

### Task 7: `@dashboard` view and routing

**Files:**
- Create: `src/views/Dashboard.tsx`
- Modify: `src/views/index.tsx`, `src/App.tsx`, `src/views/Home.tsx`

- [ ] **Step 1: `Dashboard` view**

```tsx
import { DashboardFrame } from "@qv/core";
import { useDashboard, useDashboards, useRunDashboard, useSaveDashboard } from "../api/hooks";
import { Chips } from "../components/Chips";

export function Dashboard({ name, onPrompt }: { name: string | null; onPrompt: (v: string) => void }) {
  const list = useDashboards();
  const dashboard = useDashboard(name ?? "");
  const results = useRunDashboard(name ?? "");
  const save = useSaveDashboard();

  return (
    <>
      <h2>Dashboard</h2>
      <p className="sub">Agent-authored HTML over saved object queries, rendered in a sandbox</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@data", cmd: "@data" },
          { label: "@query", cmd: "@query" },
        ]}
        onPrompt={onPrompt}
      />
      <div className="field inline">
        <select
          data-testid="dashboard-select"
          aria-label="Dashboards"
          value={name ?? ""}
          onChange={(e) => onPrompt(e.target.value ? `@dashboard ${e.target.value}` : "@dashboard")}
        >
          <option value="">Select a dashboard…</option>
          {name && !list.data?.items.some((d) => d.name === name) && <option value={name}>{name}</option>}
          {list.data?.items.map((d) => (
            <option key={d.name} value={d.name}>
              {d.name}
            </option>
          ))}
        </select>
        <button type="button" className="btn btn-sm" data-testid="dashboard-refresh" disabled={!name || results.isFetching} onClick={() => results.refetch()}>
          Refresh
        </button>
        <button
          type="button"
          className="btn btn-sm"
          data-testid="dashboard-save"
          disabled={!dashboard.data || save.isPending}
          onClick={() => {
            const d = dashboard.data;
            if (d) save.mutate({ name: d.name, body: { name: d.name, scope: d.scope, html: d.html, queries: d.queries } });
          }}
        >
          Save
        </button>
      </div>
      {!name && (
        <p className="sub" data-testid="dashboard-empty">
          {list.data?.items.length ? "Pick a dashboard to view it." : "No dashboards yet. Save one through the API, MCP, or `view dashboards save`."}
        </p>
      )}
      {(dashboard.isError || results.isError) && (
        <p className="err" data-testid="dashboard-error">
          {dashboard.error?.message ?? results.error?.message}
        </p>
      )}
      {name && results.isPending && !results.isError && (
        <p className="sub" data-testid="dashboard-loading">
          Running queries…
        </p>
      )}
      {dashboard.data && results.data && <DashboardFrame html={dashboard.data.html} results={results.data.results} />}
    </>
  );
}
```

- [ ] **Step 2: Wire routes**

`src/views/index.tsx` — add:

```ts
export { Data } from "./Data";
export { Query } from "./Query";
export { Dashboard } from "./Dashboard";
```

`src/App.tsx` — import `Dashboard, Data, Query` from `./views` and add cases before `"unknown"`:

```tsx
    case "data":
      return <Data job={route.job} object={route.object} onPrompt={onPrompt} />;
    case "query":
      return <Query job={route.job} object={route.object} onPrompt={onPrompt} />;
    case "dashboard":
      return <Dashboard name={route.name} onPrompt={onPrompt} />;
```

`src/views/Home.tsx` — add to `NAVIGATE` after `@task <id>`:

```ts
  { code: "@data [job <ref>] [<object>]", desc: "Objects of a scope — persistent or one job — and their rows.", cmd: "@data" },
  { code: "@query [job <ref>] [<object>]", desc: "Filter, order, page, and save a query over one object.", cmd: "@query" },
  { code: "@dashboard [name]", desc: "Render a saved dashboard over object queries.", cmd: "@dashboard" },
```

- [ ] **Step 3: Verify in the browser**

Run: `npm run check && npm test && npm run build`
Then start the server (`uv run python -m aaiclick local start` in the background, or `uv run uvicorn aaiclick.server.app:app --port 8000`) and, with the seed from Task 8 or a quick script, open `/?p=@data`, `/?p=@query orders`, `/?p=@dashboard`. Confirm: the scope tree lists Persistent and jobs; an object row previews rows; `@query` executes and pages; a saved query appears in the dropdown after Save.

- [ ] **Step 4: Commit**

```bash
git add src/views/Dashboard.tsx src/views/index.tsx src/App.tsx src/views/Home.tsx
git commit -m "SPA: @dashboard mode and routing for the viewer modes"
```

---

### Task 8: Playwright coverage

**Files:**
- Create: `test_e2e/web/test_viewer.py`
- Modify: `test_e2e/web/seed.py` (a `seed_viewer_objects()` helper)

- [ ] **Step 1: Seed helper** (append to `seed.py`; add imports `from aaiclick import create_object_from_value` and `from aaiclick.internal_api import viewer as viewer_api`, `from aaiclick.viewer.view_models import DashboardIn, ObjectQuery`)

```python
async def seed_viewer_objects() -> None:
    """A persistent ``orders`` object and a dashboard over it, for the viewer e2e.

    ``with_ch=True`` is needed to create the ClickHouse table, so this must run
    before the e2e server starts (it holds the chdb lock afterwards) — the
    ``viewer_seed`` fixture in ``test_viewer.py`` is session-scoped and ordered
    before ``base_url``.
    """
    async with orch_context(with_ch=True):
        await create_object_from_value(
            {"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [10, 20, 30]}, name="orders", scope="global"
        )
        await viewer_api.save_dashboard(
            DashboardIn(
                name="sales",
                html="<h1 id='title'>Sales</h1><script>document.getElementById('title').textContent = 'rows:' + window.queries.top.name.length</script>",
                queries={"top": ObjectQuery(object="orders", fields=["name", "amount"])},
            )
        )
```

- [ ] **Step 2: Tests**

`test_e2e/web/test_viewer.py`:

```python
"""Playwright coverage for the viewer modes (`@data`, `@query`, `@dashboard`)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from helpers import open_page
from seed import seed_viewer_objects

from aaiclick.backend import is_local

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"
pytest.importorskip("playwright.sync_api")

_spa_built = pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
_local_only = pytest.mark.skipif(not is_local(), reason="seeds through chdb before the server starts")


@pytest.fixture(scope="session", autouse=True)
def viewer_seed(request) -> None:
    # Seed before the server takes the chdb lock: request base_url only after.
    if is_local():
        asyncio.run(seed_viewer_objects())
    request.getfixturevalue("base_url")


@_spa_built
@_local_only
def test_data_lists_and_previews_object(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@data")
    page.get_by_test_id("objects-table").get_by_text("orders").wait_for(timeout=15000)
    page.get_by_test_id("objects-table").get_by_text("orders").click()
    page.wait_for_url(lambda url: "orders" in url)
    rows = page.get_by_test_id("object-rows")
    rows.wait_for(timeout=15000)
    assert rows.locator("tbody tr").count() == 3


@_spa_built
@_local_only
def test_query_runs_and_pages(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@query orders")
    page.get_by_test_id("query-panel").wait_for(timeout=15000)
    page.get_by_test_id("query-where").fill("amount >= 20")
    page.get_by_test_id("query-limit").fill("1")
    page.get_by_test_id("query-run").click()
    out = page.get_by_test_id("query-output")
    out.wait_for(timeout=15000)
    assert out.locator("tbody tr").count() == 1
    page.get_by_test_id("query-next").click()
    page.wait_for_function("() => document.querySelector('[data-testid=query-offset]').value === '1'")
    assert out.locator("tbody tr").count() == 1


@_spa_built
@_local_only
def test_dashboard_renders_saved_dashboard(page, base_url: str) -> None:
    open_page(page, f"{base_url}/?p=@dashboard sales")
    frame = page.frame_locator("[data-testid='dashboard-frame']")
    frame.locator("#title").wait_for(timeout=15000)
    assert frame.locator("#title").inner_text() == "rows:3"
```

- [ ] **Step 3: Run**

Run: `npm run build && uv run pytest test_e2e/web/test_viewer.py -v -p no:cov`
Expected: 3 passed (local mode).

If the seed's `with_ch=True` conflicts with a server already started by another session-scoped fixture, move the `base_url` request out of `viewer_seed` and instead order fixtures by listing `viewer_seed` before `page` in each test's signature — the important property is "seed first, then server".

- [ ] **Step 4: Commit**

```bash
git add test_e2e/web/test_viewer.py test_e2e/web/seed.py
git commit -m "e2e: viewer modes — @data preview, @query paging, @dashboard frame"
```

---

### Task 9: Docs and full verification

**Files:**
- Modify: `docs/designs/ui.md`, `docs/designs/frontend.md`, `docs/designs/viewer.md`, `docs/designs/future.md`

- [ ] **Step 1: `ui.md`** — add three `## …` sections under "Modes", each with the prompt, one paragraph, and an `**Implementation**:` line pointing at `src/views/Data.tsx` (`Data`), `src/views/Query.tsx` (`Query`, `src/components/QueryPanel.tsx`), `src/views/Dashboard.tsx` (`Dashboard`), and the queryview-core pieces used (`ResultsTable`, `FieldPickers`, `CellViewModal`, `DashboardFrame`). Add the three prompts to the Navigation table.

- [ ] **Step 2: `frontend.md`** — in "Project layout" add `src/queryview-core/  ← verbatim QueryView kernel (@qv/core), see docs/designs/viewer.md` and `src/lib/viewer.ts`; in "Build & dev workflow" add `npm test` → `vitest run`; in "Backend endpoints consumed" add the `useObjects` → `GET /objects`, `useQueryObject` → `POST /viewer/query`, `useSavedQueries`/`useSaveQuery` → `/viewer/queries`, `useDashboards`/`useDashboard`/`useRunDashboard` → `/viewer/dashboards…` rows with `aaiclick/server/routers/viewer.py`.

- [ ] **Step 3: `viewer.md`** — under "Frontend", add `**Implementation**:` references (files above) and note the `OrderBy` pair ↔ `OrderCol` adapter in `src/lib/viewer.ts`; under "QueryView Coupling → What is copied" keep the text and point at `src/queryview-core/README.md` for the commit; update Rollout items 2–4 to "landed" with references; under "Testing" replace the vitest/Playwright bullets with the real file names (`src/prompt.test.ts`, `src/lib/viewer.test.ts`, `test_e2e/web/test_viewer.py`).

- [ ] **Step 4: `future.md`** — under "Viewer Follow-ups" add: `options_sql` params (needs a free-SQL endpoint), typed Fields picker for job-scoped objects (`GET /objects/{name}` is persistent-only today), dashboard authoring in the UI.

- [ ] **Step 5: Run the `shortify` and `markdown-style` skills over the four docs.**

- [ ] **Step 6: Full verification**

Run: `npm run gen-types && git diff --exit-code src/api/schema.ts && npm run check && npm test && npm run build && uv run pytest -q -p no:cacheprovider && uv run pre-commit run --all-files`
Expected: all green.

- [ ] **Step 7: Commit, push, check CI**

```bash
git add docs src
git commit -m "docs: viewer frontend modes, kernel copy, and implementation references"
git push -u origin claude/queryview-aaiclick-plugin-rm5lrl
```

Then run the `check-pr` skill (or open the PR first if none exists — that is the user's call).
