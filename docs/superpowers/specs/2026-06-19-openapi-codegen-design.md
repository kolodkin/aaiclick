# OpenAPI Codegen for the SPA — Design

Date: 2026-06-19

## Problem

`src/api/types.ts` is hand-written to mirror the pydantic view models behind
the FastAPI server. When the API surface changes, the TypeScript types can
drift silently — there is no check that they still match the server schema.
`docs/future.md` ("OpenAPI Codegen", Deferred) calls for generating the types
from `GET /api/v0/openapi.json` with a CI freshness gate.

## Approach

Generate the TypeScript types from the server's OpenAPI schema and reduce the
hand-written file to a thin re-export shim, so consumers stay unchanged while
the generated types can no longer drift.

### 1. Schema dumper (Python)

`aaiclick/server/dump_openapi.py` — a `__main__` module that prints
`json.dumps(app.openapi(), ...)` to stdout. FastAPI produces the schema
in-process via `app.openapi()`, so no running server is required. Kept out of
the base CLI so server-only deps stay optional; invoked with
`uv run --extra server`.

### 2. Codegen pipeline (npm)

Add `openapi-typescript` as a dev dependency and a script:

- `gen-types`: dump the schema and pipe it through `openapi-typescript`,
  writing `src/api/schema.ts` (the generated, committed artifact). No
  intermediate `openapi.json` is committed — `schema.ts` is the single
  generated source of truth.

### 3. `src/api/types.ts` → re-export shim

`schema.ts` exposes one `components["schemas"][...]` shape. `types.ts` becomes
a small hand-curated shim that maps ergonomic names onto it:

```ts
import type { components } from "./schema";
type S = components["schemas"];

export type JobStatus = S["JobView"]["status"];
export type TaskStatus = S["TaskView"]["status"];
export type JobView = S["JobView"];
export type TaskView = S["TaskView"];
export type JobDetail = S["JobDetail"];
export type TaskDetail = S["TaskDetail"];
export type TaskLogs = S["TaskLogsView"];          // server name differs
export type RegisteredJobView = S["RegisteredJobView"];
export type Problem = S["Problem"];
export type RunJobRequest = S["RunJobRequest"];
export type RegisterJobRequest = S["RegisterJobRequest"];

// FastAPI emits concrete Page_JobView_ etc.; keep a hand generic so call
// sites stay `Page<JobView>`.
export interface Page<T> {
  items: T[];
  total: number | null;
  next_cursor: string | null;
}
```

Consumers (`hooks.ts`, `components/JobsTable.tsx`, `TasksTable.tsx`,
`StatusBadge.tsx`) import from `./types` and are unchanged.

### 4. CI freshness gate

A new job in `.github/workflows/_test-reusable.yaml` (node + uv): `npm ci` →
`npm run gen-types` → `git diff --exit-code src/api/schema.ts` (fails on
drift). Also runs `npm run check` (`tsc --noEmit`) — currently gated nowhere in
PR CI — which the shim needs anyway to catch mapping mistakes.

### 5. Docs

Remove the "OpenAPI Codegen" entry from `docs/future.md` and add a short
codegen-workflow note to `docs/frontend.md`.

## Rejected alternatives

- **Full replacement** (delete `types.ts`, refactor consumers to
  `components["schemas"][...]`): uglier call sites, loses the `Page<T>`
  generic, more churn for no extra safety.
- **Commit `openapi.json` as an intermediate**: a second generated artifact to
  keep fresh; piping straight to `openapi-typescript` avoids it.

## Success criteria

- `npm run gen-types` regenerates `src/api/schema.ts` from the live server
  schema.
- `tsc --noEmit` passes with the shim in place; no consumer changes.
- CI fails if `schema.ts` is out of date relative to the server schema.
- `docs/future.md` no longer lists OpenAPI Codegen.
