Future Plans
---

Planned work across aaiclick, ordered by priority.

# Blob Storage Support

Shipped: export to a bucket and read-only located Objects
(`docs/designs/blob_storage.md`). Still open:

- **Named located Objects** — `name` / `scope` so a pointer is reopenable by
  name on another server (`schema_doc` already carries the location).
- **GCS and Azure** — `gs://` via the `S3` engine with HMAC keys, `az://` via
  `AzureBlobStorage`.
- **Per-object credentials** — a named collection chosen per object, e.g.
  `open_object_from_url(url, connection="lake")` rendering
  `s3(lake, url=...)`: keys in ClickHouse config, several buckets per
  deploy; needs `named_collection_control` grants. Or the caller's own keys
  in SQL, which needs oplog redaction.
- **Lake formats** (Iceberg, Delta) as further schemes.

Rejected: a writable located Object. The S3 engine's only append is a new file
per insert read back through a glob — export to a new key under a prefix gives
the same layout without unverified read-after-write, concurrent-writer hazards
or orphan files. Revisit only if several tasks must append in place to one
bucket-backed table.

# Coverage Reporting

Coverage is off by default: its CI log table went unread and it slowed every
unit job. `pytest-cov` stays installed for ad hoc `--cov` runs (e.g. the
`python-testing-style` zero-lines-lost check).

Bring it back as one nightly job, outside the per-PR jobs, that publishes the
report where it is read (HTML artifact, or a coverage service with PR diffs).

# Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce
with the v1.0.0 release.

# Deferred

Items deferred until preconditions are met.

## Kubernetes Worker Follow-ups

- **Pod-side claim renewal**: a worker outage longer than `WORKER_TIMEOUT`
  (10 min) still reaps running Pods. The module entrypoint (and a sidecar for
  shell Pods) could renew `claim_expires_at` itself, epoch-fenced, so a run
  survives any outage.
- **Node-pool targeting**: `nodeSelector` / tolerations on `pod_config`, to
  send a job to a Karpenter `NodePool` or a GPU pool.
- **Per-replica limits**: the capacity gate counts namespace-wide, so worker
  replicas share `AAICLICK_K8S_MAX_*` instead of each getting its own.

## Build Quotas

Shell/jvm children can declare image builds, so one job can queue many builds
on the shared buildkitd. Add a per-job cap on injected build tasks and
per-build buildkitd resource limits.

## Objects in Non-Python Tasks

Unsupported. Shell and jvm tasks speak the Task Interface's file data plane
(`task_interface.md`: plain values and resolved upstream refs); `object` /
`view` refs are rejected at commit. An Object needs a per-language task
runner that owns the ClickHouse dependency and the lifecycle (refcounts,
pins, ephemeral-table ownership, oplog).

## Task Cache Directories

Tasks have no cache that outlives their process or container, so a container
task re-downloads model weights, datasets or package caches on every run
(e.g. Ollama weights for an LLM step). Add two cache scopes a task reads and
writes by path:

| Scope  | Lifetime                                           | Example                          |
|--------|----------------------------------------------------|----------------------------------|
| job    | Shared by a job's tasks, dropped when the job ends | intermediate files between tasks |
| global | Shared by every job, kept until evicted            | model weights, HF / pip caches   |

| Runner     | Backing                                                 |
|------------|---------------------------------------------------------|
| subprocess | Directories under `~/.aaiclick` (`AAICLICK_LOCAL_ROOT`) |
| docker     | Host directories bind-mounted into the container        |
| kubernetes | PersistentVolumeClaims mounted into the Pod             |

Open points: how a task names the paths (env vars vs. a helper), concurrent
writers to one global entry, eviction and size limits, and ReadWriteMany
storage or node affinity on Kubernetes.

**When to revisit**: when an example project or user job runs a container task
with a large download it should not repeat.

## Protect Upstream Outputs on Re-run

`clear_task` re-runs a task against upstream outputs that may be gone: a
consumer releases its pins once it succeeds, so cleanup can drop those
tables, and the re-run fails with `LookupError`.

Re-pinning only on clear is not enough. Deleting an upstream output and
changing it (re-running the producer) are the same hazard, so protect both
or neither.

**When to revisit**: when re-running a single task of a finished job,
without re-running its upstream, becomes a supported workflow.

## ClickHouse Settings Pass-Through

A general way to send ClickHouse settings with the queries an Object runs.
Today `create_object_from_url`, `insert_from_url` and `export` take
`ch_settings`, for their one statement.

- **Per call**: `ch_settings=` on the other calls that run a single statement
  (`data`, `open_object_from_url`).
- **Per Object**: `ch_settings` stored on the Object and in `schema_doc`, so
  it survives task boundaries; every statement reading its table passes it.
- Views inherit; an operator result is a plain table and inherits nothing;
  mixed operands merge, and a conflicting value for one setting raises.

Not a credential mechanism: ClickHouse checks
`s3_allow_server_credentials_in_user_queries` on every read, and a per-Object
value would let any caller lend themselves the server's identity
(`docs/designs/blob_storage.md` — "Credentials").

**When to revisit**: when a workload needs per-query tuning (memory limits,
format or inference settings) that a connection default cannot express.

## Quote Column Names in Generated SQL

`join` quotes its identifiers; other Object operations still splice raw
column names into SQL, so a column named `from` or `a b` breaks them. Known
sites: the default insert list in `aaiclick/data/object/emit.py`, the copy
path in `ingest.py` and the URL ingest in `url.py`. Fix it once at the shared
column-list builders rather than per operation, then audit the remaining
SELECT projections. Dotted names, the same gap:
`technical_debt.md` — "Unquoted Identifier Interpolation in Remaining SQL Builders".

**When to revisit**: before accepting user-supplied column names from
external sources (CSV headers, parquet) as a supported path.

## Tenants — Kubernetes Control Plane

Multi-tenancy as a fleet layer rather than a filtered column: a control
plane provisions one full aaiclick installation per tenant, each in its own
Kubernetes namespace, with central identity and direct routing to each
tenant's own ingress. An installation carries no tenant state (its RBAC:
`docs/designs/auth.md`). Kubernetes only — no Compose or local-mode
equivalent. Full design: `docs/drafts/tenants.md`.

**When to revisit**: when a deployment must serve mutually-distrusting
parties. The earlier metadata-level scheme filtered one shared database by
an active tenant and never provided that, which is why it was removed.

## Password Reset by Email

Admins mint reset links and hand them over out of band
(`docs/designs/auth.md` — Password Reset). Self-service "email me a link"
needs an SMTP sender, its configuration, and a public request endpoint that
always answers `204` so it never discloses whether an account exists.
`users.email` is already populated (API / CLI, or the OIDC `email` claim).
An earlier `aaiclick/auth/mail.py` (`smtplib` on a worker thread via
`asyncio.to_thread`) was removed as unused; it is in git history.

**When to revisit**: when deployments have a reachable SMTP server, or when
operators mint links often enough for it to hurt.

## OIDC / SSO Login

Authorization-code login against any OpenID Connect provider was built and
removed: with no identity provider connected, nobody could run it. Recover
from git history `aaiclick/auth/oidc.py` (discovery, PKCE, code exchange,
`id_token` validation against the JWKS), `aaiclick/internal_api/oidc.py`
(config / start / callback), and the SPA half in `src/lib/auth.ts` and
`src/views/Login.tsx`.

Restoring it also needs `users.oidc_subject` (`"<issuer>|<sub>"`, unique) to
link a local user to an external identity, an `oidc_states` table with one
row per in-flight login, and `pyjwt[crypto]` again for RS256 `id_token`
signatures. The subject must stay issuer-qualified: `sub` is unique only
within an issuer, so a bare value lets two providers collide.

**When to revisit**: when a deployment has an IdP to point at.

## Viewer Follow-ups

Shipped design: `viewer.md`.

- **SQL over several objects at once** (joins, unions): `query_object` reads
  one object with a `where` filter. A `query_sql` verb would take `scope`, a
  SQL text, and a map of the object names it uses
  (`{"o": "orders", "c": "customers"}`); the user writes `SELECT … FROM o
  JOIN c ON …` and the server prepends one CTE per entry (`WITH o AS (SELECT *
  FROM p_orders), c AS (…)`), so the SQL never names a table and the scope
  rules stay server-side. Verified on chdb: such CTEs resolve inside the
  pagination wrapper and alongside the user's own `WITH`. Deferred until
  single-object queries prove insufficient.
- **Agent push to the browser**: QueryView's remote channel (an agent pushes
  a query or dashboard into a live tab) has no aaiclick equivalent. `GET
  /events` carries one payload-less `changed` kind that the SPA answers by
  invalidating its cache (`src/api/events.ts`), so a push needs a kind with
  a payload and a handler that switches the mode.
- **Git sync and YAML export** for dashboards, as QueryView
  has (its workspaces have no counterpart — one installation is one
  workspace).
- **`options_sql` params**: a dashboard param may name a query whose first
  column feeds its selector; aaiclick has no free-SQL endpoint, so
  `save_dashboard` rejects one.
- **Filtering and cell views in `@data`**: the object view orders and pages;
  a `where` box, the kernel's cell-view YAML and CSV download have no UI
  (`data query --where … --csv` covers the CLI).
- **Dashboard params outside the browser**: `run_dashboard` (MCP, CLI) runs
  panels as stored, so a `where` holding a `{param}` placeholder fails there.
- **Dashboard authoring in the UI**: `@dashboard` picks and runs; HTML and
  panel queries are written through MCP, REST, or `view dashboards save`.

## Job Graph — Collapsible Groups

Group containers are fixed frames (`src/components/graph/GroupNode.tsx`), so
a wide group — e.g. `map()`'s per-partition children — always takes its full
size. Collapsing is client-only; the graph response already carries every
group and its members.

- A collapsed group is one node (name, rolled-up status, member count). Edges
  touching a member re-point to it and are deduplicated; intra-group edges
  are hidden. Collapsing a parent hides nested groups.
- A chevron in the header toggles it; groups start expanded, state kept in
  the URL or `localStorage`.
- Collapsing re-runs dagre, so positions change — unlike the build-edge
  toggle, the point is to reclaim space.

**When to revisit**: when a real job's group makes the graph unreadable.

## Job Graph — Wide Fan-out Layout

dagre puts every sibling of a fan-out in one rank, so `wide_pipeline` (1 task
→ 99, seeded by `test_e2e/web/seed.py`) renders as a single column that
`fitView` shrinks until nodes are unreadable.

- Wrap a rank past N nodes into several columns, or
- fold siblings with the same entrypoint and status into one "×N" node that
  expands on click (same mechanics as collapsible groups above), and
- set a minimum zoom on initial fit so nodes stay legible and the canvas pans.

**When to revisit**: a real job fans out past ~20 tasks.

## Lineage — Tier 2 Full Replay

An MCP client can already do Tier 2 by hand: `run_job` with
`preservation_mode="FULL"`, then the lineage tools against the new run's
tables. Missing is a one-call `request_full_replay` that re-runs the original
job with its recorded kwargs, returns the new job's handle, and reports input
drift (row counts, old vs new). Design: `docs/designs/lineage.md` (Tier 2).

**When to revisit**: when Tier 1's static reasoning demonstrably fails on
real questions — a bug whose explanation lives only in an intermediate table
that cleanup has already dropped.

## Lineage in the UI

Lineage is reachable only over MCP (`aaiclick/server/mcp.py`); the UI has no
view of it. `@data` shows an object's rows but not where they came from.

- A lineage tab on the object (`@data … <object>`): the operation subgraph
  (`aaiclick/oplog/lineage.py`) rendered like the job graph (same dagre +
  React Flow stack), one node per table with its operation and rendered
  `sql_template`, dropped tables greyed out.
- Click a live node to open it in `@data`.
- Needs a REST route over the subgraph under the same read scope the MCP
  tools use; the SPA does not speak MCP.

**When to revisit**: when users ask "how did this value get here?" in the UI
rather than through an agent.

## ClickHouse ADBC Driver and Arrow-Native Results

**Revisit: 2027-02.** Assessed 2026-10 against ClickHouse's
[ADBC driver](https://clickhouse.com/blog/introducing-the-clickhouse-adbc-driver)
(Rust, v0.1.0). Not adopted:

- Sync-only API; aaiclick's ClickHouse client is async end to end.
- Speaks ClickHouse HTTP, same as clickhouse-connect — no transport gain.
- Ships as a native binary via `dbc install clickhouse`, not a wheel, so it
  cannot ride `pip install aaiclick[distributed]` or the runner images.
- No `JSON` type, no schema discovery, Arrow↔ClickHouse type gaps.
- Local mode (chdb) already returns Arrow natively.

Adopt when the driver gains async, publishes to PyPI, and returns `JSON`.

Independent of ADBC, an Arrow-native read path is worth adding: the chdb
client already fetches an Arrow table and transposes it into row tuples, and
clickhouse-connect has `query_arrow`. An `Object.to_arrow()` / `to_polars()`
would skip the Python-list round trip on both backends.
