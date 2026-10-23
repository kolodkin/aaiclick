Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Blob Storage Support

Read-only Objects over files in S3 / GCS / Azure Blob through ClickHouse's
object-store table engines, plus `export` to a bucket; the MergeTree default
is unchanged. External parquet is queried in place without ingestion, and a
job's exported result is opened by the next job on any cluster. Full design:
`docs/designs/blob_storage_support.md`.

---

# Coverage Reporting

Coverage is off by default: its CI log table went unread and it slowed every
unit job. `pytest-cov` stays installed for ad hoc `--cov` runs (e.g. the
`python-testing-style` zero-lines-lost check).

Bring it back as one nightly job that publishes the report where it is read
(HTML artifact or a coverage service with PR diffs), outside the per-PR jobs.

---

# Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.

---

# Deferred

## Build Quotas

Shell/jvm children can declare image builds, so one job can queue many builds
on the shared buildkitd. Add a per-job cap on injected build tasks and
per-build buildkitd resource limits.

Items deferred until preconditions are met.

## Objects in Non-Python Tasks

Unsupported. Shell and jvm tasks speak the Task Interface's file data plane
(`task_interface.md`: plain values and resolved upstream refs); `object` /
`view` refs are rejected at commit. An
Object needs a per-language task runner that owns the ClickHouse dependency
and the lifecycle (refcounts, pins, ephemeral-table ownership, oplog).

## Protect Upstream Outputs on Re-run

`clear_task` re-runs a task against upstream outputs that may be gone: a
consumer releases its pins once it succeeds, so the cleanup pass can drop
those tables. The re-run then fails with `LookupError`.

Re-pinning only on clear is not enough. Deleting an upstream output and
changing it (re-running the producer) are the same hazard, so protect both
or neither.

**When to revisit**: when re-running a single task of a finished job,
without re-running its upstream, becomes a supported workflow.

## Quote Column Names in Generated SQL

`join` quotes its identifiers; other Object operations still splice raw
column names into SQL, so a column named `from` or `a b` breaks them. Known
sites: the default insert list in `emit_result`, the copy path in
`ingest.py` and the URL ingest in `url.py`. Fix it once at the shared
column-list builders rather than per operation, then audit the remaining
SELECT projections.

**When to revisit**: before accepting user-supplied column names from
external sources (CSV headers, parquet) as a supported path.

## Merge `claimed_at` and `started_at`

With no CLAIMED phase, both claim paths write the same `:now` to
`tasks.claimed_at` and `tasks.started_at`, and every reset clears both.
Drop one (likely `claimed_at`) and point its readers at the other.

**When to revisit**: the next migration on the `tasks` table.

## Tenants — Kubernetes Control Plane

Multi-tenancy as a fleet layer rather than a filtered column: a control
plane that provisions one full aaiclick installation per tenant, each in
its own Kubernetes namespace, with central identity and direct routing to
each tenant's own ingress.

An installation carries no tenant state — see `docs/designs/auth.md` for
the RBAC it does carry. Tenancy is a Kubernetes-only feature; there is no
Compose or local-mode equivalent.

Full design: `docs/designs/tenants_draft.md`.

**When to revisit**: when a deployment must serve mutually-distrusting
parties. The earlier metadata-level scheme filtered one shared database by
an active tenant and never provided that, which is why it was removed.

## Password Reset by Email

An admin mints reset links today and hands them over out of band
(`docs/designs/auth.md` — Password Reset). A self-service "email me a link"
flow needs an SMTP sender plus a public request endpoint that always answers
`204`, so it never discloses whether an account exists. `users.email` is
already populated — set through the API / CLI, or from the OIDC `email` claim
— so the missing pieces are the sender, its configuration, and the endpoint.
An earlier `aaiclick/auth/mail.py` (`smtplib` on a worker thread via
`asyncio.to_thread`) was removed as unused; it is recoverable from git history.

**When to revisit**: when deployments have a reachable SMTP server, or when
operators mint links often enough for it to hurt.

## OIDC / SSO Login

Authorization-code login against any OpenID Connect provider was built and
then removed: no identity provider is connected, so it was code nobody could
run. Recoverable from git history — `aaiclick/auth/oidc.py` (discovery, PKCE,
code exchange, `id_token` validation against the JWKS) and
`aaiclick/internal_api/oidc.py` (config / start / callback), with the SPA half
in `src/lib/auth.ts` and `src/views/Login.tsx`.

Restoring it needs both back, plus `users.oidc_subject` (`"<issuer>|<sub>"`,
unique) to link a local user to an external identity, an `oidc_states` table
holding one row per in-flight login, and `pyjwt[crypto]` again for RS256
`id_token` signatures. The subject must stay issuer-qualified: `sub` is unique
only within an issuer, so a bare value lets two providers collide.

**When to revisit**: when a deployment has an IdP to point at.

## Viewer Follow-ups

See `viewer.md` for the shipped design.

- **SQL over several objects at once** (joins, unions): today `query_object`
  reads one object with a `where` filter. A `query_sql` verb would take
  `scope`, a SQL text, and a map of the object names it uses
  (`{"o": "orders", "c": "customers"}`); the user writes `SELECT … FROM o
  JOIN c ON …` and the server prepends one CTE per entry (`WITH o AS (SELECT *
  FROM p_orders), c AS (…)`), so the SQL still never names a table and the
  scope rules stay server-side. Verified on chdb that such CTEs
  resolve inside the pagination wrapper and alongside the user's own `WITH`.
  Deferred until single-object queries prove insufficient.
- **Agent push to the browser**: QueryView's remote channel (an agent pushes a
  query or dashboard into a live tab) has no aaiclick equivalent. `GET
  /events` carries one payload-less `changed` kind that the SPA answers by
  invalidating its cache (`src/api/events.ts`), so a push needs a kind with
  a payload and a handler that switches the mode.
- **Git sync and YAML export** for saved queries and dashboards, as QueryView
  has (QueryView's workspaces have no aaiclick counterpart — one installation
  is one workspace).
- **`options_sql` params**: the kernel's `params:` block accepts a query
  whose first column feeds a dropdown; aaiclick has no free-SQL endpoint, so
  `QueryPanel` renders static `options` only.
- **Dashboard authoring in the UI**: `@dashboard` picks and runs; HTML and
  panel queries are written through MCP, REST, or `view dashboards save`.

## Job Graph — Collapsible Groups

Group containers are fixed frames (`GroupNode.tsx`), so a wide group — e.g.
`map()`'s per-partition children — always takes its full size. Collapsing is
client-only; the graph response already carries every group and its members.

- A collapsed group is one node (name, rolled-up status, member count). Edges
  touching a member re-point to it and are deduplicated; intra-group edges
  are hidden. Collapsing a parent hides nested groups.
- A chevron in the header toggles it; groups start expanded, state kept in
  the URL or `localStorage`.
- Collapsing re-runs dagre, so positions change — unlike the build-edge
  toggle, the point is to reclaim space.

**When to revisit**: when a real job's group makes the graph unreadable.

## Lineage — Tier 2 Full Replay

An MCP client can already do Tier 2 by hand: `run_job` with
`preservation_mode="FULL"`, then the lineage tools against the new run's
tables. Missing is a one-call `request_full_replay` that re-runs the original
job with its recorded kwargs, returns the new job's handle, and reports input
drift (row counts, old vs new). Full design: `docs/designs/lineage.md` (Tier 2).

**When to revisit**: when Tier 1's static reasoning demonstrably fails on
real questions — a bug whose explanation lives only in an intermediate table
that cleanup has already dropped.

## ClickHouse ADBC Driver and Arrow-Native Results

**Revisit: 2027-02.** Assessed 2026-10 against ClickHouse's
[ADBC driver](https://clickhouse.com/blog/introducing-the-clickhouse-adbc-driver)
(Rust, v0.1.0). Not adopted:

- Sync-only API; aaiclick's `ChClient` is async end to end.
- Speaks ClickHouse HTTP, same as clickhouse-connect — no transport gain.
- Ships as a native binary via `dbc install clickhouse`, not a wheel, so it
  cannot ride `pip install aaiclick[distributed]` or the runner images.
- No `JSON` type, no schema discovery, Arrow↔ClickHouse type gaps.
- Local mode (chdb) already returns Arrow natively.

Adopt when the driver gains async, publishes to PyPI, and returns `JSON`.

Independent of ADBC, an Arrow-native read path is worth adding: `ChdbClient.query()`
already fetches an `ArrowTable` and transposes it into row tuples, and
clickhouse-connect has `query_arrow`. An `Object.to_arrow()` / `to_polars()`
would skip the Python-list round trip on both backends.
