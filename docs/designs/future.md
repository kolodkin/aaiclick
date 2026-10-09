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

# Docker Runner Resources

Map a job's `resources` (Kubernetes requests/limits JSON) onto `docker run
--cpus` / `--memory` so the docker runner honours the same per-job field the
kubernetes runner applies to its Pods. Today the docker runner ignores it.

---

# Default Build Image — Dependency Manifests

The default Dockerfile for a git build without one (`docs/user_guide/container_images.md`
"Runner base") copies the repo onto the aaiclick base image and installs
nothing, so a job can only use what `aaiclick[all]` ships. Teach the build to
honour dependency manifests found in the checkout: `requirements.txt`,
`uv.lock` / `pyproject.toml` (`uv sync` or `pip install .`), and `pom.xml` for
`jvm` tasks. Each adds a `RUN` layer to the default Dockerfile only when the
file is present. Until then, a repo with dependencies checks in its own
Dockerfile.

---

# Deferred

Items deferred until preconditions are met.

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

## Foreign-Key Enforcement in the Local Test Backend

SQLite defaults `PRAGMA foreign_keys` to `0`, and SQLAlchemy does not turn it
on, so every `REFERENCES` clause in the local test schema is declared and never
checked. Postgres enforces them always. The local half of the CI matrix is
therefore structurally unable to catch a referential-integrity bug, and half of
the 16 jobs are local — the scope-ladder branch shipped a test helper that
inserted a child row for a parent with no row, which 2595 local tests passed
straight over and only `Internal API dist` rejected.

The fix is a `connect` event listener on the test engine issuing
`PRAGMA foreign_keys=ON`. The cost is unknown until tried: turning enforcement
on may surface existing violations in suites that have been quietly relying on
the laxity, and each one wants fixing rather than suppressing.

**When to revisit**: next time a foreign-key bug reaches `dist` after passing
`local`, or alongside any work already touching the shared test fixtures.

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

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.

## Reconsider the `>>` Dependency Syntax

`a >> b` records a dependency as a side effect of `Task.__rshift__`
(`aaiclick/orchestration/models.py`), so linters read it as a discarded value.
Pyright's `reportUnusedExpression` is purely syntactic and cannot be scoped to
`Task`/`Group`, so `pyrightconfig.json` turns it off globally, which also hides
real unused expressions elsewhere. Open a design discussion on the dependency API
before growing it further:

- Keep `>>` / `<<` (Airflow-style) and accept the global ignore
- Add or prefer an explicit call (`b.depends_on(a)`, `chain(a, b, c)`), which
  linters accept and which reads as an action
- Whether a `@job` body should need explicit edges at all, given that passing a
  task as a kwarg already wires the dependency

Decide on one taught form, then restore `reportUnusedExpression` if `>>` goes.

## Coverage Reporting

Coverage is off by default: its CI log table went unread and it slowed every
unit job. `pytest-cov` stays installed for ad hoc `--cov` runs (e.g. the
`python-testing-style` zero-lines-lost check).

Bring it back as one nightly job that publishes the report where it is read
(HTML artifact or a coverage service with PR diffs), outside the per-PR jobs.
