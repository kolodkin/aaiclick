Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Group Kwarg Creates No Dependency Edge

`_collect_upstreams()` in `decorators.py` ignores `Group`, while
`_serialize_value()` emits a `group_results_ref` for it. A group passed as a
kwarg creates no dependency edge: the consumer can be claimed before the
group finishes, and the group-results read (COMPLETED members only) silently
returns a partial or empty list. The PIN fan-out already resolves consumers
through group edges, so the edge is all that is missing.

Fix: collect `Group` values alongside `Task` values and wire `group >> task`.
Land the "`map()` never sets `expander.group_id`" backlog item with it —
until the expander is a member, the edge is vacuously satisfied on
multi-worker deployments.

---

# Expander Children Have No Pin Path

The PIN fan-out resolves consumers through group edges, but still cannot
reach the children an expander creates at runtime.

`_expand_map()` (`aaiclick/orchestration/operators.py`) creates `out`, builds
one `_map_part` child per partition, and returns them as `tasks_list`. Two
things keep the children out of any fan-out:

- Their rows and group membership are committed by
  `register_returned_tasks()` after the expander has already pinned, so at
  pin time no `dependencies` or `tasks` row names them.
- `out` is not the expander's return value, so `execute_task` never pins it
  at all; the source table's only protection is the expander's own run ref,
  which its `task_scope` exit deletes.

Between the expander exiting and the first child claiming, both tables meet
the drop sweep's "no pins, no run refs" condition. `_expand_reduce()` has the
same shape per layer.

## Design

Decide once, for `map()` and `reduce()` together:

- Pin after the children are committed — move the pin in `execute_task` to
  after `register_returned_tasks()`, and let the expander name the tables it
  pins for them (source and `out`), since the children are its consumers in
  fact but not by any edge.
- Or return `out` as `TaskResult.data` and treat the expander's own group
  members as consumers in the fan-out.

Each child releases its pin as it deserializes the table, as consumers do
today. Covers the `map()` High in the code review backlog.

---

# Cancellation Cleanup — CANCELLING State

Cancelling a task releases none of its lifecycle refs. Pin refs that upstream
producers hold for it as a consumer are never unpinned (the task never
deserializes its inputs), and a container or pod killed by `terminate` may
never run `task_scope`'s decref block, so its run refs linger too. Both keep
the job's tables alive until the job-TTL sweep. Failures already have a
contract for this — PENDING_CLEANUP, where the background worker drops the
attempt's run refs and pin refs before the task turns terminal — and
cancellation should get the same shape. No backwards compatibility is
required; renaming PENDING_CLEANUP to PENDING_FAILURE_CLEANUP alongside is
allowed.

1. `cancel_job` moves every non-terminal task to CANCELLING (the job goes to
   CANCELLED at once, as today).
2. The worker's abort check treats CANCELLING like CANCELLED and kills the
   run. When the killed run reports back, the worker stamps the last
   `run_statuses` entry CANCELLED; `update_task_status` and
   `_set_pending_cleanup` refuse to overwrite CANCELLING.
3. The background worker's cleanup pass picks up CANCELLING tasks whose last
   run has ended, or that never ran, deletes their run refs and pin refs, and
   moves them to CANCELLED — the PENDING_CLEANUP pass minus the retry branch.

The run-status stamp in step 2 is the signal that a container or pod kill has
landed, so no table is dropped under a process still being stopped.

---

# Code Review Backlog

`docs/designs/code_review_2026_09.md` — findings from the whole-project
review at commit `db56ac4`, grouped by severity with a suggested fix order.
Remove each item from that file as it lands; delete the file when empty.

---

# Deferred

Items deferred until preconditions are met.

## Task Logs — Per-Attempt History in the Log Panel

`get_task_logs` (`aaiclick/internal_api/tasks.py`) reads `task.run_ids[-1]`, so
the panel shows only the latest attempt. Earlier attempts are already in
ClickHouse — `task_logs` tags each line with `run_id`, and `Task.run_ids` /
`Task.run_statuses` hold the ordered attempts and how each ended — so a retried
task's failed runs are retained but unreachable. That is exactly the output you
want after a flaky task finally passes.

Shape, following Airflow's per-try log selector:

- `GET /tasks/{id}/logs` takes an optional 1-based `attempt`, resolved through
  `run_ids`; defaults to the last.
- `TaskLogsView` carries the attempts and their statuses, so the selector costs
  no second request.
- `LogViewer` shows the selector only when `run_ids` has more than one entry.
  Polling stays on the latest attempt; older ones are immutable.

!!! note "Pending input"
    Airflow screenshots to follow as the reference for layout and wording — do
    not settle the UI details before then.

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
  query or dashboard into a live tab) has no aaiclick equivalent yet; it
  needs the SSE endpoint planned above.
- **Git sync and YAML export** for saved queries and dashboards, as QueryView
  has (QueryView's workspaces have no aaiclick counterpart — one installation
  is one workspace).
- **`options_sql` params**: the kernel's `params:` block accepts a query
  whose first column feeds a dropdown; aaiclick has no free-SQL endpoint, so
  `QueryPanel` renders static `options` only.
- **Dashboard authoring in the UI**: `@dashboard` picks and runs; HTML and
  panel queries are written through MCP, REST, or `view dashboards save`.

## Lineage — Tier 2 Full Replay

The Tier 1 tools are built (`aaiclick/ai/agents/lineage_tools.py` —
`LineageToolbox`); `request_full_replay` and the `--deep` flag that
pre-commits to it are not. Tier 2 re-runs the original job through
`run_job()` with `preservation_mode=FULL`, so every intermediate table is
alive for the agent to query. Full design: `docs/designs/lineage.md` (Tier 2).

**When to revisit**: when Tier 1's static reasoning demonstrably fails on
real questions — a bug whose explanation lives only in an intermediate table
that cleanup has already dropped.

## Changelog

`docs/changelog.md` — version history in Keep a Changelog format. Introduce with v1.0.0 release.
