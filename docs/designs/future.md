Future Plans
---

Planned work across aaiclick, ordered by priority.

---

# Code Review Backlog

`docs/designs/code_review_2026_09.md` — findings from the whole-project
review at commit `db56ac4`, grouped by severity with a suggested fix order.
Remove each item from that file as it lands; delete the file when empty.

---

# Commit a Task's Completion and Its Job Rollup Together

On success the worker (`_handle_task_result` in
`aaiclick/orchestration/execution/execution_worker.py`) commits the task's
COMPLETED status, then runs `roll_up_job` in a second transaction:

- **Crash window.** A worker dying between the commits leaves the job RUNNING
  for good: only the success path completes a successful job, and the
  dead-worker sweep only recovers RUNNING tasks.
- **Two signals per completion.** Every open live view refetches twice. The
  web e2e no-polling tests wait out the second one with
  `_settle_after_job_terminal` (`test_e2e/web/test_smoke.py`); delete it with
  this change.

Add `complete_task_and_roll_up(task_id, result, expected_epoch)` to
`claiming.py`: one transaction that applies the epoch and cancelling guards,
writes COMPLETED, rolls up, and runs `complete_job.sql` when nothing is left.
`update_task_status` stays for RUNNING and the in-process test runner.

Lock the job row, then the task row:

- Without the job lock, two siblings finishing together on Postgres each read
  the other as RUNNING (write skew), and nobody completes the job.
- Job then task matches `cancel_job`, so the two cannot deadlock. The claim
  CTE locks task then job, but only for a PENDING task, which a completion
  never touches.
- Cost: completions within one job serialize on the job row for the rollup
  query — latency at high fan-out, no extra work.

Tests: one signal per completion; a fenced write does not roll up; the last
task completes the job in the same commit; two siblings completing
concurrently on Postgres leave the job COMPLETED.

---

# Draw a `map()` / `reduce()` Call as One Graph Frame

In the job graph the expander and `_finalize` sit outside the frame of the
parts (`map` group, or `layer_N` groups for `reduce()`). The expander cannot
be a member of a group it creates at runtime, and `_finalize` cannot be a
member of the group it waits on (`group >> finalize` would wait on itself).

Wrap the whole call in an outer group created at definition time:

- `map()` / `reduce()` create an outer `map` / `reduce` group and add the
  expander to it, passing its id to the expander.
- The expander creates the parts group(s) with `parent_group_id` set to the
  outer group, and adds `_finalize` as a direct member of the outer group.

Presentation only: scheduling resolves a group edge to its direct members
(`DEPENDENCY_WHERE`, `successor_task_ids`), nothing has an edge to or from the
outer group, and consumers already depend on `_finalize` through the hold. The
graph view and UI already nest frames (`group_member_tasks`, `nestByGroup`).

One prerequisite: `_collect_from_registry` (`orch_context.py`) walks
dependency edges only, so a definition-time group whose only link is a
member's `group_id` is never committed. Visit a task's group, and a group's
parent, before the node itself. Cover it with a job test on the committed
group tree and an assertion on the nested frames in
`test_e2e/web/test_operators_graph.py`.

---

# One Runner-Mode Validator for Container-Only Fields

`run_job` (`aaiclick/orchestration/registered_jobs.py`) rejects `image`,
`git_*`, and `dockerfile` on subprocess jobs, but the same silent drop
survives twice: `register_job` / `upsert_registered_job` store those fields
on subprocess registrations that never read them, and `run_job` ignores
`namespace` / `service_account` / `image_pull_secret` off kubernetes, as
documented. Add one `validate_runner_fields(runner_mode, ...)` next to
`validate_image_exclusivity` in `runner_config.py` for both. Preconditions:

- Move `RUNNER_*` and `RunnerMode` from `models.py` into `runner_config.py`;
  `models.py` already imports it, so the validator cannot import back.
- Decide to reject the kubernetes overrides instead of documenting them as
  ignored.

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
