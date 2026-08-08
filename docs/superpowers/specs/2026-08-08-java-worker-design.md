Java Worker for Distributed Mode
---

Design spec for a Java execution worker that runs only against the distributed
backends (PostgreSQL + ClickHouse server), with no Object/View data API
support. Phased: shell tasks first, a native `jvm` entry type second.

# Motivation

Shell tasks already run any argv (`java -jar app.jar`) under the Python
worker, so a Java worker is justified only by at least one of:

- Worker hosts with **no Python runtime** — a JVM-only fleet, or embedding the
  worker loop inside an existing Java service.
- A **native Java task API** (phase 2): tasks as Java classes with JSON kwargs
  and results, declared as an ordinary Maven dependency.

# Why It Is Feasible

Everything a distributed worker does is language-neutral SQL against
PostgreSQL:

- **Task claiming** — the writable CTE with `FOR UPDATE SKIP LOCKED` ports to
  JDBC verbatim (`aaiclick/orchestration/execution/pg_handler.py` — see
  `PgDbHandler.claim_next_task()`).
- **Registration, heartbeats, status updates** — plain rows in
  `execution_workers` and `tasks`, fenced by `run_epoch`.
- **Failure handling is centralized outside the worker.** A failing worker
  only sets `PENDING_CLEANUP`; the Python `BackgroundWorker` owns retry
  scheduling, ref cleanup, and job-failure transitions
  (`aaiclick/orchestration/background/background_worker.py` — see
  `_process_pending_cleanup()`). The Java worker never implements retries.

The two Python-coupled parts — `module` entrypoints and the Object/View
lifecycle — are exactly what this design excludes.

# Phase 1 — Shell-Only Worker

## Java side

A single deployable jar (PostgreSQL JDBC plus one small HTTP client):

1. **Config** — parse `AAICLICK_SQL_URL` / `AAICLICK_CH_URL` into JDBC and
   ClickHouse HTTP endpoints. Refuse to start on `sqlite`/`chdb` URLs —
   distributed-only by construction.
2. **Registration and heartbeat** — insert an `execution_workers` row,
   heartbeat every 30s, SIGTERM → STOPPING → finish current task → STOPPED.
   IDs come from `SELECT generateSnowflakeID()` over the ClickHouse HTTP
   interface — no local snowflake implementation.
3. **Claim loop** — the ported claim CTE plus two capability predicates:
   `entry_type = 'shell' AND image_source IS NULL`. The second predicate keeps
   docker/kubernetes shell tasks on Python workers, which own the
   image-resolution machinery (`aaiclick/orchestration/execution/dispatch.py`
   — see `build_shell_spec()`).
4. **Execution** — `ProcessBuilder` with the worker process env plus
   `command_env` overlaid (subprocess-runner semantics), exit 0 = success,
   `result_ref = NULL`. Enforces `AAICLICK_TASK_TIMEOUT`; polls task status
   ~1s for cancellation and `clear_task`, killing the child; every write is
   fenced on `run_epoch`.
5. **Completion** — success → `COMPLETED` plus a `try_complete_job`-equivalent
   SQL check (all tasks terminal → job COMPLETED); failure →
   `PENDING_CLEANUP`, then the background worker takes over.
6. **Logs** — pipe child stdout/stderr into ClickHouse `task_logs` via HTTP
   `INSERT ... FORMAT JSONEachRow` every ~2s, mirroring `_SinkFlusher`
   semantics (per-line `created_at`, `stream`, `seq`) so Java-run tasks are
   tailable in the UI like every other task. This is the worker's only
   ClickHouse touchpoint.

## Python side

Nearly nothing: the capability filter lives in the Java worker's own claim
SQL. Python workers keep claiming shell tasks too; claiming is atomic, so
whichever worker claims first wins.

## Object-lifecycle interaction

Shell tasks never deserialize Objects, so the Java worker does no
incref/unpin — identical semantics to Python-run shell tasks. (Pre-existing,
independent of this design: a shell task downstream of an Object-producing
task receives a `pin_ref` that nothing unpins on the success path.)

# Phase 2 — `jvm` Entry Type

- Add `"jvm"` to the `EntryType` Literal — a code change only, no migration
  (plain String column, no CHECK constraints per project convention).
  `tasks.entrypoint` holds a Java class name; `kwargs` JSON binds to method
  parameters via Jackson; the return value is JSON (plain values only).
- **Submission validation** — `jvm` tasks must not receive Object/View refs as
  kwargs, and their results are never auto-converted to Objects. Enforced at
  commit points, alongside `validate_image_sources()`
  (`aaiclick/orchestration/image_injection.py`). This is the line that keeps
  "no ClickHouse object support" honest.
- **Java task SDK** — an `@AaiTask` annotation plus registry; each task runs
  in a spawned child JVM, mirroring the mp worker's isolation, timeout, and
  kill semantics.
- **Claim filters** — Python workers add `entry_type != 'jvm'` (both
  `pg_handler` and `sqlite_handler` for parity); the Java worker widens to
  `entry_type IN ('shell', 'jvm')`. CLI, `run_job()`, and `RunJobRequest`
  grow the `jvm` choice.

# Folder Structure

Top-level `java/` directory — the repo is already a polyglot monorepo (Python
in `aaiclick/`, TypeScript in `src/`). A separate repo would cost
cross-language integration tests in the existing CI matrix, and phase 2 needs
coordinated changes on both sides.

```
java/
├── pom.xml                        # parent: shared deps, plugin config, version
├── aaiclick-worker/               # phase 1: the runnable worker
│   └── src/
│       ├── main/java/io/aaiclick/worker/
│       │   ├── config/            # AAICLICK_SQL_URL / AAICLICK_CH_URL parsing
│       │   ├── claim/             # ported claim CTE, capability filter
│       │   ├── exec/              # ProcessBuilder runner, timeout, cancellation poll
│       │   ├── logs/              # CH HTTP JSONEachRow flusher
│       │   └── Worker.java        # main: register, heartbeat, loop, shutdown
│       └── test/java/...          # unit tests (Testcontainers for PG + CH)
└── aaiclick-task-api/             # phase 2: the SDK user projects depend on
    └── src/main/java/io/aaiclick/task/   # @AaiTask, JSON kwargs binding
```

The module split is the load-bearing decision: `aaiclick-task-api` is what
user projects declare as a dependency (small, stable, worth publishing);
`aaiclick-worker` is an application — deployed, not depended on. Phase 1
builds only the worker module; the parent POM makes the API module additive.

# Testing & CI

- New job in `test.yaml`: Maven build + unit tests.
- The worker joins the existing distributed-backend integration matrix:
  submit a shell job via the Python CLI, run the Java worker against the same
  PostgreSQL + ClickHouse, assert completion, logs, cancellation, and
  dead-worker reaping (kill -9 the worker; the background worker marks the
  task PENDING_CLEANUP).

# Release

Publishing goes through the Central Publisher Portal (OSSRH is sunset).
Namespace `io.github.kolodkin` is auto-verified against the GitHub account; a
custom groupId needs DNS-verified domain ownership. Artifacts need GPG
signatures, sources/javadoc jars, and full POM metadata — all scriptable in
GitHub Actions with `central-publishing-maven-plugin` and two secrets (portal
token, GPG key).

Match the artifact to the phase:

| Phase   | Artifact                                   | Channel                                  |
|---------|--------------------------------------------|------------------------------------------|
| Phase 1 | `aaiclick-worker` shaded fat jar           | GitHub Release asset + docker image      |
| Phase 2 | `aaiclick-task-api` (and optionally worker)| Maven Central                            |

Both are driven by the **same `vX.Y.Z` tag** as the Python package, via a new
`java` job in `publish.yaml`. Lockstep versioning is meaningful: the Java
worker's compatibility contract is the PostgreSQL schema and task semantics of
a specific aaiclick release, so `aaiclick-worker 0.9.0` ↔ `aaiclick==0.9.0`
states exactly what was tested together.

!!! tip "De-risk Central early"
    The portal namespace verification and signing setup are the only steps
    with bureaucratic latency. Do a one-time `0.0.x` dry-run publish of an
    empty artifact during phase 1; the rest is CI.

# Decisions Log

| Decision      | Choice                                        | Alternative rejected                          |
|---------------|-----------------------------------------------|-----------------------------------------------|
| Scope         | Both phases, shell-first                      | Shell-only forever; jvm-first                 |
| Routing       | Worker-side capability filter in claim SQL    | Explicit queue column (schema + API surface)  |
| Logging       | ClickHouse HTTP insert into `task_logs`       | No CH at all (UI blind spots for Java tasks)  |
| Location      | `java/` in the monorepo                       | Separate repo (loses shared CI matrix)        |
| Versioning    | Lockstep with Python `vX.Y.Z` tags            | Independent Java versioning                   |
