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

Implemented in `java/aaiclick-worker`:

| Concern                   | Implementation (`src/main/java/io/aaiclick/worker/`)          |
|---------------------------|---------------------------------------------------------------|
| Config + local-URL refusal| `config/WorkerConfig.java` — see `fromEnv()`                  |
| CH HTTP + snowflake IDs   | `ch/ChClient.java` — see `nextSnowflakeId()`, `insertJsonEachRow()` |
| Registration / heartbeat  | `db/WorkerRepo.java` — see `heartbeat()` (STOPPING-aware)     |
| Shared SQL contract       | `aaiclick/orchestration/execution/sql/` — `claim_next_task.sql`, `job_rollup.sql`, `complete_job.sql`; loaded by `sql_loader.load_sql()` (Python) and `db/NamedParamSql.java` (Java, named→positional; both sides skip comments) |
| Claim + capability binds  | `db/TaskRepo.java` — see `claimNext()`                        |
| Job rollup (worker recipe)| `roll_up_job()` in `background/handler.py` and `TaskRepo.tryCompleteJob()` — the identical rollup-only recipe from the shared files |
| Run lifecycle + epoch fencing | `db/TaskRepo.java` — see `startRun()`, `complete()`, `failPendingCleanup()`, `tryCompleteJob()` |
| Shell execution           | `exec/ShellRunner.java` — see `run()` (env overlay, timeout, abort poll) |
| Log streaming             | `logs/LogFlusher.java` — see `flush()` (seq offsets)          |
| Main loop / shutdown      | `Worker.java` — see `runLoop()`                               |

Key semantics: both workers execute the shared claim SQL; per-worker
capabilities are bound values — Java passes `entry_types=['shell']`,
`allow_image_tasks=false`; Python passes the full set. The no-image predicate
accepts both SQL `NULL` and JSON `null` (SQLAlchemy writes the latter).
Failure only sets `PENDING_CLEANUP`, leaving retries and ref cleanup to the
Python `BackgroundWorker`; shell tasks go to whichever worker claims first.
SQLite keeps its Python-only claim path (`sqlite_handler.py`).

**One worker recipe, both languages**: on task success every worker runs the
same rollup-only completion check (`roll_up_job` / `tryCompleteJob`, from the
shared `job_rollup.sql` + `complete_job.sql`). The `UPSTREAM_FAILED` cascade
belongs exclusively to failure-transition owners — `try_complete_job` on the
BackgroundWorker's PENDING_CLEANUP path, and `cancel_job` — so a stranded
downstream task is always swept at failure time, never by a worker's success
path.

Cross-language e2e (and drift guard for the Java test schema fixture):
`aaiclick/orchestration/execution/test_java_worker_e2e.py`. CI: the
`java-worker` job in `.github/workflows/test.yaml`.

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

The `java-worker` job in `.github/workflows/test.yaml` runs Maven unit tests
and then the cross-language e2e against a Python-migrated schema. Java test
backends resolve from `AAICLICK_TEST_PG_JDBC` / `AAICLICK_TEST_CH_HTTP` env
vars or fall back to Testcontainers
(`java/aaiclick-worker/src/test/java/io/aaiclick/worker/testsupport/Backends.java`),
so the suite also runs in Docker-less sandboxes against external servers.

# Release

Not yet wired into `publish.yaml` — tracked in `docs/designs/future.md`
("Java Worker Release Flow").

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
