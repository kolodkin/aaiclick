aaiclick Orchestration Backend Specification
---

Internal design reference, not published on the docs site. User guide: [docs/user_guide/orchestration.md](../user_guide/orchestration.md).

# Basic Example

```python
from aaiclick.orchestration import job, task, job_test, TaskResult

@task
async def add(a: int, b: int) -> int:
    return a + b

@task
async def multiply(x: int, y: int) -> int:
    return x * y

@job("pipeline")
def pipeline(x: int, y: int):
    sum_result = add(a=x, b=y)      # no dependency between tasks → run in parallel
    product = multiply(x=x, y=y)
    return TaskResult(tasks=[sum_result, product])

job_test(pipeline, x=3, y=4)  # create the job and execute it synchronously
```

Full example: `aaiclick/orchestration/examples/orchestration_basic.py`.

# Decorators

**Implementation**: `aaiclick/orchestration/decorators.py`

Airflow-style TaskFlow API: passing a Task result as an argument creates an upstream dependency.

- **`@task`** — `name` (default: function name), `max_retries` (default `0`). A failure with retries left resets the task to PENDING with `attempt` incremented.
- **`@job`** — `@job("name")`, `@job(name="name")` or bare `@job`. Manages `orch_context()` and commits all tasks to SQL.
- **`job_test` / `ajob_test`** — run a `Job`, or a `@job` factory plus its kwargs, synchronously (`execution/debug.py`).

# Topology

**Implementation**: `aaiclick/orchestration/local_runtime.py`

| Component         | Count | Role                                                                 |
|-------------------|-------|----------------------------------------------------------------------|
| PostgreSQL        | 1     | Orchestration + user state: jobs, tasks, workers, auth, table refs   |
| ClickHouse        | 1     | Data factory: Object tables, `task_logs`, oplog                      |
| API server        | 1     | Combined REST + MCP FastAPI app                                      |
| Background worker | 1     | Cron scheduling, dead-worker detection, sole table-cleanup authority |
| Execution worker  | N     | Claims tasks; runs each in a child process / container / Pod         |

Processes never talk to each other: claiming, heartbeats and cancellation polling go through PostgreSQL; data and logs through ClickHouse. Local mode collapses the three process roles into one process (hence exactly one execution worker) on embedded chdb + SQLite.

# Deployment Modes

**Implementation**: `aaiclick/backend.py`

Two independent environment variables pick the mode:

| Aspect                 | Local (default)                            | Distributed                                                   |
|------------------------|--------------------------------------------|---------------------------------------------------------------|
| **Data backend**       | chdb (embedded ClickHouse)                 | ClickHouse server                                             |
| **SQL backend**        | SQLite via aiosqlite                       | PostgreSQL via asyncpg                                        |
| **`AAICLICK_CH_URL`**  | `chdb:///~/.aaiclick/chdb_data`            | `clickhouse://user:pass@host:8123/database`                   |
| **`AAICLICK_SQL_URL`** | `sqlite+aiosqlite:///~/.aaiclick/local.db` | `postgresql+asyncpg://user:pass@host:5432/database`           |
| **Setup**              | `python -m aaiclick setup`                 | Provision servers + `python -m aaiclick migrate upgrade head` |
| **Schema**             | `create_all`, no revision chain            | Alembic revision chain                                        |
| **Task claiming**      | Sequential SELECT + UPDATE                 | Atomic CTE with `FOR UPDATE SKIP LOCKED`                      |
| **Table lifecycle**    | `LocalLifecycleHandler` (in-process task)  | `OrchLifecycleHandler` (SQL refcounts)                        |
| **Detection**          | `is_chdb()` / `is_sqlite()` return `True`  | Both return `False`                                           |

**One migration chain, not two.** Alembic revisions target PostgreSQL only — they use plain `ALTER` of constraints, which SQLite cannot run without alembic's batch (copy-and-move) mode. Supporting both would mean writing, reviewing and testing every revision twice for a local database meant to be thrown away. Local SQLite is built by `create_all` and recreated, not migrated, when it falls behind (`aaiclick setup --force`).

The cost: locally there is no `alembic_version` row to answer *is this database current?* `setup` builds a throwaway in-memory database from the same `create_all` and diffs the real one against it — tables, column types, indexes, unique constraints, foreign keys. Missing tables (`missing_local_tables`) are added by a plain `create_all`; a table in the wrong shape (`stale_local_db_reason`) means recreating the database, since SQLite cannot `ALTER` its way there.

Accepted limit: anything a revision does that `create_all` would not (a backfill, a data repair) is invisible — local databases are recreated, so there is nothing to backfill.

## Worker runner

**Implementation**: `aaiclick/orchestration/execution/runner_env.py`

An execution worker launches container tasks through one **runner**, read once from `AAICLICK_RUNNER`. It is a property of the worker's deployment, not the job: the compose scaffold ships the `aaiclick-docker` worker with the host socket mounted, the helm scaffold the `aaiclick-kubectl` worker with a ServiceAccount, and a claimed task runs on whatever its worker has.

| `AAICLICK_RUNNER` | Container tasks run as                  | Extra worker requirements                      |
|-------------------|-----------------------------------------|------------------------------------------------|
| unset             | not at all — container tasks fail       | none                                           |
| `docker`          | sibling containers on the worker daemon | Docker daemon + CLI                            |
| `kubernetes`      | Pods in the cluster                     | `kubectl` (+ buildkitd & registry for `build`) |

- Tasks without an image (`tasks.image_source IS NULL`) run as a host subprocess on every worker.
- A `kubernetes` worker runs the concurrent reconciler of `kubernetes_runner.md` instead of the serial loop.
- No auto-detection: `aaiclick-kubectl` inherits the docker CLI, so an installed binary says nothing.
- `execution-worker start` validates the value and rejects `kubernetes` with `AAICLICK_LOCAL_BUILD` (the cluster cannot pull from the worker's daemon).
- Container tasks need the distributed backends: a container or Pod reaches ClickHouse + PostgreSQL over the network, and embedded chdb or a SQLite file cannot be shared into it. `run_job` rejects a resolved image source under `is_local()`.

# Runners & Entry Types

**Implementation**: `aaiclick/orchestration/runner_config.py`

Three orthogonal dials:

- **image source** (per *task*, chosen by the user) — `NULL` (host subprocess), `prebuilt` (a named image) or `build` (built from the repo).
- **runner** (per *worker*, chosen by the deployment) — `docker` or `kubernetes`: container or Pod. See [Worker runner](#worker-runner).
- **entry type** (per *task*) — `module` (import and run a Python entrypoint, default), `shell` (a literal argv) or `jvm` (a Java class name resolved by the `aaiclick-task-api` shim inside the image — `docs/designs/java-sdk.md`).

They compose freely: a `shell` task means "run this argv, success = exit 0" on a host subprocess, container or Pod alike. `jvm` tasks are container-only.

## Image source

**Implementation**: `aaiclick/orchestration/image_injection.py`

The image is a **task** property: nullable `tasks.image_source` JSON (`build` or `prebuilt`); **`NULL` runs a host subprocess** on any worker. The job row carries no runner state, only the optional `resources` snapshot (`docs/designs/kubernetes_runner.md` "Configuration").

| Source     | How                                                                                               | When built              |
|------------|---------------------------------------------------------------------------------------------------|-------------------------|
| `build`    | git repo → `aaiclick-job:<sha>` image built from `git clone` + `docker buildx build` on buildkitd | build task in the graph |
| `prebuilt` | `image="python:3.12"` run verbatim, no build stage                                                | never                   |

**Selecting a source.** The user names the source and nothing else: `image=` (`run-job --image`, or `register-job --image` as default) for prebuilt; `build=True` (`--build`) for a git build, modified by `git_remote` / `git_sha` / `git_branch` / `dockerfile`. Each modifier falls through to the registration's default, then the remote's branch head (the working tree only when the remote is unknown too). Defaults resolve **at submission**, so `image_source` always names a concrete commit. `image` and the build fields are **mutually exclusive** (`runner_config.py`, `docker_config.py`). First match wins:

1. run `image` → `prebuilt`
2. run `build` or any build modifier → `build`
3. registration `image` → `prebuilt`
4. registration `build` → `build`
5. otherwise → `NULL`, a host subprocess

A run cannot turn a containerized registration back into a subprocess.

**Children.** `run_job` stamps the resolved source onto the **entry task**; dynamic children inherit the committing task's image unless they declare their own (`create_task(image=... / git_*=...)`). `jvm` tasks must declare their own (`docs/designs/java-sdk.md`). A shell/jvm parent's children may declare a build only from the parent's own `git_remote` (trusted, any scheme) or a prefix in the worker's `AAICLICK_CHILD_BUILD_REMOTES` (`https://` or `git@host:` only): the worker clones with its own git credentials, so the container must not name an arbitrary repo.

**Build tasks.** Commit points inject one **ordinary build task** per distinct image identity (`sha256(git_remote, git_sha, dockerfile)`), host-pinned (`image_source=NULL`) and wired `build >> dependent` for every task on that image — the scheduler's dependency filter already guarantees no task is claimed before its image exists. Submission reads no build env; the **worker** running the build task picks the mode from two mutually exclusive variables:

| Env var                    | Mode     | Build body                                                                       | Launch path   |
|----------------------------|----------|----------------------------------------------------------------------------------|---------------|
| `AAICLICK_REGISTRY=<host>` | registry | registry-first (`buildx imagetools inspect` → done), else clone + build `--push` | `docker pull` |
| `AAICLICK_LOCAL_BUILD=1`   | local    | daemon-cache-first (`docker image inspect` → done), else clone + build `--load`  | run directly  |

- Both set, or neither, fails the build task naming both variables.
- Local mode is single-host by construction (the image lives only in the building daemon), so a kubernetes worker refuses to start with it.
- Crash recovery is the ordinary task retry/reaper path.
- Cross-job dedup is the registry (or daemon cache) itself: concurrent same-SHA jobs may double-build — wasteful but correct.
- The container launch path never builds; it only derives the tag: `aaiclick-job:<sha>`, suffixed `-<dockerfile>` for a non-default Dockerfile, so two Dockerfiles at one commit are two images.

**Builds run on buildkitd**, never on the host daemon: the build task drives the buildx remote driver at `AAICLICK_BUILDKIT_ADDR` (one builder per address + TLS settings, created on first use; optional `AAICLICK_BUILDKIT_TLS_{CACERT,CERT,KEY}`). Clone, default-Dockerfile scaffolding and push credentials stay in the worker; Dockerfile steps run in buildkitd's sandbox and see neither the worker's env nor `docker.sock`. `AAICLICK_PIP_INDEX_URL` / `AAICLICK_PIP_EXTRA_INDEX_URL` may carry a token, so they reach builds as BuildKit secrets (`pip_index_url`, `pip_extra_index_url`), never build-args. buildkitd does not know dockerd's `host-gateway`, so the worker resolves it in `AAICLICK_DOCKER_BUILD_ADD_HOST` to the bridge gateway IP. The deploy scaffolds run a rootless buildkitd; the helm chart also confines its egress with a NetworkPolicy.

**Worker prerequisites** beyond the [Worker runner](#worker-runner) table (every `build` also needs `AAICLICK_BUILDKIT_ADDR`):

- **`docker`** — Docker daemon + CLI (`AAICLICK_DOCKER_BIN`, default `docker`) on every worker that may run the job, to `docker run` tasks and `--load` local builds.
- **`kubernetes` + `build`** — **requires `AAICLICK_REGISTRY`** (startup rejects `AAICLICK_LOCAL_BUILD`; a missing registry fails the build task). The build task needs the docker CLI with buildx but no daemon; the Pod pulls from the registry.
- **`kubernetes` + `prebuilt`** — no Docker; only cluster access (`kubectl`), the cluster pulls the image.
- **unset** — no Docker; subprocess tasks only, and a container task fails with a message naming the variable.

After the cache check, a `build` preflights the builder (`docker buildx inspect --bootstrap`): a missing CLI, failed `buildx create`, unset `AAICLICK_BUILDKIT_ADDR` or unreachable buildkitd fails with an actionable error. Nothing falls back to the host daemon.

**Default Dockerfile.** When `dockerfile` is unset and the checkout has none, the build task writes a default (`execution/docker_scaffold.py`, from the detected root manifest) plus a `.dockerignore` for `.git` if the repo has none: a thin layer on `BASE_IMAGE` that copies the repo to `/src` and runs the install step the manifest calls for (`docs/user_guide/container_images.md` "Runner base"). Entrypoints import from there, so each directory on an entrypoint's path must be an importable package name.

- `BASE_IMAGE` is a build-arg forwarded on every build: `AAICLICK_BASE_IMAGE` on the worker (rc workers whose release tag is not promoted yet, dev installs), else `ghcr.io/kolodkin/aaiclick:v<worker's version>`. buildkitd pulls it, so a locally built base must be pushed to a registry first.
- A checked-in `Dockerfile` wins; an explicit `dockerfile=` path that is absent or resolves outside the checkout fails.
- The image key is unchanged — the fallback is a build-time detail, so a cached image keeps the base version of the worker that built it until the SHA changes (mixed-version fleets are unsupported anyway).
- This lets a repo of submitted job files (the sandbox) build without a platform-authored Dockerfile: the sandbox calls `run_job` with only `git_remote` / `git_sha` (`docs/user_guide/sandbox.md`).

## Shell entry type

A `shell` task runs a literal argv (`command`, a list) instead of a Python `entrypoint`; success is **exit code 0**. Its stdout and stderr stream live into `task_logs` on every runner (see [Task Logs](#task-logs)).

Inputs and result travel as `in.json` / `out.json` under `AAICLICK_IO_DIR` — the Task Interface, which also lets a shell task spawn children. `docs/designs/task_interface.md` covers the vocabulary, the file data plane, its transport per runner, and the env a container receives (`command_env` + `AAICLICK_IO_DIR` only, never the runner env).

```python
run_job(name, entry_type="shell", command=["python", "main.py"], command_env={"K": "v"})
```

The CLI (`--entry-type shell --command ... --command-env K=v`) and REST/MCP (`RunJobRequest`) take the same fields.

## Execution layers

**Implementation**: `aaiclick/orchestration/execution/dispatch.py`

A runner's *invocation* sits at a different layer than the task it runs; the `module` path blurs the two. Every runner has the same three layers:

1. **Host worker** — the runner's ExecuteFn (`mp_worker.py`, `docker_worker.py`, `kubernetes_worker.py`): claim, heartbeat, cancellation poll, wait, result read.
2. **Runner invocation** — *what the runner launches*: the mp child target, `docker run <image> <argv>`, or the Pod `command`. **`entry_type` branches here.**
3. **Task execution** — `execute_task(task)` running the module entrypoint.

`python -m aaiclick.orchestration.execution.remote_result --task-id N --run-epoch M` (run by docker containers and Pods) is a **layer-2 bootstrap shim** — not a queue-claiming loop, not task execution: it loads one task by id, boots `orch_context()`, calls `execute_task`, writes a `RemoteTaskResult` row and exits.

| entry_type | layer-2 runner invocation                                                     | layer-3 execution                                                          |
|------------|-------------------------------------------------------------------------------|----------------------------------------------------------------------------|
| `module`   | bootstrap shim (mp child / `remote_result` shim in a container or Pod)        | shim calls `execute_task`                                                  |
| `shell`    | the user's argv directly, with `in.json` / `out.json` under `AAICLICK_IO_DIR` | none — the argv *is* the execution                                         |
| `jvm`      | the image's own `ENTRYPOINT` (the `aaiclick-task-api` shim) + the class name  | the shim reads `in.json`, invokes the `@AaiTask` method, writes `out.json` |

A `module` entrypoint runs *inside* the shim; a `shell` argv *replaces* it, bypassing bootstrap and `execute_task`. A `jvm` task is a shell task whose argv is a class name: the shim is the SDK jar embedded in the user's image (`docs/designs/java-sdk.md`) and speaks the shell file data plane.

# DB Models

**Implementation**: `aaiclick/orchestration/models.py`

All entities use **Snowflake IDs** from ClickHouse [`generateSnowflakeID()`](https://clickhouse.com/docs/sql-reference/functions/uuid-functions#generateSnowflakeID) — distributed, time-ordered, no DB round-trip — stored as `BIGINT`.

## Status Enums

| Type                    | Values                                                                                                              |
|-------------------------|---------------------------------------------------------------------------------------------------------------------|
| `JobStatus`             | PENDING, RUNNING, COMPLETED, FAILED, CANCELLED                                                                      |
| `TaskStatus`            | PENDING, RUNNING, COMPLETED, FAILED, CANCELLED, PENDING_FAILURE_CLEANUP, PENDING_CANCELLED_CLEANUP, UPSTREAM_FAILED |
| `ExecutionWorkerStatus` | ACTIVE, STOPPING, STOPPED                                                                                           |
| `RunType`               | SCHEDULED, MANUAL, SANDBOX                                                                                          |

Three `TaskStatus` values are set by the background sweep, not the worker:

- `PENDING_FAILURE_CLEANUP` — transient; a failed or dead-worker run awaiting ref cleanup before it retries to PENDING or settles to FAILED.
- `PENDING_CANCELLED_CLEANUP` — transient; set by `cancel_job` and the fail-fast group-sibling abort. The worker kills the run; the sweep drops the attempt's refs and settles the task to CANCELLED once no worker owns it (`execution_worker_id` is NULL — released on report-back or by the expired-claim sweep).
- `UPSTREAM_FAILED` — terminal; a pending task whose transitive upstream failed or was cancelled.

## Entities

- **RegisteredJob** — job catalog entry (unique `name`, cron `schedule`, defaults, `next_run_at`); see [Registered Jobs](#registered-jobs).
- **Job** — one workflow run, optionally linked to its `RegisteredJob`.
- **Task** — single executable unit. `run_epoch` is a fencing token bumped on every reset or reclaim.
- **Group** — logical task grouping, nestable via `parent_group_id`.
- **Dependency** — composite PK `(previous_id, previous_type, next_id, next_type)`; each side is `'task'` or `'group'`, so all four combinations are supported.
- **ExecutionWorker** — a worker process with heartbeat and task counters.

## Task Parameter Serialization

Task kwargs and results are JSON: plain values are data; a dict carrying the reserved key `_aaiclick_type` is a ref (`upstream`, `object`, `native`, …) or a returned task DAG (`task`, `group`, `task_result`). Vocabulary, handles and return-value rules: `docs/designs/task_interface.md`; `aaiclick/data/object/refs.py` is the single source of truth.

# Task Execution

**Implementation**: `aaiclick/orchestration/execution/`

## ExecutionWorker Main Loop

**Implementation**: `aaiclick/orchestration/execution/execution_worker.py`

Polls for tasks, executes, updates status. Handles registration, heartbeats (30s), graceful shutdown and per-task lifecycle creation.

## Worker Liveness

Constants live in `models.py`.

1. **`WORKER_TIMEOUT` is 10 minutes** — long enough that a live worker is never swept. Trade-off: a dead worker's tasks wait that long before takeover.
2. **Worker self-stop.** A worker stops itself when a heartbeat reports it STOPPED (the conditional update matched no row) or its last successful heartbeat is older than `SELF_STOP_AFTER` (80% of `WORKER_TIMEOUT`). It aborts its running task without reporting it, exits, and rejoins only as a new `execution_workers` row. Since `SELF_STOP_AFTER < WORKER_TIMEOUT`, a worker cut off from the DB has stopped by the time it is swept.
3. **Heartbeat never revives a STOPPED worker** — it updates only where `status != STOPPED`. This depends on rule 2, or a revived worker keeps running unseen.
4. **Worker lifetime and task lifetime are independent.** Ownership lives on the task (`tasks.execution_worker_id` + `tasks.claim_expires_at`), never on worker status. Sweeping a worker STOPPED leaves its tasks alone; a task is reclaimed to `PENDING_FAILURE_CLEANUP` only when its own claim expires. Each heartbeat renews the running task's claim; a refused renewal means the run was aborted, and the cancellation watcher terminates it. A task can outlive its worker (retried elsewhere); a worker outlives many tasks.
5. **Double processing cannot be fully avoided.** Tasks must be idempotent. Reclaiming an expired claim bumps `run_epoch`, so the lost owner's late status writes are rejected; a side effect that cannot be repeated safely (payments, emails) must carry the epoch itself and reject an old one.

## Task Claiming

**Implementation**: `aaiclick/orchestration/execution/claiming.py`

Atomically claims the oldest pending task whose dependencies are all satisfied, moving its job PENDING→RUNNING on first claim (per-backend strategy: [Deployment Modes](#deployment-modes)).

# Job Management

**Implementation**: `aaiclick/orchestration/jobs/`

Queries live in `jobs/`; cancel and clear in `execution/claiming.py`.

- **Cancel** atomically cancels a job and all its non-terminal tasks; a missing or already-terminal job is a no-op.
- **Clear** (Airflow-style) resets a task and all its transitive downstream tasks to PENDING for re-run. Upstream tasks and their output tables are untouched; a terminal job is reactivated to RUNNING. Each reset bumps `run_epoch`, so an in-flight worker's late writes are rejected.

Workers detect cancellation by polling task status. **Known limitation**: CPU-bound tasks don't interrupt until they yield.

# Registered Jobs

**Implementation**: `aaiclick/orchestration/registered_jobs.py`

Catalog of known jobs, separate from runs. Each entry stores entrypoint, optional cron schedule, default kwargs, preservation-mode default and enabled flag. Re-registering needs `replace=True`; enable/disable recompute `next_run_at`.

## Preservation Config Precedence

`preservation_mode` resolves through four levels (`factories.py`):

| Level | Source                                               | Wins when                            |
|-------|------------------------------------------------------|--------------------------------------|
| 1     | Explicit `run_job(...)` / `create_job(...)` argument | The caller passes a non-`None` value |
| 2     | `RegisteredJob.preservation_mode`                    | The registered job carries a default |
| 3     | `AAICLICK_DEFAULT_PRESERVATION_MODE` env var         | Set in environment                   |
| 4     | `"NONE"`                                             | Hardcoded fallback                   |

`None` at any level inherits from the next; an explicit mode ends the chain. Scheduled runs get the level-2 default; manual runs (`run_job()`, CLI) can override at level 1.

## run_job

Links to the `RegisteredJob` matching `name` if one exists (merging `kwargs` over its `default_kwargs`); otherwise runs standalone with `registered_job_id=None`. Creates a `MANUAL` Job plus its entry Task. Mode semantics: [ObjectContext — Preservation Modes](../user_guide/object_context.md#preservation-modes).

## Cron Scheduling

`BackgroundWorker` polls enabled jobs with `next_run_at <= NOW()` (~10s). Optimistic locking on `next_run_at` prevents duplicates; once the lock commits, each run goes through `run_job(run_type="SCHEDULED")`, so registration config (preservation mode, runner, image source) applies as for a manual run. Cron is parsed by `croniter`; `next_run_at` is recomputed on registration, enable, and after each run.

# CLI

**Implementation**: `aaiclick/orchestration/cli.py`

**Local mode** (chdb + SQLite) — one process, no infrastructure. `local start` runs the combined REST + MCP server with the background and execution workers in the FastAPI lifespan, running setup first if needed:

```bash
python -m aaiclick local start [--host HOST] [--port PORT] [--reload]
# Stop with Ctrl+C / SIGTERM — the lifespan tears down workers cleanly.
```

**Distributed mode** (ClickHouse + PostgreSQL) — independent processes; tasks run in child processes for isolation:

```bash
python -m aaiclick execution-worker start [--max-tasks N]
python -m aaiclick execution-worker stop <execution_worker_id>
python -m aaiclick execution-worker list
python -m aaiclick background start
```

!!! warning "`execution-worker start`/`background start` require distributed backends"
    In local mode, use `local start` instead.

Job and registry commands: [user guide](../user_guide/orchestration.md).

# Orchestration Operators

**Implementation**: `aaiclick/orchestration/operators.py`

`map()` and `reduce()` return an **expander** task that fans out at runtime; the names below are the tasks it adds to the job graph.

| Task                                                | Role                                                                                          |
|-----------------------------------------------------|-----------------------------------------------------------------------------------------------|
| `map(cbk, obj, partition, args, kwargs) -> Task`    | Partitions the Object into Views, creates N `_map_part` children; result is the output.       |
| `_map_part`                                         | Applies `cbk(row, *args, **kwargs)` to each record; non-None returns are inserted into `out`. |
| `reduce(cbk, obj, partition, args, kwargs) -> Task` | Layered parallel reduction; result is the final single-row Object.                            |
| `_reduce_part`                                      | Calls `cbk(partition, output)`; the callback writes directly into the layer Object.           |
| `_finalize`                                         | Join task after the parts (or last layer); the expander returns it as data.                   |

## reduce()

The callback receives an input partition and a pre-allocated output Object and writes via `output.insert()`. All layer Objects and tasks are created at once; each layer depends on the previous.

```
Layer 0  input=N      tasks=⌈N/P⌉   → layer_0_obj
Layer 1  input=⌈N/P⌉  tasks=⌈.../P⌉ → layer_1_obj
…continues until 1 row remains
```

Empty input raises `TypeError("reduce() of empty sequence with no initial value")`.

Consumers of the expander wait for every layer: the expander's data is the `_finalize` task after the last layer, and registering returned tasks copies every edge leaving the expander (or its group) onto `_finalize` (`execution/runner.py`). A group consumer stays a group edge, so members it gains later are held too. The same hold applies to any `task_result` whose data is one of its returned tasks.

## Graph Frame

Each call draws as one frame in the job graph:

- `map()` / `foreach()` / `reduce()` put the expander in a `map` / `foreach` / `reduce` group at definition time.
- At runtime the expander nests its parts group(s) (`parts`, or `layer_N`) under its own group and adds `_finalize` to it.

The frame commits one extra `groups` row per call but changes no scheduling:

- No edge touches the frame, so the dependency filter and the hold on `_finalize` are unchanged.
- The expander and `_finalize` are its direct members, hence fail-fast siblings; they never run at the same time, so the sibling abort has nothing to cancel.
- Job cleanup deletes the frame with the job's other groups.

The frame is linked only through membership (`group_id` / `parent_group_id`), so task registration (`orch_context.py`) visits a task's group and a group's parent before the node; otherwise the frame is never committed. If a user moves the expander into a group of their own, the frame is never committed and the parts nest under the user's group.

# Distributed Object Lifecycle

**Implementation**: `aaiclick/orchestration/lifecycle/`

In distributed mode, Object table lifecycle lives in three PostgreSQL tables:

| Table                | PK                         | Purpose                                      |
|----------------------|----------------------------|----------------------------------------------|
| `table_context_refs` | `(table_name, context_id)` | Registry of which tasks created/used a table |
| `table_run_refs`     | `(table_name, run_id)`     | Active run references (incref/decref)        |
| `table_pin_refs`     | `(table_name, task_id)`    | Per-consumer pins protecting result tables   |

The background worker is the sole cleanup authority.

```
ExecutionWorker Process (spawns child per task)
├── OrchLifecycleHandler (per task, per child process)
│   ├── incref → insert into table_context_refs + table_run_refs
│   ├── decref → delete from table_run_refs
│   ├── pin → fan out: insert one pin_ref per downstream consumer
│   └── release_pins → delete every pin_ref held for this task
├── task_scope exit → decrefs ALL objects, stale-marks
├── BackgroundWorker (sole cleanup authority)
│   ├── DROP where no pin_refs AND no run_refs
│   ├── deletes context_refs + pin_refs alongside dropped tables
│   └── reclaims expired task claims → PENDING_FAILURE_CLEANUP
└── orch_context() — shared SQL session
```

- **Write-ahead incref**: `create_object()` increfs before `CREATE TABLE`, so a crash between the two is harmless (`DROP TABLE IF EXISTS`).
- **Local mode**: `LocalLifecycleHandler` wraps an asyncio table worker — immediate DROP on refcount 0, no PostgreSQL. See [ObjectContext](../user_guide/object_context.md).

## Pin Lifecycle

Only at runtime — when a producer returns an Object — are both the `table_name` and the upstream `task_id` known; the consumers are found through the dependencies table. This is the only point where the table→task mapping exists.

```
Task A executes, returns Object(table=T)
  ├── PIN fans out one hop via the dependencies table:
  │   → inserts pin_ref(T, B.task_id), pin_ref(T, C.task_id)
  └── task_scope exit: decref all → run_refs removed, pin_refs protect T

Task B deserializes its inputs and runs
  ├── incref → run_ref(T, B.run_id)     ← FIFO queue
  └── on success: release_pins → delete pin_ref(*, B.task_id)  ← FIFO: after every incref

Task C fails
  └── pins kept: pin_ref(T, C.task_id) holds T for the retry;
      the failure pass drops it only once C settles to FAILED

All consumers succeeded or settled → 0 pin_refs, 0 run_refs → eligible for cleanup
```

Dynamic children (map / reduce parts) exist only after their parent has pinned, so registering returned tasks pins each child on every ephemeral table its kwargs reference. The child releases them like any consumer.

## OrchLifecycleHandler

**Implementation**: `aaiclick/orchestration/orch_context.py`

Uses `task_id` as the `context_id` of context_refs. Pin fans out to consumers one dependency hop away through all four edge shapes (`dependency_graph.py`). A consumer releases all its pin_refs once its function returns successfully, including pins on tables it never reads (ordering edges).

## BackgroundWorker

**Implementation**: `aaiclick/orchestration/background/background_worker.py`

Runs these passes every `poll_interval` (default 10s), in order:

1. **Failure / cancelled cleanup** — drops refs of failed and cancelled runs and settles them. Failure runs first: its fail-fast sweep cancels group siblings, which the cancelled pass then settles in the same cycle.
2. **Table cleanup** — DROPs tables with no pin_refs and no run_refs, skipping jobs with `"FULL"` preservation.
3. **Expired jobs** — deletes all data of jobs past `AAICLICK_JOB_TTL_DAYS` (CH tables, oplog, `task_logs`, SQL metadata), plus orphans: tables registered without a job (`job_id IS NULL`) older than the same TTL.
4. **Liveness** — expired heartbeats → workers STOPPED (cutoff `WORKER_TIMEOUT`); expired claims → tasks PENDING_FAILURE_CLEANUP.
5. **Job scheduling** — creates runs for registered jobs whose `next_run_at` is due.
6. **Sandbox files** — submits every `@job` of a pending sandbox file once.

One failing pass (e.g. a DB restart) is logged and does not stop the loop or the other passes.

## View Lifecycle

Views share the underlying ClickHouse table with their source Object, which interacts with `table_run_refs` in a non-obvious way.

**`table_run_refs` is a set, not a counter.** The PK `(table_name, run_id)` allows one row per table per run (`INSERT … ON CONFLICT DO NOTHING`). A second `incref` in the same run is a no-op; the matching `decref` still deletes the one row.

**Views do not own lifecycle refs.** Only the source Object that registered the table (and actually inserted the `table_run_refs` row) issues the matching `decref`, so a View's `__del__` cannot delete the source's run_ref while the source is still alive.

**Within a task**, a View holds a Python reference to its source, keeping it alive as long as the View is. When the View is GC'd and the source has no other refs, the source is GC'd too and its `decref` fires normally.

**Across tasks**, Views use the same PIN mechanism as Objects:

```
Task A returns View(table=T)
  ├── pin fans out → pin_ref(T, B.task_id)
  └── task_scope exit → decref(T) [run_refs → 0, pin protects]

Task B deserializes View
  ├── fresh source = Object(table=T); registering it → INCREF(T)
  ├── view = View(source=source)   [view keeps source alive]
  └── release_pins() on success     [FIFO: after INCREF commits]
```

The deserialized source owns its ref and issues a normal `decref` at `task_scope` exit.

# Operation Provenance (Oplog)

With `object_context(oplog=...)` active, every Object operation in a task is logged. Spec: `docs/designs/oplog.md`.

# Task Logs

**Implementation**: `aaiclick/orchestration/logging.py`

Task stdout/stderr stream into the ClickHouse `task_logs` table every `LOG_FLUSH_INTERVAL` (2 s) with a running `seq` offset — long tasks are tailable live, and a killed run keeps everything flushed up to the last tick.

- Only stdout/stderr are captured; `logging` is left alone, so a record reaches the log only if its handler writes there. A failing task prints its traceback to stderr.
- Each row carries its `stream` (`stdout`/`stderr`, kept apart by separate pipes) and a per-line `created_at` (emit time, not flush time); the UI colors lines by keyword (`src/lib/logTone.ts`).
- Rows are job-scoped: the background worker drops a job's `task_logs` with its `operation_log` on TTL expiry, so logs share the job's retention.

Every entry type writes through one sink, so log reads hit one host-independent source wherever the task ran. `module` tasks feed it in-process; otherwise the host pumps the process's pipes:

| Entry type | subprocess                        | docker                           | kubernetes                                          |
|------------|-----------------------------------|----------------------------------|-----------------------------------------------------|
| `module`   | task child: `capture_task_output` | container: `capture_task_output` | Pod: `capture_task_output`                          |
| `shell`    | task child: argv pipes            | host: `docker logs -f`           | host: `kubectl logs -f -c task` (merged → `stdout`) |
| `jvm`      | — (rejected at commit)            | host: `docker logs -f`           | host: `kubectl logs -f -c task` (merged → `stdout`) |

Host-side following is the default for every container entry type except `module`, so a new language SDK needs no logging code. A `module` container that exits without registering a run (bootstrap failed before `execute_task`: bad DB URL, broken image) has its output copied in by the host.

`AAICLICK_TASK_LOGS` picks where task output goes:

| Value            | ClickHouse `task_logs` (UI log panel) | Console |
|------------------|---------------------------------------|---------|
| `both` (default) | yes                                   | yes     |
| `clickhouse`     | yes                                   | no      |
| `console`        | no                                    | yes     |

Console output is printed line by line. Container output carries a `[task N]` prefix — live for followed containers, at exit for `module` containers. aaiclick's own framework logs always go to the console.

# Configuration

| Variable              | Default                               | Description                               |
|-----------------------|---------------------------------------|-------------------------------------------|
| `AAICLICK_LOCAL_ROOT` | `~/.aaiclick`                         | Base directory for all local-mode state   |
| `AAICLICK_SQL_URL`    | `sqlite+aiosqlite:///{root}/local.db` | SQLAlchemy async URL for orchestration DB |
| `AAICLICK_CH_URL`     | `chdb://{root}/chdb_data`             | ClickHouse connection URL for data ops    |

`is_local()` is `True` when `AAICLICK_CH_URL` starts with `chdb://` and `AAICLICK_SQL_URL` with `sqlite`.

- **Setup (local)**: `python -m aaiclick setup`
- **Migrations (PostgreSQL)**: `python -m aaiclick migrate upgrade head` (`aaiclick/orchestration/migrate.py`)
