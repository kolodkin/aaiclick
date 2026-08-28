Orchestration
---

Define pipelines with `@task` and `@job`, run them locally with zero
infrastructure, and scale the same code to distributed workers that execute
tasks in subprocesses, Docker containers, or Kubernetes Pods.

# Quick Start

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

j = pipeline(x=3, y=4)
job_test(j)  # execute synchronously (testing/local)
```

Passing one task's result as another task's argument creates the dependency
automatically. See [Examples: Orchestration Basics](../examples/orchestration_basic.md)
for the complete runnable script and the
[tutorial chapter](../tutorial/orchestration.md) for a guided walk-through.

# Defining Tasks and Jobs

## @task

Wraps an async function into a task. Parameters:

| Parameter     | Default       | Description                                            |
|---------------|---------------|--------------------------------------------------------|
| `name`        | function name | Task display name                                      |
| `max_retries` | `0`           | Failed runs are retried this many times before failing |

A task can return a plain value or an `Object`/`View` — data results are
serialized automatically and handed to downstream tasks.

## @job

Wraps a workflow function into a job. Use `@job("name")`, `@job(name="name")`,
or bare `@job` (the function name becomes the job name). Calling the decorated
function creates the job and its tasks.

## Dynamic tasks

A task may itself return `TaskResult(tasks=[...])` to register new child tasks
at runtime — the graph grows while the job runs. See
[Examples: Orchestration Dynamic](../examples/orchestration_dynamic.md).

## Testing jobs

`job_test(j)` (sync) and `await ajob_test(j)` (async) execute every task of a
job in dependency order in the current process — ideal for developing and
debugging a pipeline before handing it to workers.

# Deployment Modes

Two deployment modes, selected by two environment variables:

| Aspect           | Local (default)                            | Distributed                                         |
|------------------|--------------------------------------------|-----------------------------------------------------|
| **Data backend** | chdb (embedded ClickHouse)                 | ClickHouse server                                   |
| **SQL backend**  | SQLite via aiosqlite                       | PostgreSQL via asyncpg                              |
| **`AAICLICK_CH_URL`**  | `chdb:///~/.aaiclick/chdb_data`      | `clickhouse://user:pass@host:8123/database`         |
| **`AAICLICK_SQL_URL`** | `sqlite+aiosqlite:///~/.aaiclick/local.db` | `postgresql+asyncpg://user:pass@host:5432/database` |
| **Setup**        | `python -m aaiclick setup`                 | Provision servers + `python -m aaiclick migrate upgrade head` |

## Local mode

Single process, no infrastructure required. `local start` runs the combined
REST + MCP server with the background and execution workers built in —
auto-runs setup if needed:

```bash
python -m aaiclick local start [--host HOST] [--port PORT] [--reload]
# Stop with Ctrl+C / SIGTERM.
```

## Distributed mode

Independent worker processes claim tasks from the shared PostgreSQL queue and
execute each in a child process for isolation:

```bash
python -m aaiclick execution-worker start [--max-tasks N]
python -m aaiclick execution-worker stop <execution_worker_id>
python -m aaiclick execution-worker list
python -m aaiclick background start        # scheduler + cleanup
```

!!! warning "`execution-worker start`/`background start` require distributed backends"
    In local mode, use `local start` instead.

# Running Jobs

Every action is available from three surfaces: Python (inside
`async with orch_context():`), the CLI, and the REST API served by
`local start` — default `http://127.0.0.1:5255`, interactive docs at
`/api/v0/docs`. The MCP server exposes the same operations. In distributed
mode the mutating REST endpoints require an admin JWT.

## Run a job

If a registered job matches `name`, the run links to it and merges `kwargs`
over its `default_kwargs`; otherwise the job runs standalone — registration is
not a prerequisite.

=== "Python"

    ```python
    from aaiclick.orchestration import orch_context
    from aaiclick.orchestration.registered_jobs import run_job

    async with orch_context():
        job = await run_job("crawl", "myapp.pipelines.crawl", kwargs={"url": "https://example.com"})
    ```

=== "CLI"

    ```bash
    python -m aaiclick run-job crawl --kwargs '{"url": "https://example.com"}'

    # Repeatable KEY=VALUE pairs; values are JSON-parsed (depth is an int,
    # force a bool, and the unparseable url stays a string).
    python -m aaiclick run-job crawl --set url=https://example.com --set depth=3 --set force=true
    ```

    `--set` beats `--kwargs`, which beats the registration's `default_kwargs`.
    A value that is not valid JSON stays a string.

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/jobs:run \
      -H "Content-Type: application/json" \
      -d '{"name": "crawl", "kwargs": {"url": "https://example.com"}}'
    ```

    A dotted `name` (e.g. `"myapp.pipelines.crawl"`) is used as the entrypoint
    directly; a bare name reuses the registered job's entrypoint.

All three surfaces also accept `preservation_mode` and the runner fields —
`entry_type` / `command` / `command_env` (see [Shell tasks](#shell-tasks)) and
`image` / `git_*` / `dockerfile` (see [Image source](#image-source-docker-kubernetes)).

## Wait for completion

`run-job` returns as soon as the job is queued. `--progress` blocks until it
reaches a terminal status; `job wait` does the same for an already-submitted job,
by id or name:

```bash
python -m aaiclick run-job crawl --set url=https://example.com --progress
python -m aaiclick job wait crawl --timeout 900
```

Both re-print the task table whenever the status counts change and exit
**non-zero** on failure, cancellation, or timeout (default 600s), so `set -e`
scripts and CI stop on a failed run. Failures print the failing task's full error
and its `task get` command; `--json` emits the final stats as one parseable
document, with diagnostics on stderr.

Registration is not a prerequisite: the wait follows the job id the run
returns, so a dotted entrypoint blocks and reports the same way.

```bash
python -m aaiclick run-job myapp.pipelines.crawl --set depth=3 --progress
```

!!! warning "A bare name that was never registered is not an entrypoint"
    With no `crawl` registration, `run-job crawl` falls back to the name itself
    as the entrypoint and the task fails on the worker with `Invalid entrypoint
    format: crawl. Expected 'module.function'`. `--progress` surfaces that in
    the same command instead of leaving a failed job to discover later.

**Implementation**: `aaiclick/cli_wait.py` — see `wait_for_job()`.

## Register a job

The catalog of known jobs, separate from individual runs. Each entry stores an
entrypoint, optional cron schedule, default kwargs, preservation-mode default,
runner defaults, and enabled flag. `name` defaults to the last dotted segment
of `entrypoint`.

=== "Python"

    ```python
    from aaiclick.orchestration import orch_context
    from aaiclick.orchestration.registered_jobs import register_job

    async with orch_context():
        await register_job(
            name="crawl",
            entrypoint="myapp.pipelines.crawl",
            schedule="0 8 * * *",
            default_kwargs={"depth": 3},
        )
    ```

=== "CLI"

    ```bash
    python -m aaiclick register-job myapp.pipelines.crawl --name crawl \
        --schedule "0 8 * * *" --kwargs '{"depth": 3}'
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/registered-jobs \
      -H "Content-Type: application/json" \
      -d '{"name": "crawl", "entrypoint": "myapp.pipelines.crawl", "schedule": "0 8 * * *", "default_kwargs": {"depth": 3}}'
    ```

Runner defaults are set here too: `--runner subprocess|docker|kubernetes` and
`--image python:3.12` on the CLI, `runner_mode` / `image` in Python and REST.

## Enable, disable, list registrations

=== "Python"

    ```python
    from aaiclick.orchestration import orch_context
    from aaiclick.orchestration.registered_jobs import (
        disable_job,
        enable_job,
        list_registered_jobs,
    )

    async with orch_context():
        await enable_job("crawl")
        await disable_job("crawl")
        registrations = await list_registered_jobs(enabled_only=True)
    ```

=== "CLI"

    ```bash
    python -m aaiclick job enable crawl
    python -m aaiclick job disable crawl
    python -m aaiclick registered-job list
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/registered-jobs/crawl/enable
    curl -X POST http://127.0.0.1:5255/api/v0/registered-jobs/crawl/disable
    curl http://127.0.0.1:5255/api/v0/registered-jobs
    ```

## Cron scheduling

A registered job with a `schedule` (standard cron expression) and
`enabled=True` is launched automatically by the background worker when its
next run time is due. Scheduled runs use the registration's `default_kwargs`
and defaults; manual `run_job()` calls can override them per run.

## Preservation modes

`preservation_mode` controls whether a job's intermediate tables are kept
after the run — see
[DataContext — Preservation Modes](data_context.md#preservation-modes) for the
two modes' semantics. The effective mode resolves through a precedence chain:

| Level | Source                                                | Wins when                            |
|-------|-------------------------------------------------------|---------------------------------------|
| 1     | Explicit `run_job(...)` argument                      | The caller passes a non-`None` value |
| 2     | Registered job's `preservation_mode`                  | The registration carries a default   |
| 3     | `AAICLICK_DEFAULT_PRESERVATION_MODE` env var          | Set in environment                   |
| 4     | `"NONE"`                                              | Hardcoded fallback                   |

# Runners

Two independent dials control how a task runs:

- **Runner mode** (per *job*) — where the execution environment lives.
- **Entry type** (per *task*) — what runs inside it: `"module"` (import and
  run a Python entrypoint, the default) or `"shell"` (run a literal argv).

## Runner modes

| Mode                      | Where the task runs                      | Worker host requirements                     |
|---------------------------|------------------------------------------|-----------------------------------------------|
| **subprocess** (default)  | Child process on the worker host         | none                                          |
| **docker**                | Container via the worker's Docker daemon | Docker daemon + CLI (`AAICLICK_DOCKER_BIN`)   |
| **kubernetes**            | Pod in a cluster                         | `kubectl` (+ Docker & `AAICLICK_REGISTRY` for `build`) |

The `docker` and `kubernetes` modes require the distributed backends — a
container or Pod reaches shared ClickHouse + PostgreSQL over the network, but
embedded chdb and a local SQLite file cannot be shared into it. `subprocess`
works in either deployment mode.

## Image source (docker / kubernetes)

The image is a **per-task** requirement. Each container task carries an
`image_source` from one of two kinds; a task with none runs as a host
subprocess even inside a docker/kubernetes job:

| Source     | How                                                        | When built            |
|------------|------------------------------------------------------------|-----------------------|
| `build`    | Your git repo, built into an image at a specific SHA       | by a `build-image` task in the job graph (with `AAICLICK_REGISTRY`), else inline at launch |
| `prebuilt` | `image="python:3.12"` run verbatim                         | never                 |

Pass `image=` (`run_job` / `run-job --image`, or `register-job --image` for a
default) to select a prebuilt image — mutually exclusive with the git build
fields (`git_remote` / `git_sha` / `git_branch` / `dockerfile`). `run_job`
stamps the resolved image on the job's entry task; dynamic child tasks inherit
their parent's image unless they declare their own
(`create_task(image=...)` or `create_task(git_remote=..., git_sha=...)`).

With `AAICLICK_REGISTRY` set, each distinct image gets one `build-image` task
in the job that every task on that image depends on — it pulls if the
registry already has the SHA, otherwise builds and pushes, and it appears in
the job graph like any other task (retries, logs, UI included).

For the released `aaiclick` container images and their Docker/Kubernetes
runtime requirements, see [Container Images](container_images.md).

## Shell tasks

A `shell` task runs a literal argv (`command`, a list) instead of importing a
Python entrypoint. Success is **exit code 0**; there is no `result.data()`.
stdout/stderr lands in the normal per-task log, so logs surface uniformly.

```python
run_job(name, entry_type="shell", command=["python", "main.py"], command_env={"K": "v"})
```

```bash
python -m aaiclick run-job <name> --entry-type shell --command 'python main.py' --command-env K=v
```

!!! warning "Shell tasks in containers see only `command_env`"
    In an isolated environment (container/Pod) a shell task receives **only**
    `command_env` on top of the image's env — not the aaiclick runner env — so
    no DB credentials leak into an arbitrary image. With the subprocess runner
    the command inherits the worker's process env with `command_env` overlaid.

# Parallel Operators

`map` and `reduce` fan a computation out over partitions of an `Object`
(`aaiclick/orchestration/operators.py`):

- `map(cbk, obj, partition=5000)` — partitions the Object and creates one
  child task per partition; `cbk(row, *args, **kwargs)` is applied to each row.
- `reduce(cbk, obj, partition=5000)` — layered parallel reduction; each layer
  reduces partitions down until a single row remains. `cbk(partition, output)`
  receives an input partition and a pre-allocated output Object and writes via
  `output.insert()`. The callback must be homomorphic: output schema equals
  input schema.

Both accept a `Task` or an `Object` as input and return a `Group` that
downstream tasks can depend on.

# Managing Jobs

## Inspect jobs

=== "Python"

    ```python
    from aaiclick.orchestration import get_job, list_jobs, orch_context

    async with orch_context():
        jobs = await list_jobs(status="RUNNING", name_like="%crawl%", limit=20)
        job = await get_job(job_id)
    ```

=== "CLI"

    ```bash
    python -m aaiclick job list [--status RUNNING] [--like "%crawl%"] [--limit 20 --offset 40]
    python -m aaiclick job get <id>
    python -m aaiclick job stats <id>
    ```

=== "REST"

    ```bash
    curl "http://127.0.0.1:5255/api/v0/jobs?status=RUNNING&limit=20"
    curl http://127.0.0.1:5255/api/v0/jobs/<id>
    curl http://127.0.0.1:5255/api/v0/jobs/<id>/stats
    ```

## Cancel a job

Atomically cancels the job and all its non-terminal tasks. Workers detect
cancellation by polling task status; a CPU-bound task won't interrupt until it
yields.

=== "Python"

    ```python
    from aaiclick.orchestration import cancel_job, orch_context

    async with orch_context():
        cancelled = await cancel_job(job_id)  # False if not found or already terminal
    ```

=== "CLI"

    ```bash
    python -m aaiclick job cancel <id>
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/jobs/<id>/cancel
    ```

## Re-run tasks (clear)

`clear_task` resets a task and all its transitive downstream tasks to PENDING
for re-run (Airflow-style "clear task"); upstream tasks and their results are
untouched, and a finished job is reactivated.

=== "Python"

    ```python
    from aaiclick.orchestration import orch_context
    from aaiclick.orchestration.execution.claiming import clear_task

    async with orch_context():
        cleared_task_ids, job = await clear_task(task_id)
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/tasks/<task_id>/clear
    ```

# Task Logs

Task stdout/stderr is streamed into the ClickHouse `task_logs` table while the
task runs (flushed every ~2 s), so a long-running task can be tailed live and
logs are readable from one place no matter which host, container, or Pod ran
the task. `logging.*` records keep their
level; raw `print()` output is captured as `INFO` (stdout) / `WARNING`
(stderr) — `ERROR` is reserved for `logging.error` records.
`AAICLICK_LOG_LEVEL` sets the captured root level (default `INFO`). Logs share
the job's retention lifecycle. Fetch them via
`GET /api/v0/tasks/<task_id>/logs?tail=100`.

# Configuration

| Variable                 | Default                               | Description                               |
|--------------------------|---------------------------------------|--------------------------------------------|
| `AAICLICK_LOCAL_ROOT`    | `~/.aaiclick`                         | Base directory for all local-mode state   |
| `AAICLICK_SQL_URL`       | `sqlite+aiosqlite:///{root}/local.db` | SQLAlchemy async URL for orchestration DB |
| `AAICLICK_CH_URL`        | `chdb://{root}/chdb_data`             | ClickHouse connection URL for data ops    |
| `AAICLICK_DEFAULT_PRESERVATION_MODE` | unset                     | Level-3 preservation-mode default         |
| `AAICLICK_DOCKER_BIN`    | `docker`                              | Docker CLI used by the docker runner      |
| `AAICLICK_REGISTRY`      | unset                                 | Registry for `build` images — pushed after build, pulled as cache by docker & kubernetes runners; required for kubernetes `build` |

# Internal Design

How the scheduler, task claiming, table lifecycle, and runner plumbing work
internally is documented in the
[orchestration design reference](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/orchestration.md)
(not part of this site).
