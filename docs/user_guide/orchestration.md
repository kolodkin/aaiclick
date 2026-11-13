Orchestration
---

Define pipelines with `@task` and `@job`, run them locally with zero
infrastructure, and scale the same code to distributed workers that execute
tasks in subprocesses, Docker containers, or Kubernetes Pods. Backends,
worker commands and environment variables: [Deployment](deployment.md).

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

job_test(pipeline, x=3, y=4)  # create the job and execute it synchronously
```

Passing one task's result as another task's argument creates the dependency.
Full script: [Examples: Orchestration Basics](../examples/orchestration_basic.md);
guided walk-through: [tutorial chapter](../tutorial/orchestration.md).

# Defining Tasks and Jobs

## @task

Wraps an async function into a task.

| Parameter     | Default       | Description                                            |
|---------------|---------------|--------------------------------------------------------|
| `name`        | function name | Task display name                                      |
| `max_retries` | `0`           | Failed runs are retried this many times before failing |

A task may return a plain value or an `Object`/`View`; data results are
serialized and handed to downstream tasks.

## @job

Wraps a workflow function into a job. Use `@job("name")`, `@job(name="name")`,
or bare `@job` (the function name becomes the job name). Calling the decorated
function creates the job and its tasks.

## Dynamic tasks

A task may itself return child tasks to register them at runtime — the graph
grows while the job runs. Return `tasks_list(a, b)` (or a plain list, `[a, b]`)
for tasks alone, and `task_result(data=..., tasks=[...])` when the task also
returns data. See
[Examples: Orchestration Dynamic](../examples/orchestration_dynamic.md).

When `data` is one of the returned tasks, consumers of the parent wait for that
task. So when the data is something the children produce, return it through a
task that depends on them: `map()`, `foreach()` and `reduce()` return a
finalize task that runs after the partition tasks and hands their result on.
Any other `data` is readable as soon as the parent completes.

!!! warning "A list carries tasks only, unnested"
    `return [obj, group]` raises `TypeError` — use
    `task_result(data=obj, tasks=[group])` to return data alongside tasks. So
    does `return [[a, b], [c, d]]`: nesting means nothing to the graph, so use a
    `Group` when tasks belong together.

## Explicit dependencies

When no data flows between two tasks, order them with `>>` (or `<<`): `a >> b`
runs `b` after `a`, `[a, b] >> c` fans in, and `a >> [b, c]` fans out. Chains
read left to right: `first >> group >> last`. `b.depends_on(a)` records the
same edge as a method call.

Pyright flags a bare `a >> b` as an unused expression: use `depends_on()`, or
put `# pyright: reportUnusedExpression=false` at the top of files that define jobs.

## Groups

A `Group` bundles tasks that belong together. `group.add_task(t)` makes `t` a
member, and a returned Group registers its members with it. Dependencies work
on the whole group: `a >> group` runs every member after `a`, and
`group >> b` runs `b` once every member has completed.

Passing a Group as a kwarg does both at once — it wires `group >> consumer`
and hands the consumer the members' results as a list, in task order:

```python
group = Group(name="producers")
for _ in range(3):
    group.add_task(step_a())
total = sum_results(results=group)  # receives [42, 42, 42]
return [group, total]
```

See [Examples: Orchestration Groups](../examples/orchestration_groups.md).

## Testing jobs

`job_test(j)` (sync) and `await ajob_test(j)` (async) execute every task of a
job in dependency order in the current process, for debugging before handing it
to workers. Both also accept the `@job` function plus its kwargs and create the
job first (`job_test(pipeline, x=3, y=4)`), and return the executed `Job`.

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
**non-zero** on failure, cancellation, or timeout (`--timeout`, default
`AAICLICK_JOB_WAIT_TIMEOUT`, 3600 s — a timeout stops watching while the job
keeps running), so `set -e` scripts and CI stop on a failed run. Failures print the failing task's full error
and its `task get` command; `--json` emits the final stats as one parseable
document, with diagnostics on stderr.

The wait follows the returned job id, so an unregistered dotted entrypoint
blocks and reports the same way.

```bash
python -m aaiclick run-job myapp.pipelines.crawl --set depth=3 --progress

# A dotted run is named after the last segment, so it is waitable by name too:
python -m aaiclick job wait crawl
```

!!! warning "A bare name that was never registered is not an entrypoint"
    With no `crawl` registration, `run-job crawl` falls back to the name itself
    as the entrypoint and the task fails on the worker with `Invalid entrypoint
    format: crawl. Expected 'module.function'`. `--progress` surfaces that in
    the same command.

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

Where a job runs is set here too: nothing → a subprocess on the worker;
`--image python:3.12` → a container from that image; `--build` → a container
built from your repo at the submitted commit (`--git-remote` / `--dockerfile`
modify it, `--git-sha` per run). Python and REST use `image` / `build`.
Container vs Pod is the worker's `AAICLICK_RUNNER` — see
[Deployment](deployment.md).

Job names follow the object name rule: letters, digits and `_`, not starting
with a digit, at most 128 characters. Others fail with HTTP 422; names
registered before the rule keep running.

Registering an existing name fails (HTTP 409). Pass `--replace` (Python
`replace=True`, REST/MCP `"replace": true`) to overwrite it — e.g. after a
signature change leaves stale `default_kwargs`. Every field is replaced: omitting `--schedule`
clears the schedule, and `enabled` resets to true.

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

The background worker launches a registered job with a `schedule` (standard
cron expression) and `enabled=True` when its next run is due, using the
registration's `default_kwargs` and defaults; manual `run_job()` calls can
override them per run.

## Preservation modes

`preservation_mode` controls whether a job's intermediate tables are kept
after the run ([ObjectContext — Preservation Modes](object_context.md#preservation-modes)).
The effective mode resolves by precedence:

| Level | Source                                       | Wins when                            |
|-------|----------------------------------------------|--------------------------------------|
| 1     | Explicit `run_job(...)` argument             | The caller passes a non-`None` value |
| 2     | Registered job's `preservation_mode`         | The registration carries a default   |
| 3     | `AAICLICK_DEFAULT_PRESERVATION_MODE` env var | Set in environment                   |
| 4     | `"NONE"`                                     | Hardcoded fallback                   |

# Runners

Two independent dials control how a task runs:

- **Runner mode** (per *job*) — where the execution environment lives.
- **Entry type** (per *task*) — what runs inside it: `"module"` (import and
  run a Python entrypoint, the default) or `"shell"` (run a literal argv).

## Runner modes

| Mode                     | Where the task runs                      | Worker host requirements                               |
|--------------------------|------------------------------------------|--------------------------------------------------------|
| **subprocess** (default) | Child process on the worker host         | none                                                   |
| **docker**               | Container via the worker's Docker daemon | Docker daemon + CLI (`AAICLICK_DOCKER_BIN`)            |
| **kubernetes**           | Pod in a cluster                         | `kubectl` (+ Docker & `AAICLICK_REGISTRY` for `build`) |

`docker` and `kubernetes` require the distributed backends: a container or Pod
reaches shared ClickHouse + PostgreSQL over the network, but embedded chdb and a
local SQLite file cannot be shared into it. `subprocess` works in either mode.

## Image source (docker / kubernetes)

The image is a **per-task** requirement. Each container task carries an
`image_source` from one of two kinds; a task with none runs as a host
subprocess even inside a docker/kubernetes job:

| Source     | How                                                  | When built                               |
|------------|------------------------------------------------------|------------------------------------------|
| `build`    | Your git repo, built into an image at a specific SHA | by a `build-image` task in the job graph |
| `prebuilt` | `image="python:3.12"` run verbatim                   | never                                    |

Pass `image=` (`run_job` / `run-job --image`, or `register-job --image` for a
default) to select a prebuilt image — mutually exclusive with the git build
fields (`git_remote` / `git_sha` / `git_branch` / `dockerfile`). `run_job`
stamps the resolved image on the job's entry task; dynamic child tasks inherit
their parent's image unless they declare their own
(`create_task(image=...)` or `create_task(git_remote=..., git_sha=...)`). A
shell or jvm task may only return children built from its own repo or a
prefix in the worker's `AAICLICK_CHILD_BUILD_REMOTES`.

| Flag           | Default                          | Auto-sets |
|----------------|----------------------------------|-----------|
| `--image`      | none                             | —         |
| `--build`      | off                              | —         |
| `--git-remote` | registered, else local `origin`  | `--build` |
| `--git-sha`    | remote branch head               | `--build` |
| `--git-branch` | remote default branch            | `--build` |

Defaults resolve at submission, so the stored task always names a concrete
commit. The working tree is read only when no remote is given or registered.
Image fields require a docker or kubernetes job, and kubernetes settings a
kubernetes job; setting either on another runner is an error, not a no-op.

Each distinct image gets one `build-image` task that every task on that image
depends on; it is an ordinary task in the job graph (retries, logs, UI). The
worker running it picks one of two mutually exclusive build modes:

| Env var                    | Mode     | What the build task does                                                              |
|----------------------------|----------|---------------------------------------------------------------------------------------|
| `AAICLICK_REGISTRY=<host>` | registry | Skips if the registry has the image, else builds and pushes; every worker pulls it    |
| `AAICLICK_LOCAL_BUILD=1`   | local    | Builds and loads into the worker's own Docker daemon, no push — single-host only      |

Either way the build runs on the buildkitd at `AAICLICK_BUILDKIT_ADDR`, not on
the worker's daemon, so Dockerfile steps never see the worker's credentials.

Setting both, or neither, fails the build task with an error naming the two
variables. Kubernetes `build` sources need registry mode: the cluster cannot
pull from a worker's daemon.

**Private GitHub repos.** Give the worker a
[GitHub App](https://docs.github.com/en/apps/creating-github-apps) key and its
clone authenticates on its own:

| Env var                        | Value                                            |
|--------------------------------|--------------------------------------------------|
| `AAICLICK_GITHUB_APP_ID`       | The App's ID                                     |
| `AAICLICK_GITHUB_APP_PEM_FILE` | Path to the App's private key, mounted read-only |

- The App needs the **Contents: read** repository permission and must be
  installed on each repo to build.
- Any `github.com` remote works: `https://github.com/org/repo`,
  `git@github.com:org/repo.git`, `ssh://git@github.com/org/repo`. An SSH
  remote is fetched over HTTPS.
- Each build mints its own one-hour token, limited to that repo, read-only.
- When GitHub will not issue a token (App not installed on the repo, missing
  permission, outage), the clone goes ahead without credentials and the build
  task's log says why: a public repo still builds, a private one fails in git.
- A remote that carries its own password (`https://user:token@github.com/...`)
  is left alone.
- Setting only one of the two fails every build that has to clone, naming
  the other.

Setup per deployment: [Docker Compose](deployment.md#docker-compose-mode),
[Kubernetes](deployment.md#kubernetes-mode).

!!! warning "Submit private-repo jobs with `--git-sha`"
    With a remote passed or registered and no SHA, the submitter asks the
    remote for the branch head — without the App's credentials, so it fails.
    Pass `--git-sha`, or submit from a checkout with no remote passed or
    registered (the working tree supplies the SHA).

!!! warning "Tasks that run on the worker can read the key"
    A task with no image runs as a subprocess of the worker and can read the
    key file, then mint tokens with everything the App is allowed. Install
    the App only on the repos to build, grant it nothing beyond
    **Contents: read**, and keep such jobs off a worker that holds the key.

For the released `aaiclick` container images and their Docker/Kubernetes
runtime requirements, see [Container Images](container_images.md).

## Resources

`resources` is a per-job dict (`register-job` default, overridable per
`run_job`) shaped for the deployment's one runner; mixed docker / kubernetes
fleets are not supported.

```python
# AAICLICK_RUNNER=docker — docker run flags, verbatim
run_job(name, image="python:3.12", resources={"cpus": "1.5", "memory": "512m"})

# AAICLICK_RUNNER=kubernetes — Pod container resources, verbatim
run_job(name, image="python:3.12", resources={"limits": {"cpu": "500m", "memory": "512Mi"}})
```

- **Docker** accepts resource flags only (`cpus`, `memory`, `gpus`, …); any
  other key fails the task with the allowed list. `0` means unlimited, and
  `memory` alone allows swap: set `memory-swap` to the same value to forbid it.
- A non-mapping is rejected at register / run; a value the runner rejects fails
  the task.

## Shell tasks

A `shell` task runs a literal argv (`command`, a list) instead of importing a
Python entrypoint. Success is **exit code 0**; stdout/stderr land in the normal
per-task log. The command reads its kwargs
from `$AAICLICK_IO_DIR/in.json` and may write a result — or child tasks — to
`out.json`: see [Task Interface](task_interface.md).

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

`map`, `foreach` and `reduce` fan a computation out over partitions of an
`Object`:

- `map(cbk, obj, partition=5000)` — partitions the Object and creates one
  child task per partition; `cbk(row, *args, **kwargs)` is applied to each row
  (a value for single-column Objects, a `dict` for multi-column ones) and its
  return value is appended to the output Object. A `None` return adds no row.
  Output rows land in partition-completion order, not input order.
- `foreach(cbk, obj, partition=5000)` — `map()` for side effects (write a
  file, call an API): the return value is discarded, no output Object is
  allocated, and the expander's result is `None`.
- `reduce(cbk, obj, partition=5000)` — layered parallel reduction; each layer
  reduces partitions down until a single row remains. `cbk(partition, output)`
  receives an input partition and a pre-allocated output Object and writes via
  `output.insert()`. The callback must be homomorphic: output schema equals
  input schema.

All three accept a `Task` or an `Object` as input and return the expander
`Task`. Its result is the output Object (`None` for `foreach`), produced by a
finalize task that runs after every partition task, so a consumer sees it
filled. See [Examples: Orchestration Operators](../examples/orchestration_operators.md).

!!! warning "Output schema equals input schema"
    Both operators allocate the output from the input's schema, and ClickHouse
    casts returned values to it on insert: a `map()` callback returning `2.5`
    for an integer column stores `2`. Map over a float column, or convert in
    the callback, when the type changes. Create the input with `aai_id=True`
    (or an `order_by` view) so the LIMIT/OFFSET partitions are disjoint.

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
    python -m aaiclick task list [--job <ref>] [--like "%load%"] [--status FAILED] [--limit 20 --offset 40]
    ```

=== "REST"

    ```bash
    curl "http://127.0.0.1:5255/api/v0/jobs?status=RUNNING&limit=20"
    curl http://127.0.0.1:5255/api/v0/jobs/<id>
    curl http://127.0.0.1:5255/api/v0/jobs/<id>/stats
    curl "http://127.0.0.1:5255/api/v0/tasks?job=<ref>&name=%25load%25"
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

Task stdout/stderr streams into the ClickHouse `task_logs` table while the
task runs (flushed every ~2 s), so a long task can be tailed live, from one
place, whichever host, container, or Pod ran it. Only stdout/stderr are captured: `logging` records appear only
when their handler writes to stdout/stderr (e.g. `logging.basicConfig()`
inside the task). The UI colors lines by keyword — `error` / `exception` /
`traceback` red, `warn` yellow, `debug` dim. Logs share
the job's retention lifecycle. Fetch them via
`GET /api/v0/tasks/<task_id>/logs?tail=100`.

## Worker logs

Every long-running command (`local start`, `execution-worker start`,
`background start`) writes timestamped `INFO` lines to stderr. The execution
worker logs `executing task` before each task and `completed task` (or the
failure state) after it, so a slow task shows up as a gap between its two
lines. Local mode also logs `chdb session ready in Ns` when the embedded
engine starts. The first task after a start is still slower than the rest: the
engine's first DDL and first insert fault more of its ~540 MB library into
memory.

# Internal Design

Scheduler, task claiming, table lifecycle, and runner plumbing are covered in
the [orchestration design reference](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/orchestration.md)
(not part of this site).
