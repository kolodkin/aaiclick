Docker Runner
---

A third task-execution runner for the orchestration backend, alongside the
existing in-process runner and the multiprocessing runner. The Docker runner
executes each task in a dedicated container built from the user's repo at a
specific git commit. A dedicated build task in the same job graph builds
(and optionally pushes) the image before any container task runs.

# Motivation

The existing runners execute task code in the same Python environment as the
worker process — either in-process (`runner.execute_task`) or in a
multiprocessing child (`mp_worker._child_run_task`). This couples the worker
deployment to the user's code dependencies and makes per-job environment
isolation impossible.

A Docker runner gives each job a reproducible, self-contained execution
environment defined by the user's repo at a specific git SHA. Workers no
longer need to share a Python env with user code; users can pin
dependencies in their Dockerfile; the same job submitted at different
times runs against the exact code that was committed when it was submitted.

# Scope

In scope:

- A new `ExecuteFn` (`_run_task_in_container`) that runs each task in a
  fresh container via `docker run`.
- A build task auto-injected into Docker-mode jobs that builds (and
  optionally pushes) the image before any container task runs.
- New `RegisteredJob` and `Job` columns capturing runner mode and Docker
  config.
- Image tagging by git SHA with local-cache and registry-cache hits.
- Cancellation and timeout via `docker kill`.
- Heartbeats sent from the host parent while the container runs.

Out of scope (deferred to future work):

- Local-mode (chdb + SQLite) Docker support — rejected at submission.
- Mid-job runner switching (a job is "all subprocess" or "all docker plus
  build task").
- BuildKit cache mounts, multi-arch builds, remote builders — user's
  Dockerfile / `buildx` config concern.
- Image GC; per-job resource limits (`--memory`, `--cpus`); user-supplied
  volume mounts.
- Non-CLI-compatible runtimes (containerd, CRI, k8s).
- Backporting Docker's mid-task cancellation to mp_worker.

# Architecture

The runner abstraction is unchanged: an `ExecuteFn` is
`Callable[[Task, int], Awaitable[(success, result_ref, log_path, error)]]`.
The Docker runner is a third implementation alongside the in-process runner
and `mp_worker._run_task_in_child`.

The worker loop gains a per-task dispatcher that selects the `ExecuteFn`
based on the task's runner kind:

```python
async def dispatch_execute(task: Task, worker_id: int):
    if _resolve_runner(task) == RUNNER_DOCKER:
        return await _run_task_in_container(task, worker_id)
    return await _run_task_in_child(task, worker_id)
```

`_resolve_runner` reads `Job.runner_mode` for the task's job, with a
hardcoded exception that the auto-injected build task always runs as
subprocess (it builds the image the rest of the job needs).

A single worker process handles both runners. Per-task dispatch — rather
than separate worker types — is required because every Docker job
contains a mix: one host-side build task and N container tasks. Dedicated
worker types would force affinity rules just to run the build task.

## Module Layout

```
aaiclick/orchestration/execution/
  runner.py           # unchanged — execute_task, deserialize, etc.
  worker.py           # _worker_loop gains dispatch_execute()
  mp_worker.py        # unchanged (subprocess ExecuteFn)
  docker_worker.py    # NEW — host-side ExecuteFn + container-side entrypoint
  docker_build.py     # NEW — build-task @task function (host-side)
```

`docker_worker.py` mirrors `mp_worker.py`'s convention of holding both
sides of the IPC: the host-side `_run_task_in_container` (the `ExecuteFn`)
and the container-side `_container_main` (invoked as
`python -m aaiclick.orchestration.execution.docker_worker --task-id N`
inside the container).

`docker_build.py` is separate because it is a regular `@task`-decorated
function — user-task-shaped code that happens to be owned by the
framework rather than the user. It runs on the host via the subprocess
runner like any other task.

# Runtime Flow

## At Job Submission

`run_job` (and the cron scheduler) does:

1. Resolve `RegisteredJob`. If `runner_mode == "docker"`:
   - Verify distributed mode (`is_chdb()` / `is_sqlite()` → reject with
     a clear error).
   - **Resolve each Docker field** by precedence and snapshot onto `Job`.
     Every field below has the same three-layer resolution: an
     explicit `run_job(...)` kwarg wins over the `RegisteredJob`
     default, which wins over the auto-detect rule (where one
     applies). `RegisteredJob` is the long-lived "job default",
     `Job` is the snapshot the build task and runner read.

     | Field           | `run_job` kwarg  | `RegisteredJob` default            | Auto-detect                          |
     |-----------------|------------------|------------------------------------|--------------------------------------|
     | `git_remote`    | `git_remote=`    | `RegisteredJob.git_remote`         | `git config remote.origin.url`       |
     | `git_sha`       | `git_sha=`       | —                                  | `git rev-parse HEAD` (rejects dirty / unpushed) |
     | `git_branch`    | `git_branch=`    | —                                  | `git rev-parse --abbrev-ref HEAD`; `NULL` on detached HEAD |
     | `build_context` | `build_context=` | `RegisteredJob.build_context`      | empty (= repo root)                  |
     | `dockerfile`    | `dockerfile=`    | `RegisteredJob.dockerfile`         | `"Dockerfile"`                       |

     `git_branch` is metadata-only — never used for resolution;
     stored so it can be propagated as a build-arg to the user's
     Dockerfile.

   - Compute `image_tag = f"{registry_prefix}aaiclick-job:{git_sha}"`,
     where `registry_prefix = f"{AAICLICK_DOCKER_REGISTRY}/"` if set, else
     empty.
2. Create `Job` row with `runner_mode="docker"` and the snapshotted
   `git_remote`, `git_sha`, `git_branch`, `build_context`,
   `dockerfile`, `image_tag`.
3. Create the build task (entrypoint
   `aaiclick.orchestration.execution.docker_build.build_image`).
4. Create the entry task with the user's job entrypoint.
5. Insert dependency: `build_task >> entry_task`.
6. Commit. Submitter returns.

`runner_mode` itself is **not** overridable at `run_job` time — switching
between subprocess and docker mid-cron-cadence is a bigger semantic
change than v1 needs. Edit the `RegisteredJob` if you need to switch.

## Build Task Execution (host, subprocess runner)

```python
@task(name="docker_build", max_retries=2)
async def build_image(job_id: int) -> None:
    job = await _fetch_job(job_id)

    if registry := os.environ.get("AAICLICK_DOCKER_REGISTRY"):
        if await _docker_pull(job.image_tag):
            return  # registry hit — image now in local daemon
    if await _docker_image_exists_locally(job.image_tag):
        return  # local cache hit

    with tempfile.TemporaryDirectory() as workdir:
        await _git_clone_at_sha(job.git_remote, job.git_sha, workdir)
        context = os.path.join(workdir, job.build_context or "")
        dockerfile = os.path.join(context, job.dockerfile or "Dockerfile")
        build_args = _collect_build_args(job)
        await _docker_build(context, dockerfile, job.image_tag, build_args)

    if registry:
        await _docker_push(job.image_tag)
```

`max_retries=2` because clone / pull / push can fail transiently, and the
build is fully idempotent (tag is content-addressed by SHA).

`_collect_build_args(job)` emits a `["--build-arg", "KEY=value", ...]`
slice with the following entries (omitted when their value is unset,
empty, or `None`). The contract: every build-arg corresponds to a
value the framework's build task already needs as a job parameter.

| Build arg             | Value                                          | Source                  |
|-----------------------|------------------------------------------------|-------------------------|
| `GIT_REMOTE`          | `job.git_remote`                               | Captured at submission  |
| `GIT_SHA`             | `job.git_sha`                                  | Captured at submission  |
| `GIT_BRANCH`          | `job.git_branch` (skipped if NULL)             | Captured at submission  |
| `BUILD_CONTEXT`       | `job.build_context` (skipped if empty)         | RegisteredJob field     |
| `PIP_INDEX_URL`       | `os.environ["AAICLICK_PIP_INDEX_URL"]`         | Operator env var        |
| `PIP_EXTRA_INDEX_URL` | `os.environ["AAICLICK_PIP_EXTRA_INDEX_URL"]`   | Operator env var        |

A future per-job `build_args` field can extend this for arbitrary
forwarding; not needed for v1.

The recommended user-Dockerfile pattern: receive a build-arg, emit it
as both `LABEL` (visible via `docker inspect <image>`) and `ENV`
(visible to the running task code via `os.environ`). See the example
fixture Dockerfile in **End-to-End Tests** below — it covers
`GIT_REMOTE`, `GIT_SHA`, `GIT_BRANCH`, `BUILD_CONTEXT`,
`PIP_INDEX_URL`, and `AAICLICK_VERSION` end-to-end.

The kwarg is `job_id` only; the build task reads the latest job state
from the DB. Output is `None` — image existence is the side effect, the
tag was already deterministically computed at submission.

Failure modes:

- Git remote unreachable → clone fails → task fails → job fails.
- SHA not pushed → clone-at-SHA fails → "the SHA you submitted isn't
  actually pushed" surfaces cleanly.
- `docker build` fails → task fails; build stdout/stderr lands in the
  task's regular log file.
- Registry push fails → task fails before any container task runs.

## Entry / Downstream Task Execution (host parent)

```python
async def _run_task_in_container(task, worker_id):
    job = await _fetch_job(task.job_id)
    if registry := os.environ.get("AAICLICK_DOCKER_REGISTRY"):
        await _docker_pull(job.image_tag)  # cached after first pull on host

    with tempfile.TemporaryDirectory() as ipc_dir:
        env = _build_container_env()
        cmd = _build_docker_run_cmd(job.image_tag, task.id, ipc_dir, env)
        container_id = await _docker_run_detached(cmd)

        done = asyncio.Event()
        heartbeat = asyncio.create_task(
            _heartbeat_while_waiting(worker_id, done)
        )
        cancel_watcher = asyncio.create_task(
            _watch_for_cancellation(task.id, container_id, done)
        )
        try:
            exit_code = await _wait_for_container(container_id, timeout)
            return _read_result_or_synthesize_failure(
                ipc_dir / "result.json", exit_code
            )
        finally:
            done.set()
            await asyncio.gather(
                heartbeat, cancel_watcher, return_exceptions=True
            )
```

The container is invoked as:

```
docker run --rm \
  -v <ipc_tmpdir>:/aaiclick-ipc \
  -v <log_base>:<log_base> \
  -e AAICLICK_SQL_URL=... \
  -e AAICLICK_CH_URL=... \
  [-e AAICLICK_TASK_TIMEOUT=...] \
  [-e <passthrough vars>] \
  <image_tag> \
  python -m aaiclick.orchestration.execution.docker_worker --task-id <task.id>
```

No `--network` flag is passed. The framework relies on the operator
ensuring `AAICLICK_SQL_URL` and `AAICLICK_CH_URL` resolve from inside a
container (e.g., real hostnames or `host.docker.internal`).

Container stdout/stderr is streamed to the host log file, but the
container also writes its own structured log via `capture_task_output`
into the bind-mounted log dir. The streaming is supplementary debug
output, not the primary log channel.

## Container-Side Execution

`_container_main` (invoked by the entrypoint above):

1. `orch_context()` boots — connects to Postgres + ClickHouse using the
   env-passed URLs.
2. Fetch Task by id, run `execute_task(task)` — same code path as
   in-process and mp_worker.
3. `register_returned_tasks` + `serialize_task_result` — same as today.
4. Write a JSON file to `/aaiclick-ipc/result.json` with the schema:

   ```json
   {
     "success": true | false,
     "result_ref": dict | null,
     "log_path": str | null,
     "error": str | null
   }
   ```

5. `sys.exit(0)` on success, `sys.exit(1)` on exception.

Difference from `mp_worker._child_run_task`: the IPC mechanism is a JSON
file in a mounted tmpdir instead of a `multiprocessing.Queue`. JSON over
pickle because the four fields are JSON-native by construction
(`result_ref` is a JSONB-bound dict, the rest are primitives), and JSON
avoids the class-path coupling pickle would create between host and
container — important because the host worker and a deployed image may
be different aaiclick versions.

The host's `_handle_task_result` (worker.py:245) does the terminal DB
write exactly as today. The Docker runner is a faithful `ExecuteFn`
peer; only the IPC transport differs.

# Image Tagging & Cache

```
image_tag = f"{registry_prefix}aaiclick-job:{git_sha}"
```

Single source of truth: computed once at submission, persisted on
`Job.image_tag`, read by the build task and the Docker runner.

Build task's cache hierarchy:

| Step | Check                                          | Hit action                |
|------|------------------------------------------------|---------------------------|
| 1    | `AAICLICK_DOCKER_REGISTRY` set + `docker pull` | Done — image now local    |
| 2    | `docker image inspect <tag>` succeeds          | Done — local cache hit    |
| 3    | Fall through                                   | Clone + `docker build`    |
| 4    | Step 3 ran + registry set                      | `docker push`             |

Re-running the same SHA twice is cheap. First run builds and (optionally)
pushes; second run hits the cache and the build task is effectively a
no-op. This makes Docker mode usable for re-running flaky pipelines
without rebuilding everything.

Tag immutability is not enforced by the framework — `docker tag` could
locally rebind a tag, and registry-side immutability is a registry
configuration concern. Documented as a recommendation, not a guarantee.

Layer caching during build, GC of accumulated images, and BuildKit
features are out of scope; the framework just calls plain `docker build`.

# Schema Changes

All on existing tables; no new tables.

## `registered_jobs`

These columns hold **defaults** — the long-lived "job default"
declared at registration time. Every column has a matching column on
`Job` that snapshots the resolved value at submission. `run_job` may
override any of them per-run (see **At Job Submission** for the
precedence table).

| Column          | Type                       | Default              | Purpose                                                |
|-----------------|----------------------------|----------------------|--------------------------------------------------------|
| `runner_mode`   | `String` + CHECK           | `"subprocess"`       | `"subprocess"` or `"docker"` (not `run_job`-overridable) |
| `dockerfile`    | `String`                   | `NULL` → `Dockerfile`| Default Dockerfile path relative to `build_context`    |
| `git_remote`    | `String`                   | `NULL` → auto-detect | Default; falls through to `git config remote.origin.url` |
| `build_context` | `String`                   | `NULL` → repo root   | Default subdirectory offset within the cloned tree used as the docker build context (monorepo support) |

CHECK constraint name: `ck_registered_jobs_runner_mode`.

Following the project's `Literal` + CHECK pattern:

```python
RUNNER_SUBPROCESS = "subprocess"
RUNNER_DOCKER = "docker"
RunnerMode = Literal["subprocess", "docker"]
RUNNER_MODES: list[RunnerMode] = [RUNNER_SUBPROCESS, RUNNER_DOCKER]
```

## `jobs`

These columns are the **primary** values the build task and runner
read. They're snapshotted at submission from a three-layer resolve:
`run_job` kwarg → `RegisteredJob` default → auto-detect. Once
written, the build task / runner never re-read `RegisteredJob` —
later edits to it don't affect in-flight or already-submitted runs.

| Column          | Type                       | Default        | Purpose                                            |
|-----------------|----------------------------|----------------|----------------------------------------------------|
| `runner_mode`   | `String` + CHECK           | `"subprocess"` | Same enum as `RegisteredJob`                       |
| `git_remote`    | `String`                   | `NULL`         | Resolved remote URL                                |
| `git_sha`       | `String(40)`               | `NULL`         | Resolved 40-char hex commit SHA                    |
| `git_branch`    | `String`                   | `NULL`         | Captured branch name; `NULL` if detached HEAD. Metadata-only — propagated as `GIT_BRANCH` build-arg |
| `build_context` | `String`                   | `NULL`         | Snapshotted subdirectory offset                    |
| `dockerfile`    | `String`                   | `NULL`         | Snapshotted Dockerfile path (relative to `build_context`) |
| `image_tag`     | `String`                   | `NULL`         | `[registry/]aaiclick-job:<sha>`                    |

All seven are `NULL` for subprocess jobs and populated for Docker jobs
(except `git_branch` and `build_context`, which can be `NULL` even
for a Docker job — detached HEAD and repo-root context respectively).
CHECK constraint name: `ck_jobs_runner_mode`.

Snapshotting (rather than reading `RegisteredJob` at task-execute time)
matches the existing pattern for `default_kwargs` → `Task.kwargs`. A run
must be reproducible independently of later edits to the registered job.

## `tasks`

**No new columns.** The task's runner is derived, not stored:

```python
def _resolve_runner(task: Task) -> RunnerMode:
    if task.entrypoint == BUILD_TASK_ENTRYPOINT:
        return RUNNER_SUBPROCESS  # build task itself runs on host
    job = _fetch_job(task.job_id)
    return job.runner_mode
```

Avoids a redundant denormalized column, prevents drift, keeps the
build-task-runs-on-host rule in one place.

## Migration

A single Alembic revision generated via `alembic revision -m "add docker
runner support"`. Adds the columns above with `server_default` so
existing rows get `runner_mode='subprocess'` at deploy time. CHECK
constraints added in the same revision. Both upgrade and downgrade paths
must work; downgrade drops columns and constraints.

No backfill needed — defaults handle existing rows.

# Configuration

Two layers: environment (operator-controlled) and per-`RegisteredJob`
(declared by the job author).

## Environment Variables

| Variable                          | Default       | Purpose                                                      |
|-----------------------------------|---------------|--------------------------------------------------------------|
| `AAICLICK_DOCKER_REGISTRY`        | unset         | If set, build task pushes; runner pulls. Multi-host support. |
| `AAICLICK_PIP_INDEX_URL`          | unset         | If set, build task forwards as `--build-arg PIP_INDEX_URL=…` so the user's `pip install` inside the Dockerfile resolves through this index. Production case: corporate / internal PyPI mirrors. Test case: e2e workflows pointing at a pypiserver service. |
| `AAICLICK_PIP_EXTRA_INDEX_URL`    | unset         | If set, forwarded as `--build-arg PIP_EXTRA_INDEX_URL=…`. Standard pip "fall through to this index for missing packages" semantics. |
| `AAICLICK_DOCKER_PASSTHROUGH_ENV` | unset         | Comma-separated env var names to copy host → container.      |
| `AAICLICK_DOCKER_BIN`             | `docker`      | Path to docker CLI (e.g., `podman`, must be CLI-compatible). |
| `AAICLICK_TASK_TIMEOUT`           | unset         | Existing var; honored via `docker kill`.                     |
| `AAICLICK_SQL_URL`                | required      | Must resolve from inside the container.                      |
| `AAICLICK_CH_URL`                 | required      | Must resolve from inside the container.                      |

Always-passed env vars (hardcoded into every `docker run`, no opt-in):

- `AAICLICK_SQL_URL`
- `AAICLICK_CH_URL`
- `AAICLICK_TASK_TIMEOUT` (when set)
- `AAICLICK_DEFAULT_PRESERVATION_MODE` (when set — affects subjobs)

The container needs all of these to do its job; making them opt-in via
`PASSTHROUGH_ENV` would be a footgun.

The framework does not pass `--network`. Operator ensures the SQL and CH
URLs resolve from inside a container (real hostnames, `host.docker.internal`,
or a Docker network the operator manages outside the framework).

## Per-RegisteredJob Defaults

These fields establish the **default** values for the registered
job. Every run snapshots the resolved value onto `Job`; `run_job` can
override on a per-run basis (see precedence table in **At Job
Submission**).

- `runner_mode` — `"subprocess"` or `"docker"`. Not `run_job`-overridable.
- `dockerfile` — default Dockerfile path (relative to `build_context`).
  `NULL` → `"Dockerfile"`.
- `git_remote` — default git URL. `NULL` → auto-detect via
  `git config remote.origin.url`.
- `build_context` — default subdirectory offset within the cloned
  tree used as the docker build context. `NULL` → empty (repo root).
  Useful for monorepos with multiple jobs.

## Not Configurable

- **Image tag formula** — fixed as `[registry/]aaiclick-job:<sha>` so the
  runner can compute it deterministically without per-job config.
- **Container entrypoint** — fixed as
  `python -m aaiclick.orchestration.execution.docker_worker --task-id N`.
  The user's Dockerfile must produce an image where this works
  (`aaiclick` importable, `python` on `PATH`).
- **Volume mounts** — only the IPC tmpdir and the log directory. User
  code reaches state through CH/PG, not the host filesystem.
- **User UID/GID** — left to the Dockerfile's `USER` directive.
- **Resource limits** (`--memory`, `--cpus`) — future work.

# Logging, Cancellation, Timeout, Heartbeats

Four lifecycle ops the host parent must handle while a container runs.
Each mirrors mp_worker's behavior, with a docker-shaped equivalent.

## Logging

`capture_task_output` runs **inside** the container (in `_container_main`).
User code's `print()` and stdlib logging flow into
`<log_base>/<job_id>/<task_id>/<run_id>.log` exactly as in the in-process
and mp_worker runners.

The log directory is bind-mounted into the container as
`-v <log_base>:<log_base>` (same path inside and outside) so absolute
paths built by `capture_task_output` work without translation.

The container's stdout/stderr is also streamed by the host parent for
debugging visibility, but the canonical log file is the one
`capture_task_output` writes.

UID/GID note: a Dockerfile that runs as a UID different from the host
worker's user can cause permission errors when writing to the bind-mounted
log dir. Documented recommendation: image's `USER` should match host
worker's UID, or the log dir should be accessible to both.

## Cancellation

A sibling asyncio task in the host parent polls
`check_task_cancelled(task_id)` while `docker run` is in flight:

```python
async def _watch_for_cancellation(task_id, container_id, done):
    while not done.is_set():
        await asyncio.sleep(POLL_INTERVAL)
        if await check_task_cancelled(task_id):
            await _docker_kill(container_id)
            return
```

On cancel, `docker kill <container>` (SIGKILL by default). The container
has no grace period — fine because all real state is in CH/PG, not in
the container. The host then returns `(success=False, error="cancelled")`
and `_handle_task_result` flips the task accordingly.

This is **better cancellation than mp_worker has today** (mp_worker can
only kill on timeout). Backporting that improvement to mp_worker is
future work.

## Timeout

Same env var as today (`AAICLICK_TASK_TIMEOUT`); same mechanism as
`mp_worker._poll_child` (mp_worker.py:163). Host parent tracks elapsed
wall time; on expiry, `docker kill <container>`. Container exits
non-zero, host returns
`(success=False, error=f"Task timed out after {timeout}s")`.

## Heartbeats

Same as `mp_worker._heartbeat_while_waiting` (mp_worker.py:131). A
sibling asyncio task in the host parent updates `worker.last_heartbeat`
every `HEARTBEAT_INTERVAL` (30s) for as long as the container is
running. The container itself plays no heartbeat role.

# Crash Safety and the Reaper Invariant

The orchestration backend already has a reaper:
`BackgroundWorker._cleanup_dead_workers` (background_worker.py:426)
periodically queries the `workers` table for rows whose
`last_heartbeat < now - worker_timeout` while still in `ACTIVE` or
`STOPPING`. For each dead worker, `mark_dead_workers` (pg_handler.py:17)
flips the worker to `STOPPED` and moves any of its `RUNNING` / `CLAIMED`
tasks to `PENDING_CLEANUP`. A separate cleanup step releases lifecycle
refs and either retries the task (if `attempt < max_retries`) or marks
it `FAILED`.

The Docker runner introduces a new race condition: the host worker can
die while its container is still running. The container will eventually
finish and try to write its result.

**Invariant (Docker runner)**: container processes never write terminal
status (`COMPLETED`, `FAILED`, `PENDING_CLEANUP`) to the `tasks` table.
Terminal status writes happen exclusively in the host worker via
`_handle_task_result`, or in the background reaper via
`mark_dead_workers`. The container's only completion-channel is
`<ipc_tmpdir>/result.json`, which is consumed by the host parent. If the
host parent is dead, the container's eventual write to that file is
harmless — no DB row is updated.

Consequence: a "ghost container" (one whose host died mid-execution)
cannot retroactively flip a reaped task back to `COMPLETED`. The
reaper's decision wins.

What the container *does* write to the DB during normal execution
(non-terminal):

- `execute_task` (runner.py:277-284) appends to `Task.run_ids` and
  `Task.run_statuses` (adds the new run_id with `TASK_RUNNING`).
  Append-only; cannot flip terminal status.
- `register_returned_tasks` may insert new rows into `tasks` and
  `dependencies` for dynamically spawned children.

Side effects of ghost containers (accepted, not Docker-specific):

- Ghost container's CH writes happen during work. When the task is
  retried, its writes happen again. The first table is orphaned and is
  cleaned up by `_cleanup_orphaned_resources`. Same failure mode as
  mp_worker's child crashing mid-CH-write.
- Ghost container's `register_returned_tasks` may create child rows.
  When the parent is retried, a new set of children is created. The old
  children become orphans of a `PENDING_CLEANUP` / `FAILED` parent.
  Same trade-off mp_worker already has.

The `--rm` flag on `docker run` ensures orphan containers self-delete on
exit so they don't accumulate.

# Testability

Tests split into two layers by **kind**: unit tests of specific
modules live next to those modules per the project convention; the
end-to-end test exercises the deployed package as a black box and
lives outside it.

## Unit Tests (in-package)

| File                          | Scope                                            | Fixture           |
|-------------------------------|--------------------------------------------------|-------------------|
| `test_docker_worker.py`       | Host-side `_run_task_in_container`: dispatch, IPC, exit-code translation, cancel poll, timeout via `docker_kill`, heartbeat cadence | `orch_ctx_no_ch`  |
| `test_docker_build.py`        | Build task: cache-hit ladder (registry → local → build), push gating on `AAICLICK_DOCKER_REGISTRY`, dockerfile-not-found error, dirty-tree / unpushed-HEAD rejection at submission | `orch_ctx`        |
| `test_docker_container_main.py` | Container-side `_container_main`: orch_context boot, `execute_task` round-trip, JSON result file format, `register_returned_tasks` integration, `sys.exit` codes on success / failure | `orch_ctx_no_ch`  |

All three live in `aaiclick/orchestration/execution/` next to the
modules they test. They use in-process fakes for `docker` / `git` CLI
calls — no daemon required — and run in the regular `test-local` /
`test-dist` matrix.

`test_docker_worker.py` and `test_docker_container_main.py` use
`orch_ctx_no_ch` for the same reason mp-worker tests do: the
**host-side** code under test must not hold a chdb session in the test
process (the host worker doesn't open one in production either; only
the container does). Container-side tests run in the test process to
exercise the same code path the container would, so the chdb session
they open lives in that test's module only — keep these in their own
module to avoid mixing with `orch_ctx`.

`test_docker_build.py` uses `orch_ctx` because the build task is a
regular `@task`-decorated function on the subprocess runner; it never
opens a chdb session itself but goes through `execute_task`.

## End-to-End Tests (out-of-package)

The e2e suite exercises a real docker daemon, real registry, and the
package as a black box. It lives **outside** `aaiclick/` — at repo
root in `test_e2e/docker/` — because (a) it tests the deployed
artifact rather than a specific module, (b) heavy fixtures (sample
job repo, fixture Dockerfiles) shouldn't bloat the published wheel,
and (c) spatial separation makes the cost / setup difference obvious
to anyone browsing the tree.

`test_e2e/` is the umbrella for any future end-to-end suite (a
distributed-cluster suite, a k8s-runner suite, etc.); for v1 only
`test_e2e/docker/` is populated.

```
test_e2e/
  docker/
    conftest.py            # docker_e2e marker registration; daemon-presence skip
    test_runner_e2e.py     # the test file
    fixtures/
      sample_job/          # the e2e's "user repo" — a normal aaiclick user project
        pyproject.toml
        sample_jobs.py
        Dockerfile          # ARG PIP_INDEX_URL; pip install aaiclick[distributed]==$VERSION
```

The fixture's `Dockerfile` is a plain user-perspective Dockerfile —
nothing test-specific. It demonstrates the recommended build-arg →
LABEL + ENV pattern:

```dockerfile
FROM python:3.10-slim

# Framework-forwarded build-args (see "_collect_build_args" table)
ARG GIT_REMOTE
ARG GIT_SHA
ARG GIT_BRANCH
ARG BUILD_CONTEXT
ARG PIP_INDEX_URL
ARG AAICLICK_VERSION

# Image metadata (visible via `docker inspect <image>`)
LABEL org.opencontainers.image.source="${GIT_REMOTE}"
LABEL org.opencontainers.image.revision="${GIT_SHA}"
LABEL org.opencontainers.image.ref.name="${GIT_BRANCH}"
LABEL aaiclick.build_context="${BUILD_CONTEXT}"

# Runtime env (visible to task code via os.environ)
ENV GIT_REMOTE=${GIT_REMOTE} \
    GIT_SHA=${GIT_SHA} \
    GIT_BRANCH=${GIT_BRANCH} \
    BUILD_CONTEXT=${BUILD_CONTEXT}

RUN pip install "aaiclick[distributed]==${AAICLICK_VERSION}"
COPY . /src
RUN pip install /src       # makes sample_jobs importable as a real package
```

`sample_job/pyproject.toml` declares `sample_jobs` as a package so
`pip install /src` puts it on the container's import path — the
container entrypoint resolves the task's `"sample_jobs.entry_task"`
via normal `importlib`. (Just `COPY` + `WORKDIR` would only work if
the container's CWD ended up on `sys.path`; installing as a package
is the standard pattern users follow and worth modeling.)

No `Dockerfile.source` / `Dockerfile.wheel` split. Both workflows
upload the wheel they want tested to the local pypiserver before
running pytest, then point the framework at it via
`AAICLICK_PIP_INDEX_URL`. The framework forwards as `--build-arg`,
the user's plain `RUN pip install aaiclick…` resolves through the
test pypi, and the container ends up running the wheel under test.

## Test Git Repo

With `build_context` available, the e2e test reuses the **aaiclick
repo itself** as its "user repo" and points `build_context` at the
fixture subdir. The aaiclick checkout the workflow already has on the
runner is the test repo — no bare-repo fixture, no per-test git
ceremony.

Test setup:

```python
async def test_docker_runner_smoke(orch_ctx):
    workspace = os.environ["GITHUB_WORKSPACE"]
    sha = os.environ["GITHUB_SHA"]

    await register_job(
        "smoke",
        "sample_jobs.entry_task",
        runner_mode=RUNNER_DOCKER,
        git_remote=f"file://{workspace}/.git",
        build_context="test_e2e/docker/fixtures/sample_job",
        # dockerfile defaults to "Dockerfile" relative to build_context
    )
    job_id = await run_job("smoke", git_sha=sha)
    await wait_for_job(job_id)
    # assert task completed
```

`file://` (not `https://github.com/...`) because the build runs on
the host and the host already has the checkout — going to GitHub
would be a pointless network round-trip and adds flake surface. The
framework treats `git_remote` as opaque and shells out to `git
clone`, so `file://` exercises the same code path as `https://`.

Image-tag note: `image_tag` is keyed on `git_sha` only, so an
aaiclick-code-only commit (sample_job unchanged) still cache-misses
and rebuilds. The Dockerfile is small and the build is cheap — accept
it. A future content-hash mode for `build_context` could fix this if
it ever matters in practice.

Production users on multi-host setups will use real `https://...` URLs
naturally; the framework supports both with zero special-casing.

Carve-outs from project defaults this requires:

- **CLAUDE.md** — add a one-liner under *Testing Guidelines* exempting
  end-to-end suites from the "tests next to modules" rule:
  > E2E tests that exercise the deployed package live in `./test_e2e/<suite>/`.
- **`pyproject.toml`** — do **not** add `test_e2e/` to `testpaths`. The
  default `pytest` invocation should not pick it up; only the e2e
  workflows pass the path explicitly. The `docker_e2e` marker registers
  in `test_e2e/docker/conftest.py` (so `--strict-markers` passes when
  pytest is invoked with that path).
- The `conftest.py` adds `pytest_plugins = ["aaiclick.testing"]` to
  reuse the shared `orch_ctx` fixture without copy-pasting.

## Shared Test Surface

Two CI workflows run the e2e suite (nightly schedule + release gate).
They share **test code**, not workflow plumbing — same pattern as
`test.yaml::test-dist` and `publish.yaml::test-package`: similar
service-container setup in both workflows, but each is its own job
that installs aaiclick its own way and points pytest at the same
target.

Shared, in `test_e2e/docker/`:

- `test_runner_e2e.py` — the test file.
- `@pytest.mark.docker_e2e` — the marker each workflow filters on.
- `fixtures/sample_job/` — the e2e's "user repo" with one plain
  `Dockerfile` that does `pip install aaiclick[distributed]==$VERSION`.

Per-workflow:

- How the test runner process gets aaiclick (`uv sync` from source
  vs. install from the `dist` artifact).
- **Where the wheel that ends up in pypiserver comes from** (`uv build`
  from source vs. the `dist` artifact). The pypiserver-upload step
  itself is identical between workflows.
- `AAICLICK_E2E_AAICLICK_VERSION` env var value (matches the wheel
  uploaded; passed to the test as the version pin).

## Nightly Workflow

`.github/workflows/test-docker-nightly.yaml` — standalone, kept out of
`test.yaml` so a slow / flaky daemon-backed test never blocks PRs.

Triggers:

- `schedule: cron: "0 6 * * *"` (06:00 UTC nightly).
- `workflow_dispatch` for manual re-runs.

Runner: `ubuntu-latest`. Docker daemon ships preinstalled, same as the
existing `test-dist` job.

Service containers (clickhouse + postgres + registry:2 + pypiserver):

| Service      | Image                              | Purpose                                                       |
|--------------|------------------------------------|---------------------------------------------------------------|
| `clickhouse` | `clickhouse/clickhouse-server:26.3`| Distributed-backend CH for both host and container            |
| `postgres`   | `postgres:18.3`                    | Distributed-backend orchestration DB                          |
| `registry`   | `registry:2`                       | Local docker registry on `localhost:5000` for push/pull       |
| `pypiserver` | `pypiserver/pypiserver:v2.3.2`     | Test PyPI on `localhost:8080` — serves the under-test wheel; `--fallback-url https://pypi.org/simple/` for everything else |

Workflow steps:

1. `uv sync --frozen --extra distributed --extra test --python 3.10` (matches `test-dist`).
2. `uv build` to produce a wheel for the current source tree
   (`aaiclick-X.Y.Z.devN+gSHA-...whl`).
3. Upload to pypiserver:
   ```bash
   for f in dist/*.whl dist/*.tar.gz; do
     curl -fsSL -F "content=@${f}" http://localhost:8080/
   done
   ```
4. Run pytest with the framework-level pip-index env set:
   ```bash
   AAICLICK_PIP_INDEX_URL=http://host.docker.internal:8080/simple/ \
   AAICLICK_E2E_AAICLICK_VERSION=$(uv run python -c 'from importlib.metadata import version; print(version("aaiclick"))') \
   AAICLICK_DOCKER_REGISTRY=localhost:5000 \
   uv run pytest test_e2e/docker/ \
     -m docker_e2e -n 0 -v --junitxml=tmp/pytest-report.xml
   ```

The framework reads `AAICLICK_PIP_INDEX_URL` and forwards it as
`--build-arg PIP_INDEX_URL=…` on `docker build`. The fixture
Dockerfile picks it up via `ARG PIP_INDEX_URL`. No special test-only
plumbing — the same env var a production user would set against an
internal mirror is what the workflow uses against the local pypiserver.

`AAICLICK_E2E_AAICLICK_VERSION` is consumed by the test's
`register_job` call (which configures the build's `--build-arg
AAICLICK_VERSION=…`), so the Dockerfile pins to exactly the wheel
that was uploaded.

Test step env (also includes):

- `AAICLICK_SQL_URL` / `AAICLICK_CH_URL` pointing at the service hosts
  via `host.docker.internal:<port>` so the launched aaiclick container
  can reach Postgres and ClickHouse the same way the spec recommends
  to operators.

`-n 0` (no xdist) because parallel `docker build` against the same tag
serializes on the daemon anyway, and image disk usage on the runner
(~14 GB free) is the binding resource.

Cleanup step before exit: `docker system prune -af`.

## Release Gate

`publish.yaml` gains a third release-gate job alongside the existing
`test-package-local` and `test-package` (does **not** call the nightly
workflow — it's its own inlined job, shaped like the other
`test-package-*` jobs):

Identical service-container set to nightly (clickhouse + postgres +
registry:2 + pypiserver). The only structural difference is **where
the wheel comes from**: the release-gate job downloads the `dist`
artifact built earlier in the workflow run instead of running
`uv build` from source.

```yaml
test-package-docker-e2e:
  needs: build
  runs-on: ubuntu-latest
  services:    # clickhouse + postgres + registry:2 + pypiserver — same as nightly
    ...
  steps:
    - uses: actions/checkout@v5  # required: e2e tests live outside the package
    - uses: astral-sh/setup-uv@v7
    - run: uv venv --python 3.10
    - uses: actions/download-artifact@v7
      with: { name: dist, path: dist/ }
    - uses: actions/download-artifact@v7
      with: { name: requirements }
    - name: Upload wheel to test pypi
      run: |
        for f in dist/*.whl dist/*.tar.gz; do
          curl -fsSL -F "content=@${f}" http://localhost:8080/
        done
    - name: Install aaiclick from test pypi
      env:
        UV_INDEX_URL: http://localhost:8080/simple/
      run: |
        uv pip install -r requirements-dist.txt
        VERSION="${{ inputs.tag }}"
        uv pip install "aaiclick[distributed]==${VERSION#v}"
    - run: uv run --no-project python -m aaiclick migrate upgrade head
    - env:
        AAICLICK_PIP_INDEX_URL: http://host.docker.internal:8080/simple/
        AAICLICK_E2E_AAICLICK_VERSION: ${{ inputs.tag }}  # vX.Y.Z, fixture strips leading v
        AAICLICK_DOCKER_REGISTRY: localhost:5000
        # ... CH/SQL URLs etc.
      run: >-
        uv run --no-project pytest test_e2e/docker/
        -m docker_e2e -n 0 -v ...
```

`actions/checkout` is the only structural deviation from the existing
`test-package-*` jobs (which rely entirely on the installed wheel and
do no checkout) — needed because the e2e test code lives outside the
package.

The existing `publish` job's `needs:` extends:

```yaml
publish:
  needs: [test-package-local, test-package, test-package-docker-e2e]
```

Effect: **a release cannot be published unless the docker runner can
build, push, pull, and execute a task using the exact wheel about to
hit PyPI.** This catches release regressions the source-tree nightly
would miss — a missing file in the wheel, an entrypoint that only
resolves from an editable install, a runtime dep dropped from the
wheel's metadata.

End-result: the path `wheel → upload to pypi → pip resolves index →
install in container → execute task` is the exact same path a
downstream user takes after `pip install aaiclick[distributed]==X.Y.Z`,
so a release that breaks any of those steps fails the gate before
real PyPI is updated.

Break-glass input on `publish.yaml` for emergency releases when the
daemon-backed test is failing for unrelated reasons:

```yaml
on:
  workflow_dispatch:
    inputs:
      skip-docker-e2e:
        description: "Skip the docker e2e release gate (emergency only)"
        type: boolean
        default: false
```

Implemented via `if: ${{ !inputs.skip-docker-e2e }}` on
`test-package-docker-e2e`, with `publish` using
`if: always() && (needs.test-package-docker-e2e.result == 'success' ||
needs.test-package-docker-e2e.result == 'skipped') && ...` so a skipped
job doesn't fail the gate but a failed job does.

## Post-Publish Smoke (deferred)

A non-blocking job that runs **after** `publish` with a third
`AAICLICK_E2E_WHEEL_SOURCE=pypi` value (the test installs
`aaiclick==<published-version>` from PyPI inside the Dockerfile) would
catch the rare case where the upload succeeded but the artifact on
pypi.org is somehow different from what we tested. Listed in
`docs/future.md` rather than v1 — pypi.org is not known to silently
corrupt uploads, and the additional release latency is hard to justify
until we've seen a real failure.

# Open Implementation Questions

1. **IPC tmpdir lifecycle**: a `tempfile.TemporaryDirectory()` context in
   `_run_task_in_container` is the obvious answer. `_wait_for_container`
   returns only after `docker run` exits, so the cleanup ordering is
   naturally correct.
2. **Dockerfile path validation**: build task should validate that
   `<build_context>/<dockerfile>` exists in the cloned tree before
   invoking `docker build`, with a clear error like
   `"Dockerfile not found at {build_context}/{dockerfile} in repo
   {git_remote}@{git_sha}"`.
3. **`--user` controllability per-RegisteredJob**: out of scope for v1;
   document the recommendation that the Dockerfile's `USER` matches the
   host worker's UID, or the log dir is world-writable.
4. **CLI shape**: `register-job <entrypoint> --runner docker
   --dockerfile path/to/Dockerfile --build-context subdir/` hung off
   the existing `register-job` command. No new top-level commands.

# Success Criteria

- A registered job with `runner_mode=docker` and a Dockerfile in the repo
  can be `run-job`'d, builds an image, runs the entry task in a
  container, and produces a CH-backed `Object` result identical to what
  the subprocess runner would produce.
- Re-running the same job (same SHA) on the same host completes in
  seconds — image cache hit, no rebuild.
- With `AAICLICK_DOCKER_REGISTRY` set, a job submitted on host A can be
  picked up and run on host B (registry pull, then `docker run`).
- Cancelling a Docker job mid-execution terminates the running container
  within ~POLL_INTERVAL seconds.
- Killing a host worker mid-task results in the task being retried by
  the reaper within `worker_timeout` seconds. The orphaned container's
  eventual completion does not flip the reaped task to `COMPLETED`.
- `AAICLICK_TASK_TIMEOUT` enforces wall-clock limit on Docker tasks.
- A subprocess job and a Docker job can run concurrently on the same
  worker without interference.
- All existing mp_worker / subprocess tests still pass unchanged.
- New unit-test files under `aaiclick/orchestration/execution/`:
  `test_docker_worker.py`, `test_docker_build.py`,
  `test_docker_container_main.py`. New end-to-end suite at
  `test_e2e/docker/test_runner_e2e.py` (out-of-package), opt-in via
  `@pytest.mark.docker_e2e`, run by the nightly workflow and the
  release gate.
