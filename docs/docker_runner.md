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
   - Capture `git_remote` (from `RegisteredJob` or
     `git config remote.origin.url`).
   - Capture `git_sha` from `git rev-parse HEAD`. Reject if working tree
     is dirty or HEAD is unpushed.
   - Compute `image_tag = f"{registry_prefix}aaiclick-job:{git_sha}"`,
     where `registry_prefix = f"{AAICLICK_DOCKER_REGISTRY}/"` if set, else
     empty.
2. Create `Job` row with `runner_mode="docker"` and the snapshotted
   `git_remote`, `git_sha`, `dockerfile`, `image_tag`.
3. Create the build task (entrypoint
   `aaiclick.orchestration.execution.docker_build.build_image`).
4. Create the entry task with the user's job entrypoint.
5. Insert dependency: `build_task >> entry_task`.
6. Commit. Submitter returns.

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
        dockerfile = os.path.join(workdir, job.dockerfile or "Dockerfile")
        await _docker_build(workdir, dockerfile, job.image_tag)

    if registry:
        await _docker_push(job.image_tag)
```

`max_retries=2` because clone / pull / push can fail transiently, and the
build is fully idempotent (tag is content-addressed by SHA).

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

| Column         | Type                       | Default              | Purpose                                            |
|----------------|----------------------------|----------------------|----------------------------------------------------|
| `runner_mode`  | `String` + CHECK           | `"subprocess"`       | `"subprocess"` or `"docker"`                       |
| `dockerfile`   | `String`                   | `NULL` → `Dockerfile`| Path to Dockerfile relative to repo root           |
| `git_remote`   | `String`                   | `NULL` → auto-detect | Override `git config remote.origin.url`            |

CHECK constraint name: `ck_registered_jobs_runner_mode`.

Following the project's `Literal` + CHECK pattern:

```python
RUNNER_SUBPROCESS = "subprocess"
RUNNER_DOCKER = "docker"
RunnerMode = Literal["subprocess", "docker"]
RUNNER_MODES: list[RunnerMode] = [RUNNER_SUBPROCESS, RUNNER_DOCKER]
```

## `jobs`

Snapshotted from `RegisteredJob` at submission for reproducibility.

| Column         | Type                       | Default        | Purpose                                  |
|----------------|----------------------------|----------------|------------------------------------------|
| `runner_mode`  | `String` + CHECK           | `"subprocess"` | Same enum as `RegisteredJob`             |
| `git_remote`   | `String`                   | `NULL`         | Resolved remote URL                      |
| `git_sha`      | `String(40)`               | `NULL`         | Resolved 40-char hex commit SHA          |
| `dockerfile`   | `String`                   | `NULL`         | Snapshotted Dockerfile path              |
| `image_tag`    | `String`                   | `NULL`         | `[registry/]aaiclick-job:<sha>`          |

All five are `NULL` for subprocess jobs and populated for Docker jobs.
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

## Per-RegisteredJob Fields

- `runner_mode` — `"subprocess"` or `"docker"`.
- `dockerfile` — path within the repo, default `Dockerfile`.
- `git_remote` — override for `git config remote.origin.url` if needed.

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

## Test File Split

| File                          | Scope                                            | Fixture           |
|-------------------------------|--------------------------------------------------|-------------------|
| `test_docker_worker.py`       | Host-side `_run_task_in_container`: dispatch, IPC, exit-code translation, cancel poll, timeout via `docker_kill`, heartbeat cadence | `orch_ctx_no_ch`  |
| `test_docker_build.py`        | Build task: cache-hit ladder (registry → local → build), push gating on `AAICLICK_DOCKER_REGISTRY`, dockerfile-not-found error, dirty-tree / unpushed-HEAD rejection at submission | `orch_ctx`        |
| `test_docker_container_main.py` | Container-side `_container_main`: orch_context boot, `execute_task` round-trip, JSON result file format, `register_returned_tasks` integration, `sys.exit` codes on success / failure | `orch_ctx_no_ch`  |
| `test_docker_runner_e2e.py`   | Real docker daemon end-to-end. Opt-in via `@pytest.mark.docker_e2e`; skipped when `which docker` fails. Not part of the PR-blocking matrix — runs in the standalone nightly workflow described below | `orch_ctx`        |

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

## Shared Test Surface

Two CI workflows run the docker e2e suite (nightly schedule + release
gate). They share **test code**, not workflow plumbing — same pattern
as the existing `test.yaml::test-dist` and `publish.yaml::test-package`
relationship: similar service-container setup in both workflows, but
each is its own job that installs aaiclick its own way and points at
the same pytest target.

Shared, lives in `aaiclick/`:

- `aaiclick/orchestration/execution/test_docker_runner_e2e.py` — the
  test code.
- `@pytest.mark.docker_e2e` — the marker each workflow filters on.
- `aaiclick/orchestration/execution/fixtures/Dockerfile.source` and
  `Dockerfile.wheel` — two install variants. The test fixture picks
  between them via `AAICLICK_E2E_WHEEL_SOURCE` (`"source"` |
  `"wheel"`).

Per-workflow:

- How the test runner process gets aaiclick (`uv sync` from source vs.
  `uv pip install` from `dist/`).
- Whether the `dist/` artifact is downloaded and bind-mounted into the
  Docker build context.
- The pytest invocation (`pytest path/...` vs. `pytest --pyargs ...`).
- `AAICLICK_E2E_WHEEL_SOURCE` env var value.

## Nightly Workflow

`.github/workflows/test-docker-nightly.yaml` — standalone, kept out of
`test.yaml` so a slow / flaky daemon-backed test never blocks PRs.

Triggers:

- `schedule: cron: "0 6 * * *"` (06:00 UTC nightly).
- `workflow_dispatch` for manual re-runs.

Runner: `ubuntu-latest`. Docker daemon ships preinstalled, same as the
existing `test-dist` job.

Service containers:

| Service      | Image                              | Purpose                                                |
|--------------|------------------------------------|--------------------------------------------------------|
| `clickhouse` | `clickhouse/clickhouse-server:26.3`| Distributed-backend CH for both host and container     |
| `postgres`   | `postgres:18.3`                    | Distributed-backend orchestration DB                   |
| `registry`   | `registry:2`                       | Local docker registry on `localhost:5000` for push/pull |

The `registry:2` service exercises the registry-cache hit,
push-after-build, and cross-host pull paths that are otherwise
untested.

Setup: `uv sync --frozen --extra distributed --extra test --python 3.10`
(matches `test-dist`).

Test step env:

- `AAICLICK_DOCKER_REGISTRY=localhost:5000`
- `AAICLICK_SQL_URL` / `AAICLICK_CH_URL` pointing at the service hosts
  via `host.docker.internal:<port>` so the launched aaiclick container
  can reach Postgres and ClickHouse the same way the spec recommends
  to operators.
- `AAICLICK_E2E_WHEEL_SOURCE=source`
- `AAICLICK_DOCKER_BUILD_GIT_REMOTE=file://${GITHUB_WORKSPACE}/.git`
  so the build task clones the checked-out repo without auth or
  external network.

Run command:

```bash
uv run pytest aaiclick/orchestration/execution/test_docker_runner_e2e.py \
  -m docker_e2e -n 0 -v --junitxml=tmp/pytest-report.xml
```

`-n 0` (no xdist) because parallel `docker build` against the same tag
serializes on the daemon anyway, and image disk usage on the runner
(~14 GB free) is the binding resource.

Cleanup step before exit: `docker system prune -af`.

## Release Gate

`publish.yaml` gains a third release-gate job alongside the existing
`test-package-local` and `test-package` (does **not** call the nightly
workflow — it's its own inlined job, shaped like the other
`test-package-*` jobs):

```yaml
test-package-docker-e2e:
  needs: build
  runs-on: ubuntu-latest
  services:    # clickhouse + postgres + registry:2 — same as nightly
    ...
  steps:
    - uses: actions/checkout@v5  # for the test fixtures (Dockerfiles)
    - uses: astral-sh/setup-uv@v7
    - run: uv venv --python 3.10
    - uses: actions/download-artifact@v7
      with: { name: dist, path: dist/ }
    - uses: actions/download-artifact@v7
      with: { name: requirements }
    - run: |
        uv pip install -r requirements-dist.txt
        uv pip install --no-deps --no-index --find-links dist/ "aaiclick[distributed]"
    - run: uv run --no-project python -m aaiclick migrate upgrade head
    - env:
        AAICLICK_E2E_WHEEL_SOURCE: wheel
        AAICLICK_DOCKER_REGISTRY: localhost:5000
        # ... CH/SQL URLs etc.
      run: >-
        uv run --no-project pytest
        --pyargs aaiclick.orchestration.execution.test_docker_runner_e2e
        -m docker_e2e -n 0 -v ...
```

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

The `wheel`-mode test fixture's `Dockerfile.wheel` does
`COPY dist/*.whl /tmp/ && pip install --no-index /tmp/*.whl`; the
fixture bind-mounts `${GITHUB_WORKSPACE}/dist` into the build context
so the same `dist` artifact the test runner installed locally is what
the container also installs.

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

1. **Where do `git_remote` and `git_sha` get captured — at `register_job`
   or at `run_job` time?** Lean: at `run_job` time so the same registered
   job can run different SHAs over time. Validate clean tree + pushed HEAD
   only at `run_job` time. Cron-scheduled runs resolve at fire time.
2. **IPC tmpdir lifecycle**: a `tempfile.TemporaryDirectory()` context in
   `_run_task_in_container` is the obvious answer. `_wait_for_container`
   returns only after `docker run` exits, so the cleanup ordering is
   naturally correct.
3. **Dockerfile path validation**: build task should validate that
   `<dockerfile>` exists in the cloned repo before invoking `docker build`,
   with a clear error like
   `"Dockerfile not found at {dockerfile} in repo {git_remote}@{git_sha}"`.
4. **`--user` controllability per-RegisteredJob**: out of scope for v1;
   document the recommendation that the Dockerfile's `USER` matches the
   host worker's UID, or the log dir is world-writable.
5. **CLI shape**: `register-job <entrypoint> --runner docker --dockerfile
   path/to/Dockerfile` hung off the existing `register-job` command. No
   new top-level commands.

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
- New test files under `aaiclick/orchestration/execution/` covering the
  matrix in the **Testability** section: `test_docker_worker.py`,
  `test_docker_build.py`, `test_docker_container_main.py`, and the
  opt-in `test_docker_runner_e2e.py`.
