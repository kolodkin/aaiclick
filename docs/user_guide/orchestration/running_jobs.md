Running Jobs
---

# Running Jobs

Every action is available from three surfaces: Python (inside
`async with orch_context():`), the CLI, and the REST API served by
`local start` — default `http://127.0.0.1:5255`, interactive docs at
`/api/v0/docs`. The MCP server exposes the same operations. In distributed
mode the mutating REST endpoints require admin scope, from a session or an API token.

## Run a job

<!-- req: run-job -->
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

    <!-- req: run-job-set-kwargs -->
    `--set` beats `--kwargs`, which beats the registration's `default_kwargs`.
    A value that is not valid JSON stays a string.

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/jobs:run \
      -H "Content-Type: application/json" \
      -d '{"name": "crawl", "kwargs": {"url": "https://example.com"}}'
    ```

    <!-- req: run-job-rest-name -->
    A dotted `name` (e.g. `"myapp.pipelines.crawl"`) is used as the entrypoint
    directly; a bare name reuses the registered job's entrypoint.

All three surfaces also accept `preservation_mode` and the runner fields —
`entry_type` / `command` / `command_env` (see [Shell tasks](runners.md#shell-tasks)) and
`image` / `git_*` / `dockerfile` (see [Image source](runners.md#image-source-docker-kubernetes)).

## Wait for completion

<!-- req: job-wait -->
`run-job` returns as soon as the job is queued. `--progress` blocks until it
reaches a terminal status; `job wait` does the same for an already-submitted job,
by id or name:

```bash
python -m aaiclick run-job crawl --set url=https://example.com --progress
python -m aaiclick job wait crawl --timeout 900
```

<!-- req: job-wait-exit-status -->
Both re-print the task table whenever the status counts change and exit
**non-zero** on failure, cancellation, or timeout (`--timeout`, default
`AAICLICK_JOB_WAIT_TIMEOUT`, 3600 s — a timeout stops watching while the job
keeps running), so `set -e` scripts and CI stop on a failed run. Failures print the failing task's full error
and its `task get` command; `--json` emits the final stats as one parseable
document, with diagnostics on stderr.

<!-- req: wait-dotted-run-by-name -->
The wait follows the returned job id, so an unregistered dotted entrypoint
blocks and reports the same way.

```bash
python -m aaiclick run-job myapp.pipelines.crawl --set depth=3 --progress

# A dotted run is named after the last segment, so it is waitable by name too:
python -m aaiclick job wait crawl
```

!!! warning "A bare name that was never registered is not an entrypoint"
    <!-- req: bare-name-needs-registration -->
    With no `crawl` registration, `run-job crawl` is rejected before anything
    is queued: `No registered job named 'crawl'; register it or pass a dotted
    entrypoint`.

## Register a job

<!-- req: register-job -->
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

<!-- req: register-job-runner -->
Where a job runs is set here too: nothing → a subprocess on the worker;
`--image python:3.12` → a container from that image; `--build` → a container
built from your repo at the submitted commit (`--git-remote` / `--dockerfile`
modify it, `--git-sha` per run). Python and REST use `image` / `build`.
Container vs Pod is the worker's `AAICLICK_RUNNER` — see
[Deployment](../deployment.md).

<!-- req: job-name-rule -->
Job names follow the object name rule: letters, digits and `_`, not starting
with a digit, at most 128 characters. Others fail with HTTP 422; names
registered before the rule keep running.

<!-- req: register-job-replace -->
Registering an existing name fails (HTTP 409). Pass `--replace` (Python
`replace=True`, REST/MCP `"replace": true`) to overwrite it — e.g. after a
signature change leaves stale `default_kwargs`. Every field is replaced: omitting `--schedule`
clears the schedule, and `enabled` resets to true.

## Enable, disable, unregister, list registrations

<!-- req: manage-registrations -->
=== "Python"

    ```python
    from aaiclick.orchestration import orch_context
    from aaiclick.orchestration.registered_jobs import (
        disable_job,
        enable_job,
        list_registered_jobs,
        unregister_job,
    )

    async with orch_context():
        await enable_job("crawl")
        await disable_job("crawl")
        registrations = await list_registered_jobs(enabled_only=True)
        await unregister_job("crawl")  # past runs stay, unlinked
    ```

=== "CLI"

    ```bash
    python -m aaiclick job enable crawl
    python -m aaiclick job disable crawl
    python -m aaiclick job unregister crawl
    python -m aaiclick registered-job list
    ```

=== "REST"

    ```bash
    curl -X POST http://127.0.0.1:5255/api/v0/registered-jobs/crawl/enable
    curl -X POST http://127.0.0.1:5255/api/v0/registered-jobs/crawl/disable
    curl -X DELETE http://127.0.0.1:5255/api/v0/registered-jobs/crawl
    curl http://127.0.0.1:5255/api/v0/registered-jobs
    ```

## Cron scheduling

<!-- req: cron-scheduling -->
The background worker launches a registered job with a `schedule` (standard
cron expression) and `enabled=True` when its next run is due, using the
registration's `default_kwargs` and defaults; manual `run_job()` calls can
override them per run.

## Preservation modes

<!-- req: preservation-modes -->
A `Job`'s `preservation_mode` controls which tables survive cleanup after it
completes; task execution is identical in both modes.

| Mode         | What survives after job              | Use case                       |
|--------------|--------------------------------------|--------------------------------|
| `NONE`       | Persistent tables only (default)     | Production runs                |
| `FULL`       | All tables until the job TTL expires | Development / debugging        |

Set the default with `AAICLICK_DEFAULT_PRESERVATION_MODE` (`NONE` or `FULL`);
override it per submission:

```python
from aaiclick.orchestration import PRESERVATION_FULL
from aaiclick.orchestration.registered_jobs import run_job

await run_job(
    "debug_run",
    "myapp.pipelines.crawl",
    preservation_mode=PRESERVATION_FULL,
)
```

The effective mode resolves by precedence:

<!-- req: preservation-mode-precedence -->
| Level | Source                                       | Wins when                            |
|-------|----------------------------------------------|--------------------------------------|
| 1     | Explicit `run_job(...)` argument             | The caller passes a non-`None` value |
| 2     | Registered job's `preservation_mode`         | The registration carries a default   |
| 3     | `AAICLICK_DEFAULT_PRESERVATION_MODE` env var | Set in environment                   |
| 4     | `"NONE"`                                     | Hardcoded fallback                   |
