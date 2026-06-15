Kubernetes Runner
---

The Kubernetes runner executes each task in a fresh Pod built from the user's
repo at a specific git SHA — the same model as the [Docker runner](orchestration.md),
swapping `docker run` for a Pod and the bind-mounted `result.json` for a
database result handoff that works across nodes.

It reuses the runner abstraction extracted for this work: a `TaskVehicle`
implementation driven by the shared `drive_vehicle` lifecycle.

**Implementation**: `aaiclick/orchestration/execution/worker.py` — see `TaskVehicle`
and `drive_vehicle`. ⚠️ NOT YET IMPLEMENTED — this document is the design spec; see
`docs/kubernetes_runner_implementation_plan.md` for phasing.

# Why a new result channel

The Docker runner reads the task result from a `result.json` file in a tmpdir
bind-mounted into the container (`docker_worker._read_result_or_synthesize_failure`).
That only works because the container shares the host's filesystem. A Pod may
be scheduled on a different node, so the bind mount has no equivalent.

Result transport lives entirely inside a vehicle's `wait()` / `collect()`,
never in `drive_vehicle` — so the Kubernetes vehicle keeps the same `collect()`
contract but reads the result from a database table instead of a file.

# Result handoff via `task_run_results`

The Pod writes its result payload to a new `task_run_results` table; the host
worker's `collect()` reads it back. The host — never the Pod — then writes the
terminal task status, preserving the reaper invariant (see
`docker_worker` module docstring).

```python
class TaskRunResult(SQLModel, table=True):
    __tablename__ = "task_run_results"

    task_id: int    # PK part, FK -> tasks.id
    run_epoch: int  # PK part — fences stale attempts
    success: bool
    result_ref: dict | None  # JSON; same payload serialize_task_result produces
    log_path: str | None
    error: str | None
    created_at: datetime
```

The primary key is `(task_id, run_epoch)`. `run_epoch` is the fencing token
`clear_task` bumps and the worker captures at claim time
(`Task.run_epoch`, `claiming.check_run_aborted`). The host launches the Pod
with the epoch it claimed and reads the row back at that same epoch. A
concurrent `clear_task` bumps the live epoch, so:

- the Pod's row lands under the *old* epoch — harmless, ignored;
- the host reads its own captured epoch and finds the right row, or none;
- terminal status writes stay fenced by `run_epoch` exactly as today.

!!! warning "The Pod must never write `Task.status` or `Task.run_statuses`"
    Terminal writes happen only in the host via `_handle_task_result`, or in
    the reaper via `mark_dead_workers`. The Pod writes only its own
    `task_run_results` row. Violating this reintroduces the double-write race
    the reaper invariant exists to prevent.

# The vehicle

`KubernetesVehicle` implements the six `TaskVehicle` methods; `drive_vehicle`
is reused verbatim (heartbeating, cancellation polling, terminate-on-cancel,
cancelled-overrides-result are all generic).

| Method           | Kubernetes behaviour                                                                 |
|------------------|--------------------------------------------------------------------------------------|
| `launch`         | Create a Pod running the task entrypoint; return a handle with Pod name + epoch      |
| `wait`           | Watch Pod phase until `Succeeded`/`Failed` or timeout; return `(exit_code, error)`   |
| `poll_cancelled` | `check_task_cancelled(task.id)` — reused unchanged from the Docker runner            |
| `terminate`      | Delete the Pod (cancellation / timeout path)                                         |
| `collect`        | Read the `task_run_results` row at `(task_id, run_epoch)`; synthesize failure if absent |
| `cleanup`        | Delete the Pod; always runs                                                          |

The Pod-side entrypoint mirrors `docker_worker._container_main`: boot
`orch_context`, run the task through the shared `runner.execute_task` path,
then write a `TaskRunResult` row instead of `result.json`.

# Image build is shared

Kubernetes reuses the Docker build pipeline unchanged — `docker_build.build_image`
clones the repo at the SHA, builds the image, and pushes it to a registry. A
Kubernetes job therefore **requires** `AAICLICK_DOCKER_REGISTRY` (no local-daemon
shortcut: cluster nodes pull the image by tag).

The auto-injected build task runs host-side on the subprocess runner, exactly
as for Docker (`_resolve_runner` keeps `BUILD_TASK_ENTRYPOINT` on subprocess).

# Configuration

`KubernetesJobConfig` extends the Docker git/image fields with cluster
specifics, resolved at submission time via the same three-layer precedence
(`run_job` kwarg → `RegisteredJob` default → auto-detect):

| Field                          | Source                                          |
|--------------------------------|-------------------------------------------------|
| `git_remote` / `git_sha` / `dockerfile` / `image_tag` | Shared with Docker (`resolve_docker_config`) |
| `namespace`                    | kwarg / registered default / `"default"`        |
| `service_account`              | kwarg / registered default / unset              |
| `image_pull_secret`            | kwarg / registered default / unset              |
| `resources` (cpu/mem req+lim)  | kwarg / registered default / unset              |

New `Job` / `RegisteredJob` columns carry the cluster fields; the git/image
columns already exist from the Docker runner.

# Selection and dispatch

`RunnerMode` gains `"kubernetes"`:

```python
RUNNER_KUBERNETES = "kubernetes"
RunnerMode = Literal["subprocess", "docker", "kubernetes"]
```

- `register-job --runner kubernetes` records the mode on the `RegisteredJob`.
- `run_job` branches on `runner_mode == RUNNER_KUBERNETES` to
  `resolve_kubernetes_config` + `create_kubernetes_job` (mirrors the Docker branch).
- `dispatch_execute` routes `RUNNER_KUBERNETES` tasks to the Kubernetes
  vehicle; everything else is unchanged.

# Cancellation comes for free

Because `poll_cancelled` is wired to `check_task_cancelled`, the Kubernetes
runner supports in-flight cancellation from day one — the driver deletes the
Pod when a run is aborted, identical to the Docker `docker kill` path.

# End-to-end test

`test_e2e/kubernetes/` mirrors `test_e2e/docker/`: the same `sample_job`
fixture and git-daemon publishing, driven through the `register-job` →
`run-job` CLI, polling for completion.

The reusable workflow `_kubernetes-e2e-reusable.yaml` follows
`_docker-e2e-reusable.yaml` with a minikube cluster (the `setup-minikube`
action) in place of the bare daemon, plus the same Postgres / ClickHouse /
registry services.

!!! warning "minikube networking is the main CI risk"
    In-cluster Pods must reach the registry, Postgres, and ClickHouse. Unlike
    Docker's `--add-host=host.docker.internal:host-gateway`, minikube needs
    the registry addon or explicit in-cluster service DSNs. This is the part
    of the e2e most likely to need iteration.

A `kubernetes_e2e` marker gates the suite; collection skips it unless
`kubectl cluster-info` succeeds (mirrors the `docker info` guard in
`test_e2e/docker/conftest.py`).
