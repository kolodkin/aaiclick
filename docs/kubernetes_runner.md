Kubernetes Runner
---

The Kubernetes runner executes each task in a fresh Pod built from the user's
repo at a specific git SHA — the same model as the [Docker runner](orchestration.md),
swapping `docker run` for a Pod, the bind-mounted `result.json` for a database
result handoff, and the bind-mounted log file for streamed `kubectl logs`.

It is a third `TaskVehicle` driven by the shared `drive_vehicle` lifecycle.

**Implementation**: `aaiclick/orchestration/execution/worker.py` — see `TaskVehicle`
and `drive_vehicle`. ⚠️ NOT YET IMPLEMENTED — this is the design spec; see
`docs/kubernetes_runner_implementation_plan.md` for phasing.

# One CLI primitive for docker and kubectl

Both `docker` and `kubectl` are external CLIs driven via `asyncio.create_subprocess_exec`.
The Docker runner already duplicates this plumbing — a capture helper in
`docker_worker`, a streaming helper in `docker_build`, and an ad-hoc call in
`docker_config`. Extract one `execution/cli.py` with two modes:

- `run(...)` — capture stdout/stderr, raise a consistent error on nonzero exit.
- `run(..., stream=True)` — tee output live to a sink while capturing (the
  existing `docker_build._stream_to_stdio` behaviour).

`docker_worker`, `docker_build`, `docker_config`, and the Kubernetes vehicle
all sit on it. The `stream=True` mode is exactly what `kubectl logs -f` needs.

!!! info "Why the CLI, not `aiodocker` / `kubernetes_asyncio`"
    Neither Docker nor Kubernetes ships an *official* async client — both async
    libraries are third-party, so adopting them means two unrelated
    dependencies, not unification. The build path (`docker build` with BuildKit,
    build-args, `--add-host`, registry auth) is also far simpler via the CLI. If
    a host runs k8s jobs it already has `kubectl`, exactly as docker jobs assume
    `docker`.

# Launch unit: a bare Pod

The vehicle creates a bare Pod with `restartPolicy: Never`, not a Kubernetes
`Job` object. aaiclick already owns retries (`Task.max_retries` / `attempt`)
and dead-worker reaping; a `Job` controller's own backoff and re-creation would
duplicate and fight that. The vehicle is the sole authority on the Pod's
lifecycle.

# Result handoff via `task_run_results`

A Pod may be scheduled on a different node, so Docker's bind-mounted
`result.json` has no equivalent. Result transport lives entirely inside a
vehicle's `wait()` / `collect()`, so the Kubernetes vehicle keeps the same
`collect()` contract but reads the result from a database table.

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
`clear_task` bumps and the worker captures at claim time (`Task.run_epoch`,
`claiming.check_run_aborted`). The host launches the Pod with the epoch it
claimed and reads the row back at that epoch. A concurrent `clear_task` bumps
the live epoch, so the Pod's row lands under the old epoch (ignored), the host
reads its own captured epoch, and terminal status writes stay fenced exactly as
today.

!!! warning "The Pod must never write `Task.status` or `Task.run_statuses`"
    Terminal writes happen only in the host via `_handle_task_result`, or in the
    reaper via `mark_dead_workers`. The Pod writes only its own
    `task_run_results` row. Violating this reintroduces the double-write race the
    reaper invariant exists to prevent.

# Logs: Pod stdout, host streams and reconciles

`capture_task_output` redirects task stdout/stderr into a file; Docker works
only because that file lands on a bind mount. K8s breaks this twice: the file
dies with the Pod, and `kubectl logs` reads the container's **stdout**, not a
file inside it. So:

- **Pod side**: `_pod_main` lets task output reach stdout (a tee in
  `capture_task_output`, gated by an env flag so subprocess and docker behaviour
  are untouched).
- **Host side**: while the Pod runs, the vehicle's `wait()` streams
  `kubectl logs -f` into a temp file (live tail, via the `cli.py` stream mode).
  On terminal phase it does one authoritative `kubectl logs` (no `-f`) to
  guarantee completeness even if the live stream dropped lines, then relocates
  the temp file to the canonical `{log_base}/{job_id}/{task_id}/{run_id}.log`
  using the `run_id` from the result row, and reports that as `log_path`.

!!! warning "Capture logs before deleting the Pod"
    The authoritative `kubectl logs` fetch runs inside `wait()`, before
    `cleanup()` deletes the Pod — a deleted Pod's logs are gone.

# The vehicle

`KubernetesVehicle` implements the six `TaskVehicle` methods; `drive_vehicle` is
reused verbatim (heartbeating, cancellation polling, terminate-on-cancel,
cancelled-overrides-result are all generic).

| Method           | Kubernetes behaviour                                                              |
|------------------|-----------------------------------------------------------------------------------|
| `launch`         | Create a Pod (image_tag, env, `--task-id` / `--run-epoch`); handle = pod name + epoch + temp-log path |
| `wait`           | Watch Pod phase to terminal/timeout **and** stream + reconcile logs; return `(exit_code, error)` |
| `poll_cancelled` | `check_task_cancelled(task.id)` — reused unchanged from the Docker runner          |
| `terminate`      | `kubectl delete pod --now` (cancellation / timeout path)                          |
| `collect`        | Read the `task_run_results` row at `(task_id, run_epoch)`; synthesize failure if absent |
| `cleanup`        | `kubectl delete pod` (idempotent); always runs, after logs are captured           |

The Pod-side entrypoint mirrors `docker_worker._container_main`: boot
`orch_context`, run the task through the shared `runner.execute_task` path, then
write a `TaskRunResult` row instead of `result.json`.

# Image build is shared

Kubernetes reuses the Docker build pipeline unchanged —
`docker_build.build_image` clones the repo at the SHA, builds the image, and
pushes it to a registry. A Kubernetes job therefore **requires** `AAICLICK_REGISTRY`
(cluster nodes pull the image by tag). The auto-injected build task runs
host-side on the subprocess runner, exactly as for Docker (`_resolve_runner`
keeps `BUILD_TASK_ENTRYPOINT` on subprocess).

!!! note "`AAICLICK_DOCKER_REGISTRY` → `AAICLICK_REGISTRY`"
    The registry is the one `AAICLICK_DOCKER_*` setting both runners share (k8s
    reuses the docker build), so it is renamed to the neutral `AAICLICK_REGISTRY`.
    The remaining `AAICLICK_DOCKER_*` vars stay docker-specific. Renamed with the
    shared CLI primitive in Phase 1.

# Configuration

Kubernetes reuses the Docker git/image fields and adds cluster specifics in a
single nullable `kubernetes_config` JSON column on `Job` / `RegisteredJob` —
these fields are k8s-only and never queried, so one column keeps the migration
small and naturally holds the nested `resources`.

```python
class KubernetesConfig(NamedTuple):
    namespace: str = "default"
    service_account: str | None = None
    image_pull_secret: str | None = None
    resources: dict | None = None  # {cpu/mem requests+limits}
```

Resolved at submission time via the same three-layer precedence as Docker
(`run_job` kwarg → `RegisteredJob` default → default), alongside the shared
`resolve_docker_config` for git/image.

# Selection and dispatch

`RunnerMode` gains `"kubernetes"`:

```python
RUNNER_KUBERNETES = "kubernetes"
RunnerMode = Literal["subprocess", "docker", "kubernetes"]
```

- `register-job --runner kubernetes` records the mode (plus `--namespace` and
  resource flags) on the `RegisteredJob`.
- `run_job` branches on `runner_mode == RUNNER_KUBERNETES` to
  `resolve_kubernetes_config` + `create_kubernetes_job` (mirrors the Docker branch).
- `dispatch_execute` routes `RUNNER_KUBERNETES` tasks to the Kubernetes vehicle.

In-flight cancellation works from day one: `poll_cancelled` is wired to
`check_task_cancelled`, so the driver deletes the Pod when a run is aborted,
identical to the Docker `docker kill` path.

# End-to-end test

`test_e2e/kubernetes/` mirrors `test_e2e/docker/`: the same `sample_job` fixture
and git-daemon publishing, driven through the `register-job` → `run-job` CLI,
polling for completion. The reusable workflow `_kubernetes-e2e-reusable.yaml`
follows `_docker-e2e-reusable.yaml` with a minikube cluster (the
`setup-minikube` action) in place of the bare daemon, plus the same Postgres /
ClickHouse / registry services. A `kubernetes_e2e` marker gates the suite;
collection skips it unless `kubectl cluster-info` succeeds.

!!! warning "minikube networking is the main CI risk"
    In-cluster Pods must reach the registry, Postgres, and ClickHouse. Unlike
    Docker's `--add-host=host.docker.internal:host-gateway`, minikube needs the
    registry addon or explicit in-cluster service DSNs. This is the part of the
    e2e most likely to need iteration; prototype it before wiring the full test.
