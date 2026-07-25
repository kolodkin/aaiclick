Kubernetes Runner
---

The Kubernetes runner executes each task in a fresh Pod built from the user's
repo at a specific git SHA — the same model as the [Docker runner](orchestration.md),
swapping `docker run` for a Pod and the bind-mounted `result.json` for a
database result handoff.

It is a third `TaskVehicle` driven by the shared `drive_vehicle` lifecycle.

**Implementation**: `aaiclick/orchestration/execution/kubernetes_worker.py` — see
`_KubernetesVehicle`, `_run_task_in_pod`, and `_pod_main`; driven by the shared
`drive_vehicle` lifecycle in `aaiclick/orchestration/execution/execution_worker.py`.

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

# Result handoff via `remote_task_results`

A Pod may be scheduled on a different node, so Docker's bind-mounted
`result.json` has no equivalent. Result transport lives entirely inside a
vehicle's `wait()` / `collect()`, so the Kubernetes vehicle keeps the same
`collect()` contract but reads the result from a database table.

```python
class RemoteTaskResult(SQLModel, table=True):
    __tablename__ = "remote_task_results"

    task_id: int    # PK part, FK -> tasks.id
    run_epoch: int  # PK part — fences stale attempts
    success: bool
    result_ref: dict | None  # JSON; same payload serialize_task_result produces
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
    `remote_task_results` row. Violating this reintroduces the double-write race the
    reaper invariant exists to prevent.

# Logs: Pod streams to ClickHouse

`capture_task_output` streams task stdout/stderr into the ClickHouse `task_logs`
table from inside the Pod, so `get_task_logs` reads them on the host with no
node coordination — the same cross-host path every runner uses
(`docs/designs/orchestration.md` — Cross-host logs).

- **Module Pods**: `capture_task_output` flushes captured output to
  `task_logs` (keyed by `task_id` / `run_id`) before the Pod exits, reaching
  the shared ClickHouse the host also queries. No host-side fetch.
- **Shell Pods**: vanilla user images run no aaiclick harness, so the vehicle's
  `wait()` fetches `kubectl logs` text and flushes it to `task_logs` under a
  host-minted `run_id` (`_pod_logs_text` + `execution/log_flush.py`).

!!! warning "Capture shell logs before deleting the Pod"
    The `kubectl logs` fetch runs inside `wait()`, before `cleanup()` deletes
    the Pod — a deleted Pod's logs are gone.

# The vehicle

`KubernetesVehicle` implements the six `TaskVehicle` methods; `drive_vehicle` is
reused verbatim (heartbeating, cancellation polling, terminate-on-cancel,
cancelled-overrides-result are all generic).

| Method           | Kubernetes behaviour                                                              |
|------------------|-----------------------------------------------------------------------------------|
| `launch`         | `kubectl apply` a bare-Pod manifest (image_tag, env, `--task-id` / `--run-epoch`); handle = pod name + namespace + host log path |
| `wait`           | Poll Pod phase to terminal/timeout, fetch `kubectl logs` to the host file, read the `remote_task_results` row and stash it on the handle; return `(exit_code, error)` |
| `poll_cancelled` | `check_task_cancelled(task.id)` — reused unchanged from the Docker runner          |
| `terminate`      | `kubectl delete pod` (cancellation / timeout path)                                |
| `collect`        | Return the stashed result row; synthesize failure if absent; cancellation overrides |
| `cleanup`        | `kubectl delete pod` (idempotent); always runs, after logs are captured           |

The Pod-side entrypoint mirrors `docker_worker._container_main`: boot
`orch_context`, run the task through the shared `runner.execute_task` path, then
write a `RemoteTaskResult` row instead of `result.json`.

# Image build is shared

Kubernetes reuses the Docker build pipeline unchanged. The image is built
on demand at dispatch, not injected as a task at submission: the worker
calls `image_builder.ensure_image`, which claims a `BuildTask` row keyed by
image identity (deduping concurrent builders onto the same build) and runs
`docker_build.build_image_to_tag` to clone the repo at the SHA, build the
image, and push it to a registry. A Kubernetes job therefore **requires**
`AAICLICK_REGISTRY` (cluster nodes pull the image by tag).

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

Resolved at submission time alongside the shared `resolve_runner_config` for
git/image. Precedence per field (highest first): `run_job` kwarg →
`RegisteredJob` default → environment variable → hardcoded default. The env
layer holds cluster-wide deployment defaults (the same across every job),
mirroring `AAICLICK_REGISTRY` and matching Argo's `workflowDefaults` /
Airflow's `AIRFLOW__KUBERNETES__*`:

| Field | Environment variable |
| --- | --- |
| `namespace` | `AAICLICK_K8S_NAMESPACE` (else `"default"`) |
| `service_account` | `AAICLICK_K8S_SERVICE_ACCOUNT` |
| `image_pull_secret` | `AAICLICK_K8S_IMAGE_PULL_SECRET` |

`resources` has no env layer (nested JSON, not a single deployment-wide value).

# Selection and dispatch

`RunnerMode` gains `"kubernetes"`:

```python
RUNNER_KUBERNETES = "kubernetes"
RunnerMode = Literal["subprocess", "docker", "kubernetes"]
```

!!! note "`runner_mode` has no DB CHECK constraint"
    Adding `"kubernetes"` dropped the `runner_mode` CHECK constraints rather than
    widening them (Alembic can't autogenerate CHECK changes, and widening recurs
    on every new mode). `runner_mode` is validated by the `RunnerMode` Literal
    (typing) + the CLI's `choices=` (runtime). This is a scoped deviation from
    the project's String+CHECK enum convention; the other enum columns keep
    their CHECKs. A codebase-wide review of which convention to standardize on is
    tracked in `docs/designs/future.md`.

- `register-job --runner kubernetes` records the mode (plus `--namespace` and
  resource flags) on the `RegisteredJob`.
- `run_job` branches on `runner_mode == RUNNER_KUBERNETES` to
  `resolve_kubernetes_config` + `create_built_job` (mirrors the Docker branch).
- `dispatch_execute` routes `RUNNER_KUBERNETES` tasks to the Kubernetes vehicle.

In-flight cancellation works from day one: `poll_cancelled` is wired to
`check_task_cancelled`, so the driver deletes the Pod when a run is aborted,
identical to the Docker `docker kill` path.

# End-to-end test

`test_e2e/kubernetes/` mirrors `test_e2e/docker/`: the same `sample_job` fixture
and git-daemon publishing, driven through the `register-job` → `run-job` CLI,
polling for completion. The reusable workflow `_kubernetes-e2e-reusable.yaml`
follows `_docker-e2e-reusable.yaml` with a **kind** cluster (the CI-standard
lightweight Kubernetes) plus the same Postgres / ClickHouse services. A
`kubernetes_e2e` marker gates the suite; collection skips it unless
`kubectl cluster-info` succeeds.

!!! note "kind networking recipe (validated green in CI)"
    - **Registry**: a local `registry:2` on `127.0.0.1:5000` + a kind
      `containerdConfigPatches` mirror (`localhost:5000` → `http://kind-registry:5000`,
      with the registry joined to the `kind` docker network). So the image tag
      `localhost:5000/aaiclick-job:<sha>` is pushed by the host over loopback
      (trusted — no insecure-registry config) and pulled in-cluster via the
      mirror.
    - **Pod → host DBs**: route via the kind network's gateway IP (read off the
      node container; falls back to the subnet `.1`). The same IP resolves
      runner-side, so one DSN serves pods and the runner.
    - **Build → test pypi**: `host.docker.internal` + `--add-host=…:host-gateway`,
      same as the docker e2e.

    (An earlier Phase 0.5 spike validated an equivalent minikube recipe with
    `host.minikube.internal` + `--insecure-registry`; the suite ultimately uses
    kind for faster CI cold-start.)
