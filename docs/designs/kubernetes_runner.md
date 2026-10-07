Kubernetes Runner
---

The Kubernetes runner executes each task in a fresh Pod built from the user's
repo at a specific git SHA — the same model as the [Docker runner](orchestration.md),
swapping `docker run` for a Pod. Both runners hand results back through the
shared `remote_task_results` transport.

It is a third `TaskVehicle` driven by the shared `drive_vehicle` lifecycle.

**Implementation**: `aaiclick/orchestration/execution/kubernetes_worker.py` — see
`_KubernetesVehicle` and `_run_task_in_pod`; the container-side entrypoint and
result transport live in `aaiclick/orchestration/execution/remote_result.py` —
see `remote_entry_main`; driven by the shared `drive_vehicle` lifecycle in
`aaiclick/orchestration/execution/execution_worker.py`.

Both `docker` and `kubectl` are driven through one subprocess helper,
`execution/cli.py` (`run(...)` captures; `run(..., stream=True)` tees live output,
which is what `kubectl logs -f` needs).

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

A Pod may be scheduled on a different node, so a bind-mounted file handoff
has no equivalent. Result transport lives entirely inside a vehicle's
`wait()` / `collect()`: the container writes a database row the host reads
back at the claimed epoch. The Docker runner shares this transport
(`remote_result.py`).

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

Module Pods stream stdout/stderr into the ClickHouse `task_logs` table from
inside the Pod (`capture_task_output`), so `get_task_logs` reads them on the host
with no node coordination. Shell Pods have no aaiclick harness, so the worker
runs them as a foreground `kubectl run --attach --rm` and streams the wrapper's
stdout, with `command_env` in a per-attempt Secret — the shared shell design in
`orchestration.md` "Shell entry type".

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

The Pod runs the shared container entrypoint (`remote_result.remote_entry_main`,
also used by Docker containers): boot `orch_context`, run the task through the
shared `runner.execute_task` path, then write a `RemoteTaskResult` row.

## Example: the Pod disappears mid-run

A Pod that is deleted (cancellation) or evicted never reaches `Succeeded` or
`Failed`, so `wait` cannot rely on phase alone. `_pod_status` reads kubectl's
exit code and stderr and reports the sentinel `POD_NOT_FOUND`; any other
kubectl failure reports an empty phase and is retried.

```
# poll 1 — Pod running, container not terminated yet
$ kubectl get pod aaiclick-task-7-1 -n jobs -o jsonpath='{.status.phase} {...exitCode}'
Running                                   → ("Running", -1)   keep polling

# meanwhile: cancel_job(7) → poll_cancelled → terminate → kubectl delete pod

# poll 2 — the Pod is gone
$ kubectl get pod aaiclick-task-7-1 -n jobs ...
Error from server (NotFound): pods "aaiclick-task-7-1" not found   (rc 1)
                                          → ("NotFound", -1)  wait ends:
                                            error "Pod aaiclick-task-7-1 disappeared"

# by contrast, an API blip is not the Pod's fault
$ kubectl get pod aaiclick-task-7-1 -n jobs ...
Unable to connect to the server: dial tcp: i/o timeout             (rc 1)
                                          → ("", -1)          keep polling
```

`collect` then folds the outcome: a fired cancellation wins (`"cancelled"`),
otherwise the `disappeared` error stands, since an evicted Pod wrote no result
row. Tests: `test_kubernetes_worker.py` — `test_pod_status_maps_kubectl_failure`,
`test_wait_ends_when_pod_disappears`, `test_wait_retries_transient_kubectl_failure`.

# Image build is shared

Kubernetes reuses the Docker build pipeline unchanged (`orchestration.md` "Image
source"): the injected build task builds and pushes on the worker host, and the
Pod pulls by tag. A `build` source therefore needs `AAICLICK_REGISTRY`:
`execution-worker start` rejects `AAICLICK_LOCAL_BUILD` on a kubernetes worker,
and a missing registry fails in the build task (prebuilt-only clusters need
none).

# Configuration

The runner itself is the worker's: `AAICLICK_RUNNER=kubernetes` on the worker
Deployment (`orchestration.md` "Worker runner"). Cluster settings follow it —
they describe the cluster the worker is in, are the same for every job, and are
bound by the worker's RBAC (the helm Role is namespaced), so they live only in
the worker's environment, mirroring `AAICLICK_REGISTRY` and matching Argo's
`workflowDefaults` / Airflow's `AIRFLOW__KUBERNETES__*`:

| Field               | Environment variable                               |
|---------------------|----------------------------------------------------|
| `namespace`         | `AAICLICK_K8S_NAMESPACE` (else `"default"`)        |
| `service_account`   | `AAICLICK_K8S_SERVICE_ACCOUNT`                     |
| `image_pull_secret` | `AAICLICK_K8S_IMAGE_PULL_SECRET`                   |

`resources` (requests/limits) is the one setting that is genuinely per job. It is
a nullable JSON column on both `RegisteredJob` (default) and `Job` (per-run
snapshot: `run_job` kwarg → registration default → `None`), exposed as
`resources` on the Python API, `RegisterJobRequest` and `RunJobRequest`. The
docker runner ignores it (`docs/designs/future.md`).

```python
class KubernetesConfig(NamedTuple):
    namespace: str
    service_account: str | None
    image_pull_secret: str | None
    resources: dict | None  # {cpu/mem requests+limits}
```

Resolved on the **worker at dispatch** — `resolve_pod_config(resources=job.resources)`
reads the three env vars and attaches the job's resources — and handed to the
vehicle as `JobDispatch.pod_config`.

# Selection and dispatch

`dispatch._resolve_dispatch` picks the runner from the task and the worker, never
from the job: a `NULL` `image_source` is a host subprocess; otherwise
`get_worker_runner()` names the vehicle, and `None` raises a `DispatchError`
("task declares an image_source but this worker has no `AAICLICK_RUNNER`"), which
the worker loop already turns into a failed task. The job row is read only for
`resources`.

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
