Kubernetes Runner
---

The Kubernetes runner executes each task in a fresh Pod built from the user's
repo at a specific git SHA — the [Docker runner](orchestration.md) model with
`docker run` swapped for a Pod. Both hand results back through the shared
`remote_task_results` transport.

Unlike the other runners, one kubernetes worker drives **many Pods at once**: a
reconciler over the namespace's task Pods, bounded by what the cluster can
schedule rather than by worker replicas, that re-adopts its Pods after a
restart.

`docker` and `kubectl` are both driven through one subprocess helper
(`aaiclick/orchestration/execution/cli.py`) that either captures output or tees
it live, which `kubectl logs -f` needs.

!!! info "Why the CLI, not `aiodocker` / `kubernetes_asyncio`"
    Neither Docker nor Kubernetes ships an *official* async client — both async
    libraries are third-party, so adopting them means two unrelated
    dependencies, not unification. The build path (`docker buildx build` on a
    remote buildkitd, secrets, `--add-host`, registry auth) is also far simpler
    via the CLI. A host that runs k8s jobs already has `kubectl`, exactly as
    docker jobs assume `docker`.

# Launch unit: a bare Pod

The worker creates a bare Pod with `restartPolicy: Never`, not a Kubernetes
`Job`. aaiclick already owns retries (`Task.max_retries` / `attempt`) and
dead-worker reaping; a `Job` controller's own backoff and re-creation would
duplicate and fight that. The worker is the sole authority on the Pod's
lifecycle.

Every Pod carries the labels `aaiclick/managed=true`, `aaiclick/job-id`,
`aaiclick/task-id` and `aaiclick/run-epoch`; the watch, the gate and restart
reclaim all key on them. The Pod kind (module / shell / jvm) comes from the
task's `entry_type`, not a label.

## Secrets owned by their Pod

The per-attempt Secret holds the DB URLs (module) or `command_env` (shell), and
the worker may not list Secrets (`rbac.yaml`: create / delete only), so no
Secret may outlive its Pod. The Pod is created first (`kubectl apply -o json`
returns its `uid`), then the Secret with an `ownerReferences` entry to that uid;
the garbage collector removes the Secret however the Pod goes — worker delete,
eviction, manual delete. Until the Secret exists the Pod waits in
`CreateContainerConfigError`; a worker crash in between leaves a Pod that never
starts, which the not-started deadline fails and deletes.

# Result handoff via `remote_task_results`

**Implementation**: `aaiclick/orchestration/execution/remote_result.py`

A Pod may land on a different node, so a bind-mounted file handoff has no
equivalent. The container writes a `remote_task_results` row — `success`,
`result_ref` (the serialized task result) or `error` — that the host reads back.
The Docker runner shares this transport.

The primary key is `(task_id, run_epoch)`. `run_epoch` is the fencing token
`clear_task` bumps and the worker captures at claim time. The host launches the
Pod with the epoch it claimed and reads the row back at that epoch. A concurrent
`clear_task` bumps the live epoch, so the Pod's row lands under the old epoch
(ignored), the host reads its own captured epoch, and terminal status writes
stay fenced as for every runner.

!!! warning "The Pod must never write `Task.status` or a terminal `task_runs` status"
    Terminal writes happen only in the host's result handler, or in the
    background worker's expired-claim release. The Pod writes only its own
    `remote_task_results` row and, for module images, the `RUNNING` `task_runs`
    row `register_run` opens. Violating this reintroduces the double-write race
    the reaper invariant exists to prevent.

# Logs: Pod streams to ClickHouse

Module Pods stream stdout/stderr into the ClickHouse `task_logs` table from
inside the Pod, so `get_task_logs` reads them on the host with no node
coordination. Shell Pods have no aaiclick harness, so the host follows `kubectl
logs -f -c task` into `task_logs` once the task container runs, with
`command_env` in the per-attempt Secret and `in.json` / `out.json` moved through
the `io-in` / `io` helper containers — see `task_interface.md`.

# The reconciler

**Implementation**: `aaiclick/orchestration/execution/k8s_worker_loop.py`

`execution-worker start` runs the reconciler when `AAICLICK_RUNNER` is
`kubernetes` (the serial mp loop otherwise). Everything runs on one asyncio
event loop; per-tick cost is fixed, not per Pod.

```
+------------------------------------------------------+
| ExecutionWorker  (AAICLICK_RUNNER=kubernetes)        |
| +-----------+   +-----------------+   +------------+ |
| | Pod watch |-->| Pod-state map   |-->| Gate       | |
| +-----------+   +-----------------+   +------------+ |
|    | futures          | wake event        | open     |
|    |        +------------------------------------+   |
|    |        | Dispatcher: reclaim, claim, spawn  |   |
|    |        +------------------------------------+   |
|    v                   | create_task                 |
| +---------------------------+ +-----------+ +------+ |
| | Module coro | Shell coro  | | Heartbeat | |Status| |
| | Host task (mp child) x 1  | | 30 s      | | 1 s  | |
| +---------------------------+ +-----------+ +------+ |
+------------------------------------------------------+
```

| Component   | Behaviour                                                                                                                                                                                                                                                                                          |
|-------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Pod watch   | One `kubectl get pods --watch-only --output-watch-events -l aaiclick/managed=true -o json` stream; updates the map, resolves per-Pod stage futures, wakes the dispatcher. On stream end: re-list, re-watch; a re-list on every heartbeat tick backstops events lost between list and stream start. |
| Pod state   | Labels, `spec.nodeName`, phase, `io-in` / `task` container states. **Done** = the `task` container terminated, or the Pod is gone — one rule for module and shell Pods (a shell Pod's `io` sidecar never exits).                                                                                   |
| Gate        | Pure function over the map, see [Capacity gate](#capacity-gate).                                                                                                                                                                                                                                   |
| Dispatcher  | Wakes on a Pod event or a 1 s tick; while the gate is open, claims (skipping capped jobs and, while one runs, host tasks) and spawns one coroutine per task. The first pass is restart reclaim.                                                                                                    |
| Module coro | Pod, then owned Secret → await done → read `remote_task_results` → delete the Pod.                                                                                                                                                                                                                 |
| Shell coro  | Stage `in.json` → Pod (+ Secret when `command_env`) → `io-in` running: `kubectl cp` → `task` running: `register_run`, follow logs → done: `kubectl exec` reads `out.json` → delete the Pod.                                                                                                        |
| Host task   | `image_source IS NULL` tasks (image builds, plain host tasks) keep the mp child, one at a time; the claim query skips them while one runs.                                                                                                                                                         |
| Heartbeat   | Every 30 s: worker heartbeat, one claim-renew UPDATE for every in-flight Pod run, then the stale-Pod sweep.                                                                                                                                                                                        |
| Status      | Every 1 s: one SELECT of each in-flight run's abort state plus its attempt's `RUNNING` `task_runs.started_at`. Drives cancellation and both deadlines.                                                                                                                                             |

A coroutine registers its Pod's futures **before** `kubectl apply`, so no event
is missed. Cancellation is delivered as `Task.cancel(reason)`: a plain cancel
(`cancel_job` / `clear_task`) deletes the Pod and reports `"cancelled"`; a
deadline reason deletes the Pod and reports that reason as the failure.

## Capacity gate

Unscheduled Pods are the backpressure signal: Kubernetes (and any autoscaler —
cluster-level, outside aaiclick's RBAC) does the capacity accounting; the worker
only stops claiming while too many of its Pods wait for a node. A Pod is
unscheduled when it has no `spec.nodeName`; Pod tasks the watch has not reported
yet count as unscheduled too, so a burst of claims within one tick cannot
overshoot.

| Setting                 | Env var                          | Default           | Effect                                                |
|-------------------------|----------------------------------|-------------------|-------------------------------------------------------|
| `max_pending` (per job) | `AAICLICK_K8S_MAX_PENDING`       | 10                | A job at the cap is skipped by the claim; others flow |
| `max_pending_total`     | `AAICLICK_K8S_MAX_PENDING_TOTAL` | 4 × `max_pending` | Claiming pauses                                       |
| `max_pods`              | `AAICLICK_K8S_MAX_PODS`          | unset             | Optional cap on live (not done) Pods                  |
| `pending_timeout`       | `AAICLICK_K8S_PENDING_TIMEOUT`   | 900 s             | Not-started deadline, see [Deadlines](#deadlines)     |

Counts are namespace-wide, so with several worker replicas the limits —
`max_pods` included — are shared, not per replica. A `ResourceQuota` rejection
of `kubectl apply` (`exceeded quota`) releases the claim back to `PENDING` under
a new epoch, with no attempt consumed. The task's `retry_after` keeps every
replica off it for 30 s, and the refused worker claims nothing meanwhile.

## Deadlines

Both deadlines come from the aaiclick DB only — no Kubernetes timestamps, no
worker-local launch time — so they survive a restart and match what the UI
shows. A task counts as **started** once the attempt's `RUNNING` `task_runs`
row exists: `register_run` inside a module Pod, or the host's `register_run`
when a shell Pod's `task` container starts.

| Phase       | Deadline                              | On expiry                                                                                                                                                        |
|-------------|---------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Not started | `tasks.started_at + pending_timeout`  | Delete the Pod; fail with the scheduler message or container waiting reason (`ImagePullBackOff: …`), or "Pod running but the task never started"; normal retries |
| Started     | `task_runs.started_at + task timeout` | Delete the Pod; fail through normal retries                                                                                                                      |

One rule covers "never fits" and stuck image pulls: no event parsing, no message
classification, no extra RBAC. A doomed task costs `pending_timeout ×
(max_retries + 1)` before it fails for good; the error carries the reason.
Fail-fast was rejected because Kubernetes documents no reason meaning
"permanent": `ImagePullBackOff` covers transient and permanent causes,
`InvalidImageName` / `ErrImagePull` exist only in kubelet source, the
autoscaler's `NotTriggerScaleUp` also fires on "max size reached", and OCI
registry codes reach the Pod only as free text.

## Ownership and restart

Ownership is `execution_worker_id` + `run_epoch`, as for every worker. A Pod
coroutine is **detached** before its cancel whenever the worker lets go of a
running Pod without finishing it; a detached coroutine deletes nothing.

- **Shutdown** (`SIGTERM`, `execution-worker stop`): stop claiming, detach every
  Pod coroutine, finish host tasks, deregister. The Pods keep running.
- **Heartbeat outage**: 1.5 heartbeat intervals of silence (failed calls are
  retried at a quarter interval, each bounded by it) detach every Pod coroutine
  — before `ADOPT_AFTER`, so no adopter overlaps a live driver. The next good
  heartbeat re-lists and takes the worker's own Pods back unless another worker
  adopted them; a bounded worker (`--max-tasks`) stays up for that even with
  nothing in flight. This replaces `SELF_STOP_AFTER` on kubernetes.
- **Shutdown before the Pod exists**: a detached coroutine whose Pod never
  reached the cluster releases the claim, so a rollout costs no attempt.
- **One cancel per coroutine**: deadlines and aborts cancel a coroutine once;
  its cleanup (`kubectl delete --wait=false`, log drain) runs shielded, so a
  result is never lost to a second cancel.
- **Lost ownership**: a claim-renew row that did not match is re-read — epoch
  changed (cleared / reaped) → cancel and delete the Pod; unchanged (adopted) →
  detach.
- **Adoption**: a worker may take a run whose owner is `STOPPED` or silent for
  `ADOPT_AFTER` (2 × `HEARTBEAT_INTERVAL`); adoption is fenced on the previous
  owner and the epoch, so two restarting replicas cannot both win.

On start (and after an outage) the worker lists the managed Pods, loads their
tasks and settles each one; the same pass without adoption runs on every
heartbeat tick as the **stale-Pod sweep**, so Pods of tasks reaped or cleared
during a long outage do not hold capacity:

| Task row                                  | Action                                                         |
|-------------------------------------------|----------------------------------------------------------------|
| `RUNNING`, epoch matches, owner gone      | Adopt → resume the coroutine at the Pod's stage (reclaim only) |
| Cancelling, epoch matches, owner gone     | Delete the Pod, release the cancelled run                      |
| Missing, epoch stale, or any other status | Delete the Pod                                                 |
| Owner alive                               | Leave                                                          |

An adopted module Pod is awaited where it is. An adopted shell Pod re-stages
`in.json` from the DB if `io-in` still waits, reuses the open `task_runs` row
and resumes the log follower with `--since-time` set to the last stored line
(second precision, so that line may repeat — log delivery across a restart is
at-least-once), or reads `out.json` if it is already done. Deadlines are
unchanged by adoption. An outage longer than `WORKER_TIMEOUT` (10 min) still
falls back to claim expiry and a retry; Pod-side claim renewal is in
`future.md`.

# Image build is shared

Kubernetes reuses the Docker build pipeline unchanged (`orchestration.md` "Image
source"): the injected build task builds and pushes on the worker host, and the
Pod pulls by tag. A `build` source therefore needs `AAICLICK_REGISTRY`:
`execution-worker start` rejects `AAICLICK_LOCAL_BUILD` on a kubernetes worker,
and a missing registry fails in the build task (prebuilt-only clusters need
none).

# Configuration

**Implementation**: `aaiclick/orchestration/kubernetes_config.py`

The runner itself is the worker's: `AAICLICK_RUNNER=kubernetes` on the worker
Deployment (`orchestration.md` "Worker runner"). Cluster settings follow it —
they describe the cluster the worker is in, are the same for every job, and are
bound by the worker's RBAC (the helm Role is namespaced), so they live only in
the worker's environment, mirroring `AAICLICK_REGISTRY` and matching Argo's
`workflowDefaults` / Airflow's `AIRFLOW__KUBERNETES__*`:

| Field               | Environment variable                        |
|---------------------|---------------------------------------------|
| `namespace`         | `AAICLICK_K8S_NAMESPACE` (else `"default"`) |
| `service_account`   | `AAICLICK_K8S_SERVICE_ACCOUNT`              |
| `image_pull_secret` | `AAICLICK_K8S_IMAGE_PULL_SECRET`            |

The concurrency limits (`AAICLICK_K8S_MAX_PENDING`, `…_MAX_PENDING_TOTAL`,
`…_MAX_PODS`, `…_PENDING_TIMEOUT`) sit beside them, see
[Capacity gate](#capacity-gate).

`resources` is the one per-job setting: a nullable JSON column
on `RegisteredJob` (default) and `Job` (per-run snapshot: `run_job` kwarg →
registration default → `None`), exposed on the Python API, `RegisterJobRequest`
and `RunJobRequest`, and checked to be a mapping at submission. It rides on the
job's dispatch record, not the Pod config, and is runner-shaped: the Pod
container `resources` here, `docker run` flags on docker
([Resources](../user_guide/orchestration.md#resources)).

Both are resolved on the **worker at dispatch** — the runner is picked from the
task and the worker, never from the job (a `NULL` `image_source` is a host
subprocess) — and handed to the Pod coroutines.

# End-to-end test

`test_e2e/kubernetes/` mirrors `test_e2e/docker/`: the same `sample_job` fixture
and git-daemon publishing, driven through the `register-job` → `run-job` CLI,
with the reconciler running in-process. The reusable workflow
`_kubernetes-e2e-reusable.yaml` follows `_docker-e2e-reusable.yaml` with a
**kind** cluster (the CI-standard lightweight Kubernetes) plus the same
Postgres / ClickHouse services. A `kubernetes_e2e` marker gates the suite;
collection skips it unless `kubectl cluster-info` succeeds.

Three tests cover the concurrent worker: a fan-out whose peak number of live
Pods, sampled while it runs, is above one; a worker stopped mid-run whose module
and shell Pods the next worker adopts (one attempt each, shell log intact across
the restart); and a job no node can fit that fails on the not-started deadline
while another job completes beside it.

!!! note "kind networking recipe (validated green in CI)"
    - **Registry**: a local `registry:2` on `127.0.0.1:5000` + a kind
      `containerdConfigPatches` mirror (`localhost:5000` →
      `http://kind-registry:5000`, with the registry joined to the `kind` docker
      network). The image tag `localhost:5000/aaiclick-job:<sha>[-<dockerfile>]`
      is pushed by a host-network buildkitd over loopback (trusted — no
      insecure-registry config) and pulled in-cluster via the mirror.
    - **Pod → host DBs**: route via the kind network's gateway IP (read off the
      node container; falls back to the subnet `.1`). The same IP resolves
      runner-side, so one DSN serves pods and the runner.
    - **Build → test pypi**: `host.docker.internal` + `--add-host=…:host-gateway`,
      same as the docker e2e.
