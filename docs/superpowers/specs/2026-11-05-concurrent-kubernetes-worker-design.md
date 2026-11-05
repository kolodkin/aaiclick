Concurrent Kubernetes Worker
---

# Intent

On Kubernetes the execution worker runs one task at a time: claim → launch Pod
→ poll until terminal → next claim. Concurrency equals worker replicas, and each
replica mostly waits. Concurrency should instead be bounded by what the cluster
can schedule (node pools, autoscaler, `ResourceQuota`), not by how many workers
are deployed.

One execution worker in a **concurrent Kubernetes mode** drives many Pods at
once, stops claiming when the cluster is out of room, and re-adopts its Pods
after a restart.

# Goals

- One worker replica drives as many concurrent Pods as the cluster schedules.
- Capacity backpressure from Pending Pods works the same with cluster-autoscaler,
  Karpenter, or fixed node pools.
- A job waiting on capacity (e.g. big nodes) does not block other jobs.
- Pods keep running through a worker restart; the restarted worker picks them up.
- Fixed per-worker cost per poll tick, not per-Pod: one Pod watch, one heartbeat,
  one cancellation query.

# Non-goals

- Docker runner concurrency — stays one task at a time.
- Local / mp workers — unchanged.
- Surviving a worker outage longer than `WORKER_TIMEOUT` (10 min). Beyond that,
  today's fail-and-retry stands. Pod-side claim renewal goes to `future.md`.
- Installing or configuring an autoscaler — cluster-level, outside aaiclick's RBAC.
- Node-pool targeting (`nodeSelector` / tolerations in `pod_config`).
- Active/standby HA for the worker.

# Anti-goals

- The Pod writing `Task.status` or a terminal `task_runs` row (reaper invariant,
  `kubernetes_runner.md`).
- Per-Pod polling loops that multiply `kubectl` processes or DB queries by Pod count.
- Failing tasks because the cluster is temporarily full.
- Moving Pod supervision into the background worker.

# Constraints (current code)

- Loop: `execution_worker.py:_execution_worker_loop` awaits `execute_fn` serially.
- Per-task lifecycle: `drive_vehicle` runs a per-task heartbeat
  (`renew_claim`, 30 s), per-task cancellation poll (`check_run_aborted`, 1 s),
  and `_KubernetesVehicle.wait` spawns `kubectl get pod` every `POLL_INTERVAL` (1 s).
- Claims: `Task.claim_expires_at` renewed by the owner; `release_expired_claims`
  fails the run and bumps `run_epoch` once it lapses (`WORKER_TIMEOUT` = 10 min).
- Pods: bare Pod `aaiclick-task-<task_id>-<run_epoch>` + same-named per-attempt
  Secret; result via `remote_task_results` at `(task_id, run_epoch)`.
- Module Pods write their own `task_logs`; the host follows `kubectl logs -f` only
  for shell Pods (and the bootstrap-failure rescue in `_collect_unfollowed_output`).
- A kubernetes worker also runs `image_source=NULL` tasks (injected image-build
  tasks, plain host tasks) as host subprocesses. Builds execute on buildkitd;
  the worker runs only the client (clone, scaffold, `buildx`, push).

# Approaches considered

| Approach                                   | Trade-off                                                                       | Verdict   |
|--------------------------------------------|---------------------------------------------------------------------------------|-----------|
| Supervisor inside the background worker    | One fewer Deployment; couples cleanup and supervision failures; cleanup gains Pod RBAC + registry/git creds; k8s-only branch in a process every backend runs | Rejected |
| **Execution worker, concurrent k8s mode**  | Reuses claiming, `renew_claim`, `remote_task_results`, worker SA/Role unchanged; adds a shared Pod watch and batched DB polls | **Chosen** |
| New standalone supervisor                  | Cleanest single-watch reconciler; largest rewrite, duplicates claiming          | Rejected  |

Within the chosen approach, capacity gating: global gate (head-of-line blocking),
per-resource-shape gate (resource-dict normalization, claim join on
`jobs.resources`), **per-job gate** (chosen — `resources` lives on the job, so a job
is effectively a shape; one claim-query filter).

Restart survival: **re-adopt within the claim window** (chosen); Pod-side claim
renewal (deferred to `future.md`; shell Pods need a sidecar); reaper querying
Kubernetes (rejected — gives the background worker cluster RBAC).

# Design

## Mode selection

Concurrent mode is on when `AAICLICK_RUNNER=kubernetes`. `execution-worker start`
runs a concurrent loop instead of `_execution_worker_loop`. Helm keeps the
worker Deployment, default `replicas: 1`.

## Shared machinery (one per worker)

- **Pod watch** — one long-lived
  `kubectl get pods -w -n <ns> -l aaiclick/managed=true -o json` stream
  (via `execution/cli.py` streaming), feeding an in-memory `pod name → state`
  map. Each task's wait awaits an event for its own Pod instead of polling.
  The watch restarts on stream end with a fresh `list` (resync).
- **Batched heartbeat** — every `HEARTBEAT_INTERVAL`: worker heartbeat plus one
  `UPDATE tasks SET claim_expires_at = … WHERE execution_worker_id = :me AND
  (id, run_epoch) IN (in-flight)`. Rows that didn't match are runs this worker
  no longer owns. Re-read their `run_epoch`: changed (cleared / reaped) → delete
  the Pod as today; unchanged (adopted by another worker) → stop tracking, **never
  delete**.
- **Batched cancellation poll** — every `POLL_INTERVAL`: one query returning which
  in-flight `(task_id, run_epoch)` are aborted (the `check_run_aborted` predicate
  over a set). Each hit fires that task's cancel → `kubectl delete`.

## Per-task work

Each claimed Pod task runs as an asyncio task: launch (Secret + Pod), await
terminal state from the watch, read `remote_task_results`, `_handle_task_result`,
cleanup. Cancellation override, result fencing, timeout and shell-Pod log
following keep their current semantics. `drive_vehicle` is refactored so its
heartbeat / cancel-watch / wait inputs can come from the shared machinery
(kubernetes vehicle) or its own loops (docker, unchanged).

## Pod labels

Every Pod (and Secret) carries:

| Label                | Value          |
|----------------------|----------------|
| `aaiclick/managed`   | `"true"`       |
| `aaiclick/job-id`    | `job_id`       |
| `aaiclick/task-id`   | `task_id`      |
| `aaiclick/run-epoch` | `run_epoch`    |

## Capacity gate

A Pod is **unscheduled** when it has no `spec.nodeName` (covers just-created Pods
the scheduler hasn't marked yet, so a burst of claims cannot overshoot).

Counts are namespace-wide over `aaiclick/managed=true` Pods (my call: the
default is one replica; with more, the limits are shared, not per replica).
Before each claim the worker computes, from the watch map:

- `blocked_jobs` = jobs with ≥ `max_pending` unscheduled Pods.
- If total unscheduled ≥ `max_pending_total`, or running Pods ≥ `max_pods`
  (when set), claim nothing this tick.
- Otherwise claim with `job_id NOT IN blocked_jobs`
  (`claim_next_task` gains an optional exclusion set; the handler SQL adds the filter).

| Setting                          | Env var                              | Default                |
|----------------------------------|--------------------------------------|------------------------|
| `max_pending` (per job)          | `AAICLICK_K8S_MAX_PENDING`           | 10                     |
| `max_pending_total`              | `AAICLICK_K8S_MAX_PENDING_TOTAL`     | 4 × `max_pending`      |
| `max_pods` (running, optional)   | `AAICLICK_K8S_MAX_PODS`              | unset (no cap)         |
| `pending_timeout`                | `AAICLICK_K8S_PENDING_TIMEOUT`       | 15 min                 |

Parsed in `kubernetes_config.py` next to the existing `AAICLICK_K8S_*` fields.

## Pod-state handling

| Pod state                                                     | Action                                                                   |
|---------------------------------------------------------------|--------------------------------------------------------------------------|
| Unscheduled, `NotTriggerScaleUp` event (cluster-autoscaler)   | Fail the task with the event message                                     |
| Unscheduled longer than `pending_timeout`                     | Fail the task with the scheduler's `Unschedulable` message               |
| `kubectl create` rejected: `exceeded quota`                   | Release the claim back to `PENDING` (no attempt consumed), back off claiming |
| Scheduled, container waiting `ImagePullBackOff` / `ErrImagePull` / `CreateContainerConfigError` / `InvalidImageName` | Fail the task with the reason/message |
| Pod gone mid-run (eviction, preemption, node loss)            | Existing "Pod disappeared" path → normal retry                           |
| `Succeeded` / `Failed`                                        | Read `remote_task_results`, existing collect                             |

`NotTriggerScaleUp` is read from Pod events (needs `events` `list/watch` in the
Role). The task timeout (`parse_task_timeout`) starts when the Pod gets a
`spec.nodeName`, not at launch.

## Restart reclaim

On start (and before the first claim), the worker lists `aaiclick/managed=true`
Pods and Secrets and joins them with tasks in `RUNNING` / cancelling states:

| Found                                                                 | Action                                                     |
|-----------------------------------------------------------------------|------------------------------------------------------------|
| Live Pod, task `RUNNING`, label epoch == `run_epoch`, owner adoptable | Move claim to this worker (`execution_worker_id`, renew `claim_expires_at`, epoch-fenced UPDATE), resume as an in-flight task |
| Pod terminal, epoch matches                                           | Adopt, read `remote_task_results`, finish normally         |
| Label epoch ≠ `run_epoch`, or task terminal / not found               | Delete Pod + Secret                                        |
| Orphan Secret with no Pod                                             | Delete                                                     |

Reclaim starts from labeled Pods only, so docker / local / mp tasks are never
touched. A `RUNNING` task whose Pod is gone is left to claim expiry
(`release_expired_claims`, today's path) — no new release logic, no leaked
`task_runs` / `table_run_refs`.

**Adoptable owner** (my call): `STOPPED`, or `last_heartbeat` older than
2 × `HEARTBEAT_INTERVAL`. A live replica that briefly lost the DB and finds its
runs adopted just stops tracking them (see batched heartbeat) — no Pod is killed.

Gate counts and the watch map are rebuilt from the same list; no state is persisted.

## Shutdown

`SIGTERM` (rollout, node drain): stop claiming, stop watches, deregister —
**do not delete running Pods**. The next worker adopts them. `execution-worker
stop` (`STOPPING`) behaves the same. A worker marked `STOPPED` by the reaper
(heartbeat lost) stops tracking its tasks; it never deletes Pods it no longer owns.

## Host-subprocess tasks

`image_source=NULL` tasks claimed by this worker (image-build, plain host tasks)
run as asyncio tasks alongside Pod tasks, capped at **one at a time** (my
call: builds are thin buildkitd clients, and serial host tasks keep today's
shared cwd/env behaviour). While one runs, the claim query excludes
`image_source IS NULL` tasks.

## Docs and tests

- `docs/designs/kubernetes_runner.md`: replace the per-task `wait` polling
  description; add capacity gate, Pod-state table, restart reclaim, shutdown.
- `docs/user_guide/deployment.md` / Helm values: new env vars, `events` RBAC.
- `docs/designs/future.md`: Pod-side claim renewal; node-pool targeting.
- Unit tests in `test_kubernetes_worker.py` driving a fake watch stream: gate per
  job, quota release, pending timeout, image-pull failure, adoption table rows.
- e2e (`test_e2e/kubernetes/`): N tasks in parallel on one worker; worker restart
  mid-run completes the job without retries.

# Left to the builder

- Shape of the refactor between `drive_vehicle` and the shared machinery.
- Watch resync / backoff details and in-memory map structure.
- Exact back-off after a quota rejection.
- Whether the batched heartbeat replaces the loop's between-claims heartbeat.
