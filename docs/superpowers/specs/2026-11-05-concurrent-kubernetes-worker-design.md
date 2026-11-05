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
  one status query.
- Module, shell and jvm Pods all survive a worker restart.

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
- Shell / jvm Pods: `io-in` init container waits for `in.json` (`kubectl cp` from
  the worker's temp dir, built by `stage_in_json` from the task's kwargs), the
  `io` sidecar sleeps forever (so the Pod never reaches a terminal phase),
  `out.json` is read by `kubectl exec`; the host registers the run
  (`register_host_log_run`) **before launch** and follows `kubectl logs -f`.
- `execution-worker start` runs `mp_worker_main_loop`; `image_source=NULL` tasks
  run in a multiprocessing child (the parent must not touch ClickHouse,
  `ch_refused`).
- A worker tolerates `SELF_STOP_AFTER` (8 min) without a heartbeat reaching the
  DB before aborting its run.
- Module Pods write their own `task_logs`; the host follows `kubectl logs -f` only
  for shell Pods (and the bootstrap-failure rescue in `_collect_unfollowed_output`).
- Image builds execute on buildkitd; the worker runs only the client (clone,
  scaffold, `buildx`, push) as one of those host tasks.

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

The worker becomes a reconciler: one Pod watch keeps a Pod-state map current, a
dispatcher claims and launches against a capacity gate computed from that map,
and each claimed Pod task is a coroutine that advances through stages on its own
Pod's events. Heartbeat and status are one batched timer each, not one per task.
Module, shell and jvm Pods share this machinery; `drive_vehicle` is no longer used
on Kubernetes.

```
+------------------------------------------------------+
| ExecutionWorker  (AAICLICK_RUNNER=kubernetes)        |
| +----------------+     +---------------------------+ |
| | Pod watch      | --> | Pod-state map             | |
| +----------------+     +---------------------------+ |
|                          |                           |
| +----------------+     +---------------------------+ |
| | Dispatcher     | <-- | Gate: per job, total, max | |
| +----------------+     +---------------------------+ |
|   | claim + launch                                   |
| +----------------+  +--------------+  +------------+ |
| | Task coro  x N |  | Heartbeat    |  | Status     | |
| +----------------+  +--------------+  +------------+ |
+------------------------------------------------------+
```

## Mode selection

Concurrent mode is on when `AAICLICK_RUNNER=kubernetes`: `execution-worker start`
runs the reconciler instead of `mp_worker_main_loop`. Helm keeps the worker
Deployment, default `replicas: 1`. Docker / local / mp keep the serial loop.

## Components

All run on one asyncio event loop — no locks, no threads.

| Component     | Behaviour |
|---------------|-----------|
| Pod watch     | One `kubectl get pods -w --output-watch-events -n <ns> -l aaiclick/managed=true -o json` stream. Updates the map, resolves per-Pod futures, sets the dispatcher wake event. On stream end: re-list (resync), then re-watch. |
| Pod-state map | `dict[pod name → PodState]`: labels, `spec.nodeName`, phase, per-container state (`io-in` init, `task`) with waiting reason / message / exit code. Written only by the watch. **Done** = the `task` container terminated, or the Pod is gone — the same rule for module and shell Pods (a shell Pod's `io` sidecar never exits). |
| Gate          | Pure function over the map (see Capacity gate). |
| Dispatcher    | Loop: wait on wake event or 1 s tick → run gate → `claim_next_task(exclude_job_ids=…, allow_host_tasks=…)` until the gate closes or no task → `create_task(coro)` per claim. First pass is restart reclaim. |
| Module coro   | Create Pod, then its Secret owned by the Pod → await **done** → read `remote_task_results` → `_handle_task_result` → delete Pod. |
| Shell coro    | Stage `in.json` → create Pod (+ owned Secret when `command_env`) → await `io-in` running → `kubectl cp` `in.json` + ready marker → await `task` running → `register_run`, start the `kubectl logs -f` follower → await **done** → `kubectl exec` read `out.json` → result → delete Pod, remove the temp dir. |
| Host task     | `image_source=NULL` tasks (image build, plain host) run in the mp child exactly as today (`_run_task_in_child`), one at a time per worker (my call: builds are thin buildkitd clients; serial keeps today's shared cwd/env). While one runs, the claim query excludes them. |
| Heartbeat     | Every `HEARTBEAT_INTERVAL`: worker heartbeat + one claim-renew `UPDATE … WHERE execution_worker_id = :me AND (id, run_epoch) IN (in-flight)` + the stale-Pod sweep. |
| Status        | Every `POLL_INTERVAL`: one query over in-flight `(task_id, run_epoch)` returning aborted (the `check_run_aborted` predicate) and the `started_at` of the attempt's `RUNNING` `task_runs` row. Drives cancellation and both deadlines. |

## Wiring

| From → to              | Mechanism |
|------------------------|-----------|
| Watch → map            | Direct `dict` mutation |
| Watch → coro           | `asyncio.Future` per Pod name and stage (`io_in_running`, `task_running`, `done`), resolved from the container states; the coro registers them **before** `kubectl apply` so no event is missed |
| Watch → dispatcher     | One shared `asyncio.Event`, set on any scheduled / done / gone change |
| Map → gate             | Function call |
| Dispatcher → coro      | `asyncio.create_task`; worker keeps `in_flight: dict[task_id, InFlight]` (the `Task`, Pod name, asyncio task, `detach` flag) used by heartbeat and status |
| Status → coro          | `task.cancel()` with a reason: `cancelled` deletes the Pod and reports `"cancelled"` (cancelled-overrides-result, as today); a deadline reason deletes the Pod and reports that reason as the failure |
| Shutdown / lost owner  | Set `detach`, then `task.cancel()`: the coro stops following, removes only its local temp dir, **never** deletes the Pod |

Not an `asyncio.Queue`: every coro waits on its own Pod, so a future per Pod
delivers each event to exactly one waiter.

## Ownership

Ownership is `execution_worker_id` + `run_epoch`, as today. Three rules keep two
workers from driving one run:

- **Adoption threshold.** A worker may adopt a run whose owner is `STOPPED`, or
  whose `last_heartbeat` is older than `2 × HEARTBEAT_INTERVAL` (60 s). The
  adoption UPDATE is fenced on the previous owner id and the epoch, so two
  restarting replicas cannot both adopt the same run.
- **Self-detach, symmetric.** A worker whose heartbeat has not reached the DB for
  `2 × HEARTBEAT_INTERVAL` detaches from all its runs (sets `detach`, cancels the
  coros, stops writing) and re-runs reclaim when the DB answers again — its own
  Pods are still there to re-adopt unless another replica took them. This
  replaces `SELF_STOP_AFTER` in kubernetes mode.
- **Lost ownership.** A claim-renew row that didn't match: re-read its `run_epoch`.
  Changed (cleared / reaped) → cancel with `cancelled`, delete the Pod. Unchanged
  (adopted) → detach, never delete.

## Pod labels

Every Pod (and Secret) carries:

| Label                | Value          |
|----------------------|----------------|
| `aaiclick/managed`   | `"true"`       |
| `aaiclick/job-id`    | `job_id`       |
| `aaiclick/task-id`   | `task_id`      |
| `aaiclick/run-epoch` | `run_epoch`    |

The Pod kind (module / shell / jvm) is read from the task's `entry_type` at claim
or reclaim, not from a label.

## Secret ownership

The per-attempt Secret holds the DB URLs (module) or `command_env` (shell), and
the worker may not list Secrets (`rbac.yaml`: create / delete only). So no Secret
may outlive its Pod:

- Create the **Pod first** (`kubectl apply -o json` returns its `uid`), then the
  Secret with `metadata.ownerReferences: [{apiVersion: v1, kind: Pod, name, uid}]`
  (no `blockOwnerDeletion`, so no extra RBAC). Both `launch` functions are
  rewritten for this order.
- The Kubernetes garbage collector deletes the Secret whenever the Pod goes —
  worker delete, eviction, manual delete.
- Until the Secret exists the Pod waits in `CreateContainerConfigError`. A worker
  crash in between leaves a Pod that never starts; the not-started deadline fails
  it and the Pod delete removes everything.

## Capacity gate

A Pod is **unscheduled** when it has no `spec.nodeName` (covers just-created Pods
the scheduler hasn't marked yet, so a burst of claims cannot overshoot). A Pod is
**running** when it is scheduled and not done.

Counts are namespace-wide over `aaiclick/managed=true` Pods (my call: the
default is one replica; with more, the limits are shared, not per replica).
Before each claim the worker computes, from the watch map:

- `blocked_jobs` = jobs with ≥ `max_pending` unscheduled Pods.
- If total unscheduled ≥ `max_pending_total`, or running Pods ≥ `max_pods`
  (when set), claim nothing this tick.
- Otherwise claim with `job_id NOT IN blocked_jobs`.

| Setting                          | Env var                              | Default                |
|----------------------------------|--------------------------------------|------------------------|
| `max_pending` (per job)          | `AAICLICK_K8S_MAX_PENDING`           | 10                     |
| `max_pending_total`              | `AAICLICK_K8S_MAX_PENDING_TOTAL`     | 4 × `max_pending`      |
| `max_pods` (running, optional)   | `AAICLICK_K8S_MAX_PODS`              | unset (no cap)         |
| `pending_timeout`                | `AAICLICK_K8S_PENDING_TIMEOUT`       | 15 min                 |

Parsed in `kubernetes_config.py` next to the existing `AAICLICK_K8S_*` fields.

## Deadlines

Both deadlines come from the aaiclick DB only — no Kubernetes timestamps, no
worker-local launch or adoption time — so they survive a worker restart and
match what the UI shows. A task counts as **started** once the attempt's
`RUNNING` `task_runs` row exists. For module Pods that is `register_run` inside
the Pod; for shell / jvm Pods the host now registers the run **when the `task`
container starts** (the watch says so) instead of before launch, so both kinds
mean the same thing and shell Pods gain the not-started deadline they lack today.

| Phase       | Deadline                              | On expiry |
|-------------|---------------------------------------|-----------|
| Not started | `tasks.started_at + pending_timeout`  | Delete the Pod; fail with the scheduler message / container waiting reason from the watch, or "Pod running but the task never started"; normal retries |
| Started     | `task_runs.started_at + task timeout` | Delete the Pod; fail through normal retries (as today) |

Both checks ride on the 1 s status query: no extra queries, no race with
`register_run`. `tasks.started_at` is stamped by the worker, `task_runs.started_at`
by the Pod or the host; NTP-synced nodes keep the skew to seconds against
minute-scale timeouts. `activeDeadlineSeconds` on shell Pods stays as the
cluster-side bound for a Pod nobody ever adopts.

## Pod-state handling

| Pod state                                              | Action |
|--------------------------------------------------------|--------|
| Not started past `pending_timeout`                     | See Deadlines |
| `kubectl apply` rejected: `exceeded quota`             | `release_claim`: back to `PENDING`, `run_epoch + 1`, owner / claim / `started_at` cleared, a `RUNNING` run row closed; no attempt consumed. Claiming pauses 30 s |
| Pod gone mid-run (eviction, preemption, node loss)     | "Pod disappeared" failure → normal retry |
| Done (`task` container terminated)                     | Module: read `remote_task_results`; shell: read `out.json`; existing collect semantics |

One rule covers "never fits" and stuck image pulls (`ImagePullBackOff`,
`InvalidImageName`, `CreateContainerConfigError`, ...): no event parsing, no
message classification, no extra RBAC. A doomed task costs
`pending_timeout × (max_retries + 1)` before it fails for good; the error carries
the reason so the user sees why.

Fail-fast was rejected: Kubernetes documents no reason meaning "permanent".
`ImagePullBackOff` ([Images](https://kubernetes.io/docs/concepts/containers/images/#imagepullbackoff))
covers transient and permanent causes; `InvalidImageName` / `ErrImagePull` exist
only in kubelet source; `NotTriggerScaleUp`
([CA FAQ](https://github.com/kubernetes/autoscaler/blob/master/cluster-autoscaler/FAQ.md#what-events-are-emitted-by-ca))
is autoscaler-specific and also fires on "max size reached"; OCI registry codes
([error codes](https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes))
reach the Pod only as free text.

## Restart reclaim

On start (and before the first claim), the worker lists `aaiclick/managed=true`
Pods, loads the `Task` rows they name, and for each Pod:

| Found | Action |
|-------|--------|
| Task `RUNNING`, label epoch == `run_epoch`, owner adoptable | `adopt_run` (owner + epoch fenced UPDATE moving the claim and renewing it) → start the module or shell coro **in adopted mode** at the Pod's current stage |
| Task in a cancelling status, epoch matches, owner adoptable | Delete the Pod, `release_cancelled_run` |
| Label epoch ≠ `run_epoch`, or task terminal / not found     | Delete the Pod |
| Owner live (not adoptable)                                  | Leave untouched |

Adopted mode skips launch and image resolution (`pod_config` only). A shell coro
adopted at `io-in` re-stages `in.json` from the DB and copies it in; adopted at
`task` running it reuses the existing `RUNNING` run row (or registers one) and
resumes the log follower with `--since-time` set to the last line already stored
for that run, so no lines are lost and at most a second repeats; adopted at done
it reads `out.json`. A done module Pod is read and finished the same way.

Reclaim starts from labeled Pods only, so docker / local / mp tasks are never
touched. A `RUNNING` task whose Pod is gone is left to claim expiry
(`release_expired_claims`, today's path). Gate counts and the watch map are
rebuilt from the same list; no state is persisted. Secrets need no pass (see
Secret ownership).

## Stale-Pod sweep

Startup reclaim alone would leave Pods of tasks reaped or cleared during a long
outage running, holding capacity and counting toward the gate. On each heartbeat
tick the worker takes the managed Pods that are not in flight, loads their tasks,
and applies the reclaim table's delete rows (stale epoch, terminal / missing
task, cancelling task). Pods of a live owner are left alone.

## Shutdown

`SIGTERM` (rollout, node drain): stop claiming, detach every Pod coro (no Pod is
deleted; shell temp dirs are removed), stop the watch, deregister. The next worker
adopts the Pods. `execution-worker stop` (`STOPPING`) behaves the same. Host
tasks in the mp child are cancelled as today.

## Docs and tests

- `docs/designs/kubernetes_runner.md`: replace the vehicle / `wait` polling
  description with the reconciler; add capacity gate, deadlines, restart reclaim,
  sweep, shutdown; fix the stale `mark_dead_workers` reference.
- `docs/user_guide/deployment.md` / Helm values: new env vars.
- `docs/designs/future.md`: Pod-side claim renewal; node-pool targeting;
  per-replica limits.
- Unit tests drive a fake watch stream and a fake `kubectl`: gate per job, quota
  release, both deadlines, adoption rows for module and shell Pods, detach vs
  delete, sweep.
- e2e (`test_e2e/kubernetes/`): N tasks in parallel on one worker; worker restart
  mid-run completes a job with module and shell tasks without retries. The e2e
  harness (`test_e2e/job_wait.py`) must drive the reconciler, not
  `mp_worker_main_loop`, on kubernetes.

# Left to the builder

- Watch resync / backoff details and the map's exact fields.
- Exact back-off after a quota rejection.
- How the log follower learns the last stored line's timestamp (`read_task_logs`
  or a small ClickHouse query).
