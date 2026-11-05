# Concurrent Kubernetes Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (inline, per CLAUDE.md). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One execution worker on `AAICLICK_RUNNER=kubernetes` drives many Pods at once, gated by unscheduled-Pod backpressure per job, and re-adopts its Pods after a restart.

**Architecture:** A reconciler replaces `_execution_worker_loop` in kubernetes mode: one `kubectl get pods -w` stream feeds a Pod-state map; a dispatcher claims against a pure gate over that map and spawns one coroutine per task; heartbeat and cancellation are one batched timer each. Module Pods await per-Pod futures; shell/jvm Pods keep their existing `drive_shell_vehicle` path (rare, per the user).

**Tech Stack:** Python asyncio, SQLModel/SQLAlchemy (Postgres + SQLite), `kubectl` via `execution/cli.py`, pytest, kind e2e.

**Spec:** `docs/superpowers/specs/2026-11-05-concurrent-kubernetes-worker-design.md`

## Global Constraints

- Concurrent mode only when `get_worker_runner() == RUNNER_KUBERNETES`; docker / local / mp keep `_execution_worker_loop`.
- Env vars and defaults: `AAICLICK_K8S_MAX_PENDING` = 10, `AAICLICK_K8S_MAX_PENDING_TOTAL` = 4 × max_pending, `AAICLICK_K8S_MAX_PODS` = unset, `AAICLICK_K8S_PENDING_TIMEOUT` = 900 s.
- Unscheduled = Pod has no `spec.nodeName`. Started = `task` container `running`/`terminated`, or phase `Succeeded`/`Failed`.
- Pod and Secret labels: `aaiclick/managed: "true"`, `aaiclick/job-id`, `aaiclick/task-id`, `aaiclick/run-epoch` (string values).
- Adoptable owner: worker `STOPPED`, or `last_heartbeat` older than `2 * HEARTBEAT_INTERVAL`.
- The Pod never writes `Task.status` or a terminal `task_runs` row.
- No Pod is ever deleted by a worker that lost ownership to adoption, nor on SIGTERM/`STOPPING`.
- Not-started longer than `pending_timeout` → fail through normal retries with the scheduler message / container waiting reason; no event parsing, no extra RBAC.
- RBAC stays as is: the worker never lists or reads Secrets (orphan-Secret cleanup from the spec's reclaim table is dropped — see Task 9).
- All imports at module top; `ContextVar` not `global`; `Literal` over enums; `NamedTuple` over tuples (CLAUDE.md).

## Review Focus

1. **Event arrives before the coroutine waits** — a Pod finishing between `kubectl apply` and the coroutine's `await` must still resolve: register the future before apply (Task 5/7 test `test_future_registered_before_apply_sees_fast_finish`).
2. **Watch stream drops mid-run** (API server restart) — in-flight tasks must not hang: re-list resolves futures of Pods that finished or vanished meanwhile (Task 5 `test_resync_resolves_missed_terminal`).
3. **Two replicas briefly overlap during a rollout** — the old one must not have its live Pods adopted nor delete the new one's (Task 3 `test_adopt_run_refuses_live_owner`, Task 8 `test_lost_ownership_by_adoption_keeps_pod`).
4. **Quota rejection storm** — many claims hitting `exceeded quota` must not consume attempts or spin: release + 30 s claim back-off (Task 7 `test_quota_rejection_releases_claim`, Task 8 `test_quota_backoff_pauses_claims`).
5. **Cancel while the Pod is still Pending** — `cancel_job` on an unscheduled Pod deletes it and reports `"cancelled"`, not a pending-timeout failure (Task 8 `test_cancel_pending_pod`).

---

### Task 1: Kubernetes limits config

**Files:**
- Modify: `aaiclick/orchestration/kubernetes_config.py`
- Test: `aaiclick/orchestration/test_kubernetes_config.py`

**Interfaces:**
- Produces: `class K8sLimits(NamedTuple): max_pending: int; max_pending_total: int; max_pods: int | None; pending_timeout: float` and `resolve_k8s_limits() -> K8sLimits`; constants `ENV_MAX_PENDING`, `ENV_MAX_PENDING_TOTAL`, `ENV_MAX_PODS`, `ENV_PENDING_TIMEOUT`.

- [ ] **Step 1: Write failing tests**

```python
def test_k8s_limits_defaults(monkeypatch):
    for v in (ENV_MAX_PENDING, ENV_MAX_PENDING_TOTAL, ENV_MAX_PODS, ENV_PENDING_TIMEOUT):
        monkeypatch.delenv(v, raising=False)
    assert resolve_k8s_limits() == K8sLimits(10, 40, None, 900.0)

def test_k8s_limits_total_follows_per_job(monkeypatch):
    monkeypatch.setenv(ENV_MAX_PENDING, "3")
    assert resolve_k8s_limits().max_pending_total == 12

@pytest.mark.parametrize("var,value", [(ENV_MAX_PENDING, "0"), (ENV_MAX_PODS, "-1"), (ENV_PENDING_TIMEOUT, "abc")])
def test_k8s_limits_reject_invalid(monkeypatch, var, value):
    monkeypatch.setenv(var, value)
    with pytest.raises(ValueError, match=var):
        resolve_k8s_limits()
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/test_kubernetes_config.py -v` → FAIL (import error).
- [ ] **Step 3: Implement** `K8sLimits` / `resolve_k8s_limits()`; positive ints / positive float, `ValueError` naming the env var.
- [ ] **Step 4:** same command → PASS.
- [ ] **Step 5: Commit** `feat(k8s): concurrency limit settings`.

### Task 2: Claim filters

**Files:**
- Modify: `aaiclick/orchestration/execution/sql/claim_next_task.sql`, `pg_handler.py`, `sqlite_handler.py`, `db_handler.py:81`, `claiming.py:50`
- Test: `aaiclick/orchestration/execution/test_execution_worker.py`

**Interfaces:**
- Produces: `claim_next_task(execution_worker_id: int, *, exclude_job_ids: Collection[int] = (), allow_host_tasks: bool = True) -> Task | None`; `DbHandler.claim_next_task(session, execution_worker_id, now, exclude_job_ids, allow_host_tasks)`.

- [ ] **Step 1: Write failing tests** (use `create_job` / `create_task` factories, `orch_ctx`)

```python
async def test_claim_skips_excluded_jobs(orch_ctx):
    # job A and job B each with one ready task
    task = await claim_next_task(worker_id, exclude_job_ids={job_a.id})
    assert task.job_id == job_b.id

async def test_claim_skips_host_tasks_when_disallowed(orch_ctx):
    # one image_source=None task, one image task, host task first in order
    task = await claim_next_task(worker_id, allow_host_tasks=False)
    assert task.image_source is not None
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_execution_worker.py -k "claim_skips" -v` → FAIL.
- [ ] **Step 3: Implement.** SQL adds bound-value predicates (keep the file's "values, never structural edits" rule): `AND NOT (t.job_id = ANY(:exclude_job_ids))` and `AND (:allow_host_tasks OR NOT (t.image_source IS NULL OR t.image_source::text = 'null'))`; update the header comment's parameter list. SQLite mirrors it with an expanding `IN` (skip the clause when empty) and `json_type`-free check `(t.image_source IS NULL OR t.image_source = 'null')`.
- [ ] **Step 4:** same command, plus `pytest aaiclick/orchestration/execution -k claim -q` → PASS.
- [ ] **Step 5: Commit** `feat(claim): exclude jobs and host tasks from a claim`.

### Task 3: Batched claim operations

**Files:**
- Modify: `aaiclick/orchestration/execution/claiming.py`
- Test: `aaiclick/orchestration/execution/test_claim_batch.py` (new)

**Interfaces:**
- Produces:
  - `class RunKey(NamedTuple): task_id: int; run_epoch: int`
  - `renew_claims(execution_worker_id: int, runs: Collection[RunKey]) -> set[int]` — task ids renewed (owner + epoch match, one UPDATE).
  - `aborted_runs(runs: Collection[RunKey]) -> set[int]` — `check_run_aborted` predicate over the set (one SELECT); missing task → not aborted.
  - `adopt_run(run: RunKey, execution_worker_id: int) -> bool` — one UPDATE moving `execution_worker_id` + renewing `claim_expires_at` where `status = RUNNING`, epoch matches, owner adoptable.
  - `release_claim(run: RunKey) -> bool` — back to `PENDING`, `run_epoch + 1`, clear `execution_worker_id` / `claim_expires_at` / `started_at`, close a `RUNNING` `task_runs` row as `CANCELLED`; no attempt consumed.

- [ ] **Step 1: Write failing tests**

```python
async def test_renew_claims_only_owned_epochs(orch_ctx): ...
    assert await renew_claims(w.id, [RunKey(t1.id, t1.run_epoch), RunKey(t2.id, t2.run_epoch + 1)]) == {t1.id}

async def test_aborted_runs_cancelled_and_cleared(orch_ctx): ...
    assert await aborted_runs([k_live, k_cancelled, k_cleared]) == {k_cancelled.task_id, k_cleared.task_id}

async def test_adopt_run_from_stopped_worker(orch_ctx): ...
    assert await adopt_run(key, new.id) and (await get_task(key.task_id)).execution_worker_id == new.id

async def test_adopt_run_refuses_live_owner(orch_ctx):   # Review Focus 3
    assert not await adopt_run(key, new.id)   # old worker heartbeated just now

async def test_adopt_run_refuses_stale_epoch(orch_ctx): ...

async def test_release_claim_returns_task_to_pending(orch_ctx): ...
    task = await get_task(key.task_id)
    assert (task.status, task.run_epoch, task.execution_worker_id, task.attempt) == (TASK_PENDING, key.run_epoch + 1, None, attempt_before)
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_claim_batch.py -v` → FAIL.
- [ ] **Step 3: Implement** the four functions; build `(id, run_epoch)` matching as `OR` of `and_` pairs (works on both backends); `renew_claims` uses `RETURNING id`.
- [ ] **Step 4:** same → PASS.
- [ ] **Step 5: Commit** `feat(claim): batched renew/abort checks, adoption and release`.

### Task 4: Pod and Secret labels

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` (`_pod_scaffold`, `_build_env_secret`, both launch paths)
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

**Interfaces:**
- Produces: `pod_labels(task_id: int, job_id: int, run_epoch: int) -> dict[str, str]`; `LABEL_MANAGED = "aaiclick/managed"`, `LABEL_JOB_ID`, `LABEL_TASK_ID`, `LABEL_RUN_EPOCH`; `_pod_scaffold(..., labels: dict[str, str])`, `_build_env_secret(..., labels: dict[str, str])`.

- [ ] **Step 1:** Update `test_build_pod_manifest_shape` to expect `m["metadata"]["labels"] == {"aaiclick/managed": "true", "aaiclick/job-id": "3", "aaiclick/task-id": "7", "aaiclick/run-epoch": "2"}`; same assertion for `build_shell_pod_manifest` and `_build_env_secret`.
- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -v` → FAIL.
- [ ] **Step 3: Implement**; thread `job_id` into `_build_pod_manifest` / `build_shell_pod_manifest`.
- [ ] **Step 4:** same → PASS.
- [ ] **Step 5: Commit** `feat(k8s): label task Pods and Secrets`.

### Task 5: Pod watch

**Files:**
- Create: `aaiclick/orchestration/execution/pod_watch.py`
- Test: `aaiclick/orchestration/execution/test_pod_watch.py`

**Interfaces:**
- Consumes: `LABEL_*` (Task 4).
- Produces:
  - `class PodState(NamedTuple): name: str; job_id: int; task_id: int; run_epoch: int; node_name: str | None; phase: str; started: bool; terminal: bool; exit_code: int; waiting_reason: str | None; message: str | None; created_at: float` (monotonic, set when first seen).
  - `parse_pod(obj: dict) -> PodState`
  - `class PodWatch`: `__init__(namespace: str, kubectl: str)`, `states: dict[str, PodState]`, `changed: asyncio.Event`, `expect(name: str) -> PodFutures` (call **before** apply), `forget(name: str)`, `async run()` (stream forever), `async resync()`, `apply(obj: dict, deleted: bool)` (pure state update — the unit-test seam).
  - `class PodFutures(NamedTuple): started: asyncio.Future[PodState]; done: asyncio.Future[PodState]` — `done` resolves on terminal **or** deletion (`phase=POD_NOT_FOUND`).

- [ ] **Step 1: Write failing tests** with literal Pod JSON dicts driven through `apply` (no kubectl):

```python
def test_parse_unscheduled_pod(): assert parse_pod(pending_json).node_name is None and not parse_pod(pending_json).started
def test_parse_image_pull_backoff(): assert parse_pod(backoff_json).waiting_reason == "ImagePullBackOff"
async def test_future_registered_before_apply_sees_fast_finish():   # Review Focus 1
    f = watch.expect("aaiclick-task-7-1"); watch.apply(succeeded_json, deleted=False)
    assert f.started.done() and (await f.done).exit_code == 0
async def test_deleted_pod_resolves_done_as_not_found(): ...
async def test_change_sets_event_only_on_scheduling_or_terminal_transition(): ...
async def test_resync_resolves_missed_terminal(monkeypatch):   # Review Focus 2
    # fake cli.run returns a list without the Pod -> its done future resolves NOT_FOUND
def test_stream_decoder_splits_concatenated_json(): ...   # pretty-printed objects back to back
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_pod_watch.py -v` → FAIL.
- [ ] **Step 3: Implement.** `run()` spawns `kubectl get pods -n <ns> -l aaiclick/managed=true -w --output-watch-events -o json` with `asyncio.create_subprocess_exec`, decodes back-to-back objects with `json.JSONDecoder.raw_decode` over a growing buffer, maps `type: DELETED` to `deleted=True`. On EOF/error: `resync()` (`kubectl get pods ... -o json` via `cli.run(stream=False)`; Pods tracked by `expect` but absent → resolve `done` as not found), sleep 1 s, restart. Reuse `POD_NOT_FOUND` from `kubernetes_worker.py`.
- [ ] **Step 4:** same → PASS.
- [ ] **Step 5: Commit** `feat(k8s): shared Pod watch`.

### Task 6: Capacity gate

**Files:**
- Create: `aaiclick/orchestration/execution/pod_gate.py`
- Test: `aaiclick/orchestration/execution/test_pod_gate.py`

**Interfaces:**
- Consumes: `PodState` (Task 5), `K8sLimits` (Task 1).
- Produces: `class GateDecision(NamedTuple): open: bool; blocked_jobs: frozenset[int]` and `gate(states: Iterable[PodState], limits: K8sLimits) -> GateDecision`.

- [ ] **Step 1: Write failing parametrized test** over (states, limits) → expected decision: per-job saturation blocks only that job; total ≥ `max_pending_total` closes; running (`node_name` set, not terminal) ≥ `max_pods` closes; terminal Pods count toward nothing.
- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_pod_gate.py -v` → FAIL.
- [ ] **Step 3: Implement** — pure function, no I/O.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): per-job capacity gate`.

### Task 7: Module Pod coroutine

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py`
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

**Interfaces:**
- Consumes: `PodWatch.expect/forget`, `PodFutures` (Task 5), `K8sLimits.pending_timeout` (Task 1), existing `_KubernetesVehicle.launch/terminate/cleanup`, `read_task_run_result`, `collect_remote_result`.
- Produces:
  - `class QuotaExceeded(Exception)` — raised by launch when `kubectl apply`/`create` stderr contains `exceeded quota`.
  - `async run_module_pod(task: Task, dispatch: JobDispatch, watch: PodWatch, limits: K8sLimits, *, adopted: bool = False) -> RunnerResult` — waits `started` up to `pending_timeout` (timeout → `RunnerResult(False, None, "Pod <name> not started after <n>s: <waiting_reason or message>")`), then `done` up to `parse_task_timeout()` (timer starts at `started`), reads the result row; deletes Pod + Secret in `finally` **unless** cancelled with `detach=True` (see Task 8). With `adopted=True` it skips launch.

- [ ] **Step 1: Write failing tests** (fake watch: a `PodWatch` driven via `apply`; `dispatch_with_fake_cli` for kubectl):

```python
async def test_run_module_pod_success_reads_result_row(...): ...
async def test_run_module_pod_pending_timeout_fails_with_reason(...):
    assert "ImagePullBackOff" in result.error
async def test_task_timeout_starts_at_container_start(...): ...
async def test_quota_rejection_raises(...):   # Review Focus 4 (release is Task 8)
    with pytest.raises(QuotaExceeded): ...
async def test_pod_disappeared_reports_error(...): ...
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -k run_module_pod -v` → FAIL.
- [ ] **Step 3: Implement.** Call `watch.expect(name)` before `launch`. Cancellation handling lives in Task 8's caller; this function lets `CancelledError` propagate after `terminate` unless detached.
- [ ] **Step 4:** → PASS; run the whole file.
- [ ] **Step 5: Commit** `feat(k8s): watch-driven module Pod run`.

### Task 8: Reconciler loop

**Files:**
- Create: `aaiclick/orchestration/execution/k8s_worker_loop.py`
- Modify: `aaiclick/orchestration/cli.py:30-55` (`start_execution_worker` picks `k8s_worker_main_loop` when runner is kubernetes), `execution/__init__.py` export
- Test: `aaiclick/orchestration/execution/test_k8s_worker_loop.py`

**Interfaces:**
- Consumes: Tasks 1–7; `register_execution_worker`, `execution_worker_heartbeat`, `deregister_execution_worker`, `_handle_task_result`, `_resolve_dispatch`, `dispatch_execute` (host + shell/jvm paths unchanged).
- Produces:
  - `class InFlight(NamedTuple): run: RunKey; job_id: int; pod_name: str | None; task: asyncio.Task; detach: list[bool]` (one-element mutable flag).
  - `async k8s_worker_main_loop(execution_worker_id: int | None = None, *, install_signal_handlers: bool = True, max_tasks: int | None = None, max_empty_polls: int | None = None) -> int`.

Behaviour (spec "Components" / "Wiring"):
- Dispatcher: wait on `watch.changed` or 1 s tick → `gate(...)` → while open: `claim_next_task(id, exclude_job_ids=blocked, allow_host_tasks=no host task in flight)`; `image_source` set and entry type `module` → `run_module_pod`; anything else → `dispatch_execute` (host subprocess, shell/jvm Pods keep `drive_shell_vehicle` — my call: rare, keeps their io copy logic intact).
- Per task: result → `_handle_task_result`; `QuotaExceeded` → `release_claim`, pause claiming 30 s.
- Heartbeat timer: worker heartbeat (STOPPING/STOPPED → shutdown) + `renew_claims`; missing ids → `aborted_runs`: aborted → cancel (delete Pod); not aborted → set `detach`, cancel, never delete.
- Cancel timer: `aborted_runs(in_flight)` → cancel those coroutines → result `"cancelled"` → `_handle_task_result` path as today (`release_cancelled_run`).
- Shutdown (SIGTERM / STOPPING): stop claiming, set `detach` on every module-Pod in-flight, cancel, `deregister_execution_worker`. Host/shell tasks: cancel as today.

- [ ] **Step 1: Write failing tests** (fake watch + fake kubectl, `orch_ctx`, `max_empty_polls` to end):

```python
async def test_runs_tasks_concurrently(...):          # 3 module tasks, all Pods launched before any finishes
async def test_gate_blocks_saturated_job_only(...):
async def test_quota_backoff_pauses_claims(...):     # Review Focus 4
async def test_cancel_pending_pod(...):              # Review Focus 5 -> Pod deleted, task cancelled
async def test_lost_ownership_by_adoption_keeps_pod(...):   # Review Focus 3 -> no kubectl delete
async def test_shutdown_leaves_pods_running(...):
async def test_one_host_task_at_a_time(...):
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_k8s_worker_loop.py -v` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** → PASS; `pytest aaiclick/orchestration -q` green; `ruff check aaiclick`.
- [ ] **Step 5: Commit** `feat(k8s): concurrent reconciler worker`.

### Task 9: Restart reclaim

**Files:**
- Modify: `aaiclick/orchestration/execution/k8s_worker_loop.py`
- Modify: spec reclaim table (drop the orphan-Secret row: listing Secrets would let the worker read DB URLs, which `rbac.yaml` deliberately forbids)
- Test: `aaiclick/orchestration/execution/test_k8s_worker_loop.py`

**Interfaces:**
- Produces: `async reclaim(execution_worker_id: int, watch: PodWatch, limits: K8sLimits) -> list[InFlight]` — runs once after `watch.resync()`, before the first claim.

Rows (from the spec): live + epoch match + `adopt_run` true → `run_module_pod(adopted=True)`; terminal + `adopt_run` true → read result, `_handle_task_result`, delete; label epoch ≠ task `run_epoch` or task terminal/missing → delete Pod + Secret by name; task in a cancelling status → delete Pod, `release_cancelled_run`. `adopt_run` false (live owner) → leave untouched. Adopted Pods' pending/task timers restart at adoption (my call).

- [ ] **Step 1: Write failing tests** — one per row, plus `test_reclaim_ignores_live_owner_pods`.
- [ ] **Step 2:** `pytest ... -k reclaim -v` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): adopt Pods on worker restart`.

### Task 10: Docs and e2e

**Files:**
- Modify: `docs/designs/kubernetes_runner.md` (vehicle table → reconciler; capacity gate; Pod-state table; restart reclaim; shutdown), `docs/user_guide/deployment.md` (new env vars), `docs/designs/future.md` (Pod-side claim renewal; node-pool targeting; per-replica limits), `aaiclick/deploy/templates/helm/aaiclick/templates/worker.yaml` (comment: one replica drives many Pods)
- Test: `test_e2e/kubernetes/test_kubernetes_runner_e2e.py`
- Delete: the spec and this plan once green (CLAUDE.md).

- [ ] **Step 1: Add e2e tests**: `test_parallel_tasks_one_worker` (job with 4 independent sleeping tasks finishes in < 2× one task's time with one worker); `test_worker_restart_adopts_pods` (kill worker mid-run, restart, job completes, `attempt` unchanged).
- [ ] **Step 2:** Push; run the kubernetes e2e workflow via `check-pr` → green.
- [ ] **Step 3: Update docs** per `markdown-style`.
- [ ] **Step 4:** Delete spec + plan, fix references; commit `docs(k8s): concurrent worker`.
