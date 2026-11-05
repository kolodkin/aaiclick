# Concurrent Kubernetes Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (inline, per CLAUDE.md). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One execution worker on `AAICLICK_RUNNER=kubernetes` drives many Pods at once, gated by unscheduled-Pod backpressure per job, and re-adopts module, shell and jvm Pods after a restart.

**Architecture:** A reconciler replaces `mp_worker_main_loop` in kubernetes mode: one `kubectl get pods -w` stream feeds a Pod-state map; a dispatcher claims against a pure gate over that map and spawns one coroutine per task; heartbeat and status are one batched timer each. Module and shell coroutines await per-Pod, per-stage futures; host tasks keep the mp child.

**Tech Stack:** Python asyncio, SQLModel/SQLAlchemy (Postgres + SQLite), `kubectl` via `execution/cli.py`, pytest, kind e2e.

**Spec:** `docs/superpowers/specs/2026-11-05-concurrent-kubernetes-worker-design.md`

## Global Constraints

- Concurrent mode only when `get_worker_runner() == RUNNER_KUBERNETES`; docker / local / mp keep `_execution_worker_loop`.
- Env vars and defaults: `AAICLICK_K8S_MAX_PENDING` = 10, `AAICLICK_K8S_MAX_PENDING_TOTAL` = 4 × max_pending, `AAICLICK_K8S_MAX_PODS` = unset, `AAICLICK_K8S_PENDING_TIMEOUT` = 900 s.
- Unscheduled = no `spec.nodeName`. Done = `task` container terminated, or Pod gone. Running = scheduled and not done.
- Pod and Secret labels: `aaiclick/managed: "true"`, `aaiclick/job-id`, `aaiclick/task-id`, `aaiclick/run-epoch` (string values). Pod kind comes from `Task.entry_type`.
- Secrets are created **after** their Pod with an `ownerReferences` entry to the Pod `uid` (no `blockOwnerDeletion`). The worker never lists Secrets.
- Deadlines from the DB only: started = the attempt's `RUNNING` `task_runs` row exists; not started → `tasks.started_at + pending_timeout`; started → `task_runs.started_at + task timeout`; both checked by the 1 s status query.
- Adoptable owner: `STOPPED`, or `last_heartbeat` older than `2 * HEARTBEAT_INTERVAL`; the adoption UPDATE is fenced on previous owner id + epoch. A worker whose own heartbeat fails for `2 * HEARTBEAT_INTERVAL` detaches from all runs.
- No Pod is ever deleted by a detached coro: SIGTERM, `STOPPING`, self-detach, or ownership lost to adoption.
- Not-started / timeout failures go through normal retries; no event parsing, no extra RBAC.
- Host tasks (`image_source IS NULL`) stay in the mp child (`_run_task_in_child`), one at a time; the reconciler parent runs under `ch_refused` like `mp_worker_main_loop`.
- `kubectl apply`/`create` with `-o json` use `cli.run(stream=False)`.
- Circular-import rules: `HEARTBEAT_INTERVAL` moves to `models.py` beside `WORKER_TIMEOUT`; labels and `POD_NOT_FOUND` live in `pod_watch.py`, imported by `kubernetes_worker.py`.
- CLAUDE.md: imports at top, `ContextVar` not `global`, `Literal` over enums, `NamedTuple` over tuples.

## Review Focus

1. **Event before the wait** — a Pod stage reached between `kubectl apply` and the coro's `await` must still resolve: futures registered before apply (Task 4 `test_futures_registered_before_apply_see_fast_transitions`).
2. **Watch stream drops mid-run** — resync must resolve stages from the listed state, including a Pod that reached done or vanished meanwhile (Task 4 `test_resync_resolves_done_and_vanished`).
3. **Two replicas overlap during a rollout** — no double adoption, no delete by the loser (Task 3 `test_adopt_run_fenced_on_previous_owner`, Task 9 `test_lost_ownership_by_adoption_keeps_pod`).
4. **Quota storm** — `exceeded quota` on many claims consumes no attempts and pauses claiming (Task 3 `test_release_claim_closes_host_run_row`, Task 9 `test_quota_backoff_pauses_claims`).
5. **SIGTERM with a shell Pod mid-copy** — the Pod stays, the temp dir goes, the adopter re-stages `in.json` (Task 9 `test_shutdown_leaves_pods_running`, Task 10 `test_reclaim_shell_pod_at_io_in_restages_input`).

---

### Task 1: Kubernetes limits config

**Files:**
- Modify: `aaiclick/orchestration/kubernetes_config.py`
- Test: `aaiclick/orchestration/test_kubernetes_config.py`

**Interfaces:**
- Produces: `class K8sLimits(NamedTuple): max_pending: int; max_pending_total: int; max_pods: int | None; pending_timeout: float`; `resolve_k8s_limits() -> K8sLimits`; `ENV_MAX_PENDING`, `ENV_MAX_PENDING_TOTAL`, `ENV_MAX_PODS`, `ENV_PENDING_TIMEOUT`.

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
- [ ] **Step 3: Implement** `K8sLimits` / `resolve_k8s_limits()`; positive ints / positive float; `ValueError` naming the env var.
- [ ] **Step 4:** same command → PASS.
- [ ] **Step 5: Commit** `feat(k8s): concurrency limit settings`.

### Task 2: Claim filters

**Files:**
- Modify: `aaiclick/orchestration/execution/sql/claim_next_task.sql`, `pg_handler.py`, `sqlite_handler.py`, `db_handler.py:81`, `claiming.py:50`
- Test: `aaiclick/orchestration/execution/test_execution_worker.py`

**Interfaces:**
- Produces: `claim_next_task(execution_worker_id: int, *, exclude_job_ids: Collection[int] = (), allow_host_tasks: bool = True) -> Task | None` (converts `exclude_job_ids` to a `list` before binding); `DbHandler.claim_next_task(session, execution_worker_id, now, exclude_job_ids: list[int], allow_host_tasks: bool)`.

- [ ] **Step 1: Write failing tests** (`create_job` / `create_task` factories, `orch_ctx`)

```python
async def test_claim_skips_excluded_jobs(orch_ctx):
    task = await claim_next_task(worker_id, exclude_job_ids={job_a.id})
    assert task.job_id == job_b.id

async def test_claim_skips_host_tasks_when_disallowed(orch_ctx):
    task = await claim_next_task(worker_id, allow_host_tasks=False)   # host task is first in order
    assert task.image_source is not None
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_execution_worker.py -k claim_skips -v` → FAIL.
- [ ] **Step 3: Implement.** SQL: `AND NOT (t.job_id = ANY(:exclude_job_ids))` and `AND (:allow_host_tasks OR NOT (t.image_source IS NULL OR t.image_source::text = 'null'))` (bound values, per the file's rule); update the header parameter list. SQLite: expanding `IN` (omit when empty) and `(t.image_source IS NULL OR t.image_source = 'null')`.
- [ ] **Step 4:** same command, plus `pytest aaiclick/orchestration/execution -k claim -q` → PASS.
- [ ] **Step 5: Commit** `feat(claim): exclude jobs and host tasks from a claim`.

### Task 3: Batched claim operations

**Files:**
- Modify: `aaiclick/orchestration/models.py` (move `HEARTBEAT_INTERVAL` here; `execution_worker.py` imports it), `aaiclick/orchestration/execution/claiming.py`
- Test: `aaiclick/orchestration/execution/test_claim_batch.py` (new)

**Interfaces:**
- Produces:
  - `class RunKey(NamedTuple): task_id: int; run_epoch: int`
  - `renew_claims(execution_worker_id: int, runs: Collection[RunKey]) -> set[int]` — task ids renewed (owner + epoch match), one UPDATE … RETURNING.
  - `class InFlightStatus(NamedTuple): task_id: int; aborted: bool; run_started_at: datetime | None` and `in_flight_status(runs: Collection[RunKey]) -> list[InFlightStatus]` — one SELECT joining `task_runs` on `task_id` with `status = RUNNING`; missing task → not aborted.
  - `adopt_run(run: RunKey, previous_owner: int, execution_worker_id: int) -> bool` — one UPDATE where `status = RUNNING`, epoch matches, `execution_worker_id = previous_owner`, and the previous owner is `STOPPED` or its `last_heartbeat < now - 2 * HEARTBEAT_INTERVAL`.
  - `release_claim(run: RunKey) -> bool` — `PENDING`, `run_epoch + 1`, clear `execution_worker_id` / `claim_expires_at` / `started_at`; `end_run(session, task_id, TASK_CANCELLED)` for a `RUNNING` row; attempt unchanged.

- [ ] **Step 1: Write failing tests**

```python
async def test_renew_claims_only_owned_epochs(orch_ctx): ...
    assert await renew_claims(w.id, [RunKey(t1.id, t1.run_epoch), RunKey(t2.id, t2.run_epoch + 1)]) == {t1.id}
async def test_renew_claims_keeps_claim_fresh_across_ticks(orch_ctx): ...   # claim_expires_at advances on each call
async def test_in_flight_status_flags_cancelled_and_cleared(orch_ctx): ...
async def test_in_flight_status_reports_run_start(orch_ctx): ...   # register_run on one task -> run_started_at set, others None
async def test_adopt_run_from_stopped_worker(orch_ctx): ...
async def test_adopt_run_refuses_live_owner(orch_ctx): ...
async def test_adopt_run_fenced_on_previous_owner(orch_ctx):   # Review Focus 3: second adopter with the old owner id gets False
async def test_adopt_run_refuses_stale_epoch(orch_ctx): ...
async def test_release_claim_returns_task_to_pending(orch_ctx): ...
async def test_release_claim_closes_host_run_row(orch_ctx):   # Review Focus 4: a RUNNING task_runs row ends CANCELLED
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_claim_batch.py -v` → FAIL.
- [ ] **Step 3: Implement**; `(id, run_epoch)` matching as `or_` of `and_` pairs (both backends).
- [ ] **Step 4:** → PASS; `pytest aaiclick/orchestration/execution/test_execution_worker.py -q` still green after the constant move.
- [ ] **Step 5: Commit** `feat(claim): batched renew/status, fenced adoption, claim release`.

### Task 4: Pod watch

**Files:**
- Create: `aaiclick/orchestration/execution/pod_watch.py`
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` (import `POD_NOT_FOUND` from `pod_watch`)
- Test: `aaiclick/orchestration/execution/test_pod_watch.py`

**Interfaces:**
- Produces:
  - `LABEL_MANAGED = "aaiclick/managed"`, `LABEL_JOB_ID`, `LABEL_TASK_ID`, `LABEL_RUN_EPOCH`, `pod_labels(task_id: int, job_id: int, run_epoch: int) -> dict[str, str]`, `POD_NOT_FOUND`.
  - `class ContainerState(NamedTuple): state: str; exit_code: int; reason: str | None; message: str | None` (`state` in `"waiting" | "running" | "terminated" | ""`).
  - `class PodState(NamedTuple): name: str; job_id: int; task_id: int; run_epoch: int; node_name: str | None; phase: str; io_in: ContainerState; task: ContainerState; gone: bool`, with properties `unscheduled`, `done` (task terminated or gone), `running`, `not_started_reason() -> str`.
  - `parse_pod(obj: dict) -> PodState`
  - `class PodFutures(NamedTuple): io_in_running: asyncio.Future[PodState]; task_running: asyncio.Future[PodState]; done: asyncio.Future[PodState]`
  - `class PodWatch`: `__init__(namespace: str, kubectl: str)`, `states: dict[str, PodState]`, `changed: asyncio.Event`, `expect(name) -> PodFutures` (before apply), `forget(name)`, `apply(obj: dict, deleted: bool)` (pure update, the test seam), `async resync()`, `async run()`.

- [ ] **Step 1: Write failing tests** with literal Pod JSON driven through `apply`:

```python
def test_parse_unscheduled_pod(): ...
def test_parse_shell_pod_container_states(): ...      # io-in init running, task waiting
def test_parse_image_pull_backoff_reason(): ...
def test_done_when_task_container_terminated_even_if_pod_running(): ...   # shell Pod, io sidecar alive
async def test_futures_registered_before_apply_see_fast_transitions(): ...   # Review Focus 1: all three resolve
async def test_deleted_pod_resolves_done_as_gone(): ...
async def test_changed_set_only_on_scheduling_done_or_gone(): ...
async def test_resync_resolves_done_and_vanished(monkeypatch): ...   # Review Focus 2: listed Succeeded resolves done; absent resolves gone
def test_stream_decoder_splits_concatenated_json(): ...
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_pod_watch.py -v` → FAIL.
- [ ] **Step 3: Implement.** `run()`: `asyncio.create_subprocess_exec(kubectl, "get", "pods", "-n", ns, "-l", "aaiclick/managed=true", "-w", "--output-watch-events", "-o", "json")`, `json.JSONDecoder.raw_decode` over a buffer, `type == "DELETED"` → `deleted=True`; on EOF/error `resync()` (`cli.run(..., stream=False)` list), sleep 1 s, restart.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): shared Pod watch`.

### Task 5: Labels, Pod-first launch, owned Secrets

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` (`_pod_scaffold`, `_build_env_secret`, `_kubectl_create_from_file`, both `launch` methods, `_PodSpec`), `aaiclick/deploy/templates/helm/aaiclick/templates/rbac.yaml` (comment)
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py` (update kubectl fakes at lines ~119-133 to return a dict with `metadata.uid`)

**Interfaces:**
- Consumes: `pod_labels` (Task 4).
- Produces: `_pod_scaffold(..., labels: dict[str, str])`; `_build_env_secret(name, namespace, env, *, labels, owner_uid: str) -> dict`; `_kubectl_create_from_file(manifest, verb="create") -> dict` (`-o json`, `stream=False`); `_PodSpec.job_id`; launch order Pod → Secret in `_KubernetesVehicle.launch` and `_KubernetesShellVehicle.launch` (the except-path `_kubectl_delete` of the Pod now also collects the Secret).

- [ ] **Step 1: Write failing tests**: labels on `_build_pod_manifest` / `build_shell_pod_manifest` / `_build_env_secret`; `test_env_secret_owned_by_pod` (`ownerReferences == [{"apiVersion": "v1", "kind": "Pod", "name": ..., "uid": "u1"}]`); `test_launch_creates_pod_before_secret` for both vehicles (fake cli records argv order and the uid it returned); shell vehicle creates no Secret without `command_env`.
- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -v` → FAIL.
- [ ] **Step 3: Implement**; update existing fakes.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): label Pods, create Secrets owned by their Pod`.

### Task 6: Capacity gate

**Files:**
- Create: `aaiclick/orchestration/execution/pod_gate.py`
- Test: `aaiclick/orchestration/execution/test_pod_gate.py`

**Interfaces:**
- Consumes: `PodState` (Task 4), `K8sLimits` (Task 1).
- Produces: `class GateDecision(NamedTuple): open: bool; blocked_jobs: frozenset[int]`; `gate(states: Iterable[PodState], limits: K8sLimits) -> GateDecision`.

- [ ] **Step 1:** Parametrized test over (states, limits) → decision: per-job saturation blocks only that job; total ≥ `max_pending_total` closes; running ≥ `max_pods` closes; done Pods count toward nothing.
- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_pod_gate.py -v` → FAIL.
- [ ] **Step 3: Implement** — pure, no I/O.
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): per-job capacity gate`.

### Task 7: Module Pod coroutine

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py`
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

**Interfaces:**
- Consumes: `PodWatch`, `PodFutures` (Task 4); `_KubernetesVehicle.launch/cleanup` (Task 5); `read_task_run_result`, `collect_remote_result`.
- Produces:
  - `class QuotaExceeded(Exception)` — raised by launch when kubectl stderr contains `exceeded quota`.
  - `class DeadlineExpired(Exception)` with `.reason: str` — the status timer cancels a coro by raising this inside it (via `asyncio.Task.cancel(msg=...)` read back as the reason).
  - `async run_module_pod(task: Task, execution_worker_id: int, pod_config: KubernetesConfig, watch: PodWatch, *, image_tag: str | None, resources: dict | None, detach: list[bool]) -> RunnerResult` — `image_tag=None` means adopted (skip launch). Registers futures before apply; awaits `done`; reads the result row; `finally`: `forget`, and delete the Pod unless `detach[0]`. On cancel: `"cancelled"` or the deadline reason, Pod deleted unless detached.

- [ ] **Step 1: Write failing tests** (fake watch driven via `apply`, `dispatch_with_fake_cli`):

```python
async def test_run_module_pod_success_reads_result_row(...): ...
async def test_run_module_pod_adopted_skips_launch(...): ...
async def test_cancel_with_deadline_reason_reports_it_and_deletes_pod(...):
    assert "ImagePullBackOff" in result.error
async def test_cancel_detached_keeps_pod(...): ...   # no kubectl delete
async def test_quota_rejection_raises(...): ...
async def test_pod_gone_reports_disappeared(...): ...
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_kubernetes_worker.py -k run_module_pod -v` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): watch-driven module Pod run`.

### Task 8: Shell / jvm Pod coroutine

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` (reuse `_KubernetesShellVehicle.launch`, `_copy_in`, the `out.json` exec; drop its `wait`), `aaiclick/orchestration/execution/runner.py` (`follow_vehicle_output` gains `since: datetime | None`)
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

**Interfaces:**
- Consumes: Task 4 futures; `stage_in_json`, `register_run`, `follow_vehicle_output`, `stop_output_follower`, `parse_out_json`, `read_task_logs`.
- Produces: `async run_shell_pod(task: Task, execution_worker_id: int, pod_config: KubernetesConfig, watch: PodWatch, *, image_tag: str | None, command: list[str] | None, command_env: dict[str, str] | None, resources: dict | None, run_id: int | None, detach: list[bool]) -> ShellOutcome` where `class ShellOutcome(NamedTuple): result: RunnerResult; out_value: Any` (feeds the existing `drive_shell_vehicle` post-processing of `out.json` into a result ref — extract that tail into `finish_shell_result(task, outcome)` if it is not already a function).
  - Stages: stage `in.json` (always, adopted or not) → launch unless adopted → await `io_in_running` → `_copy_in` (skip if the watch shows `io-in` already finished) → await `task_running` → `register_run` unless `run_id` was passed → follower with `since=` last stored log line for that run → await `done` → exec read `out.json` → `finally`: stop follower, `forget`, rmtree the temp dir, delete the Pod unless detached.
  - `register_host_log_run` is no longer called before launch for kubernetes shell tasks.

- [ ] **Step 1: Write failing tests**

```python
async def test_run_shell_pod_full_lifecycle(...): ...            # cp in, register_run after task running, out.json parsed
async def test_run_shell_pod_adopted_at_io_in_restages_and_copies(...): ...
async def test_run_shell_pod_adopted_at_task_running_reuses_run_and_follows_since(...): ...
async def test_run_shell_pod_adopted_at_done_reads_out_json(...): ...
async def test_run_shell_pod_detached_keeps_pod_removes_tmpdir(...): ...
async def test_run_shell_pod_done_without_pod_terminal_phase(...): ...   # io sidecar alive
```

- [ ] **Step 2:** `pytest ... -k run_shell_pod -v` → FAIL.
- [ ] **Step 3: Implement.** Delete `_KubernetesShellVehicle.wait`/`terminate`/`output_argv` and `_run_shell_in_pod`/`_run_task_in_pod` once nothing calls them; remove their tests.
- [ ] **Step 4:** → PASS; whole file green.
- [ ] **Step 5: Commit** `feat(k8s): watch-driven shell/jvm Pod run with mid-stage adoption`.

### Task 9: Reconciler loop

**Files:**
- Create: `aaiclick/orchestration/execution/k8s_worker_loop.py`
- Modify: `aaiclick/orchestration/cli.py:30-55` (`start_execution_worker` picks `k8s_worker_main_loop` on kubernetes), `aaiclick/orchestration/execution/__init__.py`, `aaiclick/orchestration/execution/dispatch.py` (expose `_resolve_dispatch` as `resolve_dispatch`)
- Test: `aaiclick/orchestration/execution/test_k8s_worker_loop.py`

**Interfaces:**
- Consumes: Tasks 1–8; `register_execution_worker`, `execution_worker_heartbeat`, `deregister_execution_worker`, `_handle_task_result`, `_run_task_in_child`, `resolve_launch_image`, `ch_refused`.
- Produces:
  - `class InFlight(NamedTuple): task: Task; run: RunKey; pod_name: str | None; aio: asyncio.Task; detach: list[bool]`
  - `async k8s_worker_main_loop(execution_worker_id: int | None = None, *, install_signal_handlers: bool = True, max_tasks: int | None = None, max_empty_polls: int | None = None) -> int`

Behaviour:
- Dispatcher: wake on `watch.changed` or 1 s tick → `gate` → claim with `exclude_job_ids=blocked`, `allow_host_tasks=(no host task in flight)` → by `entry_type` / `image_source`: module Pod → `run_module_pod`; shell/jvm → `run_shell_pod`; host → `_run_task_in_child`. Each result → `_handle_task_result`.
- `QuotaExceeded` → `release_claim`, no claims for 30 s.
- Heartbeat timer (`HEARTBEAT_INTERVAL`): worker heartbeat (STOPPING/STOPPED → shutdown; no success for `2 * HEARTBEAT_INTERVAL` → detach all, then reclaim when it succeeds again) + `renew_claims`; missing ids → `in_flight_status`: aborted → cancel; else detach + cancel. Then the stale-Pod sweep (Task 10).
- Status timer (`POLL_INTERVAL`): `in_flight_status` → aborted → cancel (`"cancelled"`); `run_started_at is None and utc_now() > task.started_at + pending_timeout` → cancel with the not-started reason from `watch.states[pod].not_started_reason()`; `run_started_at is not None and utc_now() > run_started_at + task_timeout` → cancel with `"Task timed out after <n>s"`.
- Shutdown (SIGTERM / STOPPING): stop claiming, detach + cancel every Pod coro, cancel host tasks as today, stop the watch, deregister.

- [ ] **Step 1: Write failing tests** (fake watch + fake kubectl, `orch_ctx`, `max_empty_polls`):

```python
async def test_runs_tasks_concurrently(...): ...
async def test_gate_blocks_saturated_job_only(...): ...
async def test_max_pods_stops_claims(...): ...
async def test_quota_backoff_pauses_claims(...): ...                 # Review Focus 4
async def test_cancel_pending_pod(...): ...                          # Pod deleted, task cancelled
async def test_pending_timeout_counts_from_claim(...): ...
async def test_task_timeout_counts_from_run_start(...): ...
async def test_running_pod_without_run_row_hits_pending_timeout(...): ...
async def test_lost_ownership_by_adoption_keeps_pod(...): ...        # Review Focus 3
async def test_heartbeat_outage_detaches_all(...): ...
async def test_shutdown_leaves_pods_running(...): ...                # Review Focus 5, module and shell
async def test_one_host_task_at_a_time(...): ...
```

- [ ] **Step 2:** `pytest aaiclick/orchestration/execution/test_k8s_worker_loop.py -v` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** → PASS; `pytest aaiclick/orchestration -q`; `ruff check aaiclick`.
- [ ] **Step 5: Commit** `feat(k8s): concurrent reconciler worker`.

### Task 10: Restart reclaim and stale-Pod sweep

**Files:**
- Modify: `aaiclick/orchestration/execution/k8s_worker_loop.py`
- Test: `aaiclick/orchestration/execution/test_k8s_worker_loop.py`

**Interfaces:**
- Produces: `async reclaim(execution_worker_id: int, watch: PodWatch, limits: K8sLimits) -> list[InFlight]` (after `watch.resync()`, before the first claim; also after a heartbeat outage ends) and `async sweep(execution_worker_id: int, watch: PodWatch, in_flight: dict[int, InFlight]) -> None` (heartbeat tick).

Rows (spec "Restart reclaim"): load `Task` rows for all labeled Pods; `RUNNING` + epoch match + `adopt_run(previous_owner=task.execution_worker_id)` → `run_module_pod(image_tag=None)` or `run_shell_pod(image_tag=None, …)` with `pod_config` from `resolve_pod_config()`, `run_id` from the `RUNNING` run row if any; cancelling status + adoptable → delete Pod + `release_cancelled_run`; stale epoch / terminal / missing → delete Pod; live owner → untouched. `sweep` applies only the delete rows to managed Pods not in `in_flight`.

- [ ] **Step 1: Write failing tests** — one per row for a module Pod, plus `test_reclaim_shell_pod_at_io_in_restages_input` (Review Focus 5), `test_reclaim_shell_pod_at_task_running_resumes_logs`, `test_reclaim_done_module_pod_finishes_task`, `test_reclaim_ignores_live_owner_pods`, `test_adopted_pod_keeps_original_deadlines`, `test_sweep_deletes_reaped_task_pod`, `test_sweep_leaves_live_owner_pod`.
- [ ] **Step 2:** `pytest ... -k "reclaim or sweep" -v` → FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** → PASS.
- [ ] **Step 5: Commit** `feat(k8s): adopt Pods on restart, sweep stale Pods`.

### Task 11: Docs and e2e

**Files:**
- Modify: `test_e2e/job_wait.py` (`run_worker_until_all_done` drives `k8s_worker_main_loop` when `get_worker_runner() == RUNNER_KUBERNETES`), `test_e2e/kubernetes/test_kubernetes_runner_e2e.py`, `docs/designs/kubernetes_runner.md`, `docs/user_guide/deployment.md`, `docs/designs/future.md`, `aaiclick/deploy/templates/helm/aaiclick/templates/worker.yaml` (comment)
- Delete: the spec and this plan once green (CLAUDE.md).

- [ ] **Step 1: Add e2e tests**: `test_parallel_tasks_one_worker` (4 independent sleeping tasks finish in < 2× one task's time); `test_worker_restart_adopts_pods` (job with one module and one shell task; stop the loop mid-run, start a new one; job completes with `attempt` unchanged and shell logs intact).
- [ ] **Step 2:** Push; run the kubernetes e2e workflow via `check-pr` → green.
- [ ] **Step 3: Update docs** per `markdown-style`: reconciler, gate, deadlines, ownership, reclaim, sweep, shutdown; fix the stale `mark_dead_workers` reference; `future.md` gets Pod-side claim renewal, node-pool targeting, per-replica limits.
- [ ] **Step 4:** Delete spec + plan, fix references; commit `docs(k8s): concurrent worker`.
