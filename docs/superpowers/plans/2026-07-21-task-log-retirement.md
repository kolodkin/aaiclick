# Task Log Retirement Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** ClickHouse `task_logs` becomes the sole task log store; every file-based log path (`log_path` columns, file tee, `_capture_pod_logs`) is removed.

**Architecture:** Two halves. Enabling: shell-entry tasks (host shell / docker shell / k8s shell) currently log only to host files — give them host-minted `run_id`s and flush their captured text into CH `task_logs` from a short-lived spawned child process (worker parent stays `with_ch=False` per the chdb single-session constraint). Removal: delete the file tee in `capture_task_output`, `_capture_pod_logs`, and all `log_path` threading (RunnerResult, ExecuteFn, result.json, `update_task_status`, models, views, CLI, generated TS schema), then drop the two columns via Alembic.

**Tech Stack:** Python (asyncio, multiprocessing spawn, SQLModel, pydantic), ClickHouse (chdb/remote), Alembic via generate-migration skill, openapi-typescript.

**Spec:** `docs/designs/task_log_retirement.md`

## Global Constraints

- All imports at top of file; three groups (stdlib / external / package). Inline imports only for documented circular-dep last resorts (mp_worker's `orch_context` pattern).
- No `Any` shortcuts; NamedTuples over anonymous tuples.
- Tests follow `python-testing-style` skill (load before writing tests).
- `build_tasks.log_path` is OUT of scope — do not touch it.
- Never hand-write Alembic migrations — generate-migration skill only.
- Docs edits follow `markdown-style` skill; run `shortify` after doc edits.

---

### Task 1: `register_run` helper + shell-log conversion + subprocess flush

**Files:**
- Modify: `aaiclick/orchestration/execution/runner.py` (factor `register_run` out of `execute_task`)
- Modify: `aaiclick/orchestration/logging.py` (add `shell_text_to_lines`)
- Create: `aaiclick/orchestration/execution/log_flush.py`
- Test: `aaiclick/orchestration/execution/test_execution.py` (register_run), `aaiclick/orchestration/test_logging_helpers.py` or nearest existing logging test module (shell_text_to_lines)

**Interfaces:**
- Produces: `async def register_run(task_id: int) -> int` (runner.py) — mints snowflake run_id, appends to `Task.run_ids` and `TASK_RUNNING` to `run_statuses`, commits.
- Produces: `def shell_text_to_lines(text: str) -> list[LogLine]` (logging.py) — split on newlines, drop trailing empty line, each line `LogLine(stream=STDOUT_STREAM, level="INFO", text=...)`.
- Produces: `async def flush_shell_logs(task_id: int, job_id: int, run_id: int, text: str) -> None` (log_flush.py) — converts text and flushes `task_logs` from a spawned child process; returns after join; best-effort (log + swallow errors, same contract as `flush_task_logs`).

- [ ] **Step 1:** Write failing tests: `register_run` appends run_ids/run_statuses; `shell_text_to_lines("a\nb\n")` → 2 stdout/INFO lines; e2e `flush_shell_logs` then `read_task_logs` returns the lines (default backend).
- [ ] **Step 2:** Run tests, verify failure (names undefined).
- [ ] **Step 3:** Implement. `execute_task` calls `register_run` instead of its inline block. `log_flush.py` sketch:

```python
"""Flush shell-task logs to ClickHouse from a spawned child process.

The worker parent runs ``orch_context(with_ch=False)`` (chdb single-session
constraint), so the CH write happens in a short-lived spawned child that opens
its own context — the same isolation pattern as ``mp_worker``'s task child.
"""
import asyncio
import logging
import multiprocessing

from ..logging import flush_task_logs, shell_text_to_lines

logger = logging.getLogger(__name__)
_mp_ctx = multiprocessing.get_context("spawn")


def _flush_child_target(task_id: int, job_id: int, run_id: int, text: str) -> None:
    from ..orch_context import orch_context  # Circular dep: orch_context imports execution modules at top level.

    async def _run() -> None:
        async with orch_context():
            await flush_task_logs(task_id, job_id, run_id, shell_text_to_lines(text))

    asyncio.run(_run())


async def flush_shell_logs(task_id: int, job_id: int, run_id: int, text: str) -> None:
    if not text:
        return
    proc = _mp_ctx.Process(target=_flush_child_target, args=(task_id, job_id, run_id, text), daemon=True)
    proc.start()
    await asyncio.to_thread(proc.join)
    if proc.exitcode != 0:
        logger.error("Shell log flush child exited %s for task %s run %s", proc.exitcode, task_id, run_id)
```

(Verify whether orch_context is actually cyclic from log_flush; prefer top-level import if not.)
- [ ] **Step 4:** Tests pass. **Step 5:** Commit.

### Task 2: Host-shell vehicle — memory capture, CH flush

**Files:**
- Modify: `aaiclick/orchestration/execution/mp_worker.py` (`_HostShellHandle`, `_HostShellVehicle`, `_run_shell_on_host`)
- Test: `aaiclick/orchestration/execution/test_mp_worker_shell.py`

**Interfaces:**
- Consumes: `register_run`, `flush_shell_logs` (Task 1).
- Produces: `_HostShellHandle(proc, run_id)`; vehicle pipes `stdout=PIPE, stderr=STDOUT`, `wait()` does `communicate()` (with timeout-kill) then `await flush_shell_logs(...)`; `collect` returns `RunnerResult` (log_path stays until Task 5 strips it — pass `None`).

- [ ] Update test: `_run_shell_on_host` on `["sh", "-c", "echo hi"]` → `get_task_logs`-style read via `read_task_logs(task.id, run_ids[-1])` contains "hi"; run_ids appended. Run→fail→implement→pass→commit.

### Task 3: Docker shell — CH flush, drop file capture

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py` — `launch` calls `register_run` for `ENTRY_SHELL` (handle carries `run_id`, drops `log_path`); `wait` replaces `_capture_container_logs` with `docker logs` → `flush_shell_logs`; delete `_capture_container_logs`; `_build_docker_run_cmd` shell branch drops the `-v log_base` mount.
- Test: `aaiclick/orchestration/execution/test_docker_worker.py`

- [ ] Update mocked vehicle tests for new handle shape and flush call (patch `flush_shell_logs`); run→implement→pass→commit.

### Task 4: Kubernetes — delete `_capture_pod_logs`; shell → CH flush

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py` — `_PodHandle` drops `log_path`, gains `run_id: int | None` (shell only); `launch` calls `register_run` for `ENTRY_SHELL`, stops computing `log_path`, stops setting `env["AAICLICK_LOG_DIR"]`; `wait` fetches `kubectl logs` text ONLY for shell and flushes via `flush_shell_logs`; delete `_capture_pod_logs`; `POD_LOG_DIR` removed.
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py`

- [ ] Update mocks/asserts; run→implement→pass→commit.

### Task 5: Strip `log_path` end to end + remove file tee

**Files:**
- Modify: `aaiclick/orchestration/logging.py` — `capture_task_output` yields nothing, no file/`log_file`, `_ChLogHandler(sink, console)`; delete `get_logs_dir` (verify zero remaining users first — `AAICLICK_LOG_DIR` docs mention removed too).
- Modify: `aaiclick/orchestration/execution/runner.py` — `execute_task` returns `Any` (data_result only).
- Modify: `aaiclick/orchestration/execution/execution_worker.py` — `ExecuteFn` 3-tuple `(success, result_ref, error)`; `RunnerResult(success, result_ref, error)`; `_handle_task_result` / `_execute_in_process` updated.
- Modify: `aaiclick/orchestration/execution/mp_worker.py`, `docker_worker.py` (result.json payload loses `log_path`; module branch loses log_base mount + `AAICLICK_LOG_DIR` env), `kubernetes_worker.py` (`_write_task_run_result` loses log_path; `_pod_main`), `claiming.py` (`update_task_status` param; `clear_task` SQL drops `log_path = NULL`).
- Modify: `aaiclick/orchestration/models.py` — delete `Task.log_path`, `RemoteTaskResult.log_path`.
- Modify: `aaiclick/orchestration/view_models.py` — `TaskDetail`/`TaskLogsView` lose `log_path`; `task_to_detail`.
- Modify: `aaiclick/internal_api/tasks.py` — `get_task_logs` returns views without log_path.
- Modify: `aaiclick/cli_renderers.py` — drop `Log path:` line.
- Tests: `test_execution.py` (drop file asserts; keep CH asserts), `test_clear_task.py`, `test_view_models.py`, `test_docker_container_main.py`, `test_kubernetes_pod_main.py`, `test_mp_worker_shell.py`, `test_docker_worker.py`, `test_kubernetes_worker.py`, `test_models_kubernetes.py`, plus any server/internal_api tests referencing log_path (grep).

- [ ] Grep `log_path` across `aaiclick/` — after this task the only hits are `BuildTask.log_path` (+ its migration files). Run full orchestration test dir; commit.

### Task 6: Alembic migration (drop columns)

- [ ] Push branch (`git push -u origin claude/future-md-items-xm3omr`).
- [ ] Invoke `generate-migration` skill to autogenerate the migration dropping `tasks.log_path` and `remote_task_results.log_path`; pull the generated file; verify it does NOT touch `build_tasks.log_path`; commit/pull per skill flow.

### Task 7: Frontend schema regen

- [ ] Run `npm run gen-types` (regenerates `src/api/schema.ts` from OpenAPI). Verify `log_path` gone from `src/`. Commit.

### Task 8: Docs

**Files:** `docs/user_guide/orchestration.md` (log section), `docs/designs/orchestration.md` (shell task log sentence + Task model field list), `docs/designs/kubernetes_runner.md` (log file / RemoteTaskResult.log_path mentions), `docs/designs/api_server.md` (TaskDetail row), `docs/designs/frontend.md` (get_task_logs note), `docs/designs/future.md` (delete "Retire File-Based Task Logs" item), `docs/designs/task_log_retirement.md` (add implementation references).

- [ ] Update per markdown-style; run `shortify` skill; commit.

### Task 9: Verify + ship

- [ ] Full local test run (`uv run pytest aaiclick/ -x -q` or project's test command); fix fallout.
- [ ] Push; run `check-pr` skill (per CLAUDE.md) to watch GitHub Actions; fix failures.
