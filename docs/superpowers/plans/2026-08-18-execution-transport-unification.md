# Execution Transport Unification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Unify the Docker and Kubernetes runners on one result transport (the `RemoteTaskResult` SQL row) and one container-side entrypoint, and collapse `run_job_tasks`'s hand-rolled status transitions onto the shared `claiming` helpers.

**Architecture:** A new `aaiclick/orchestration/execution/remote_result.py` owns the cross-boundary result contract: the container/Pod entrypoint that writes a `RemoteTaskResult` row, the host-side reader, and the shared collect logic. `docker_worker.py` drops its IPC-tmpdir/`result.json` transport (safe: docker containers already boot `orch_context()` against SQL to read the Task row) and `kubernetes_worker.py` drops its Pod-side half. `run_job_tasks` keeps its contract (synchronous, single-job, fail-fast, no worker rows — the debug harness `job_test` depends on it, and the chdb single-session constraint forbids routing it through mp children) but delegates status writes to `claiming.update_task_status` / `claiming.update_job_status`.

**Tech Stack:** Python 3.12, SQLModel/SQLAlchemy async, pytest (`uv run pytest`), chdb + SQLite local backend.

**Spec:** This plan is its own spec (refactor, behavior-preserving except: docker module-task results now travel via the `remote_task_results` table instead of a bind-mounted `result.json`). Relevant design docs to update: `docs/designs/kubernetes_runner.md`, `docs/designs/orchestration.md`.

## Global Constraints

- Do NOT touch `aaiclick/orchestration/execution/sql/*.sql`, `db_handler.py` handlers, or the claim path — frozen cross-language contract with `java/aaiclick-worker`.
- Do NOT delete `_execute_in_process` / `execution_worker_main_loop` — load-bearing for local mode (chdb single-session-per-process; see `docs/designs/testing.md`).
- All imports at top of file; no `__all__`; no history comments (CLAUDE.md).
- Reaper invariant must hold: the container/Pod never writes terminal Task status.
- Run tests with `uv run pytest <paths> -q --no-cov -p no:cacheprovider`.

---

### Task 1: `remote_result.py` — shared transport module + tests

**Files:**
- Create: `aaiclick/orchestration/execution/remote_result.py`
- Create: `aaiclick/orchestration/execution/test_remote_result.py`
- Delete: `aaiclick/orchestration/execution/test_docker_container_main.py`
- Delete: `aaiclick/orchestration/execution/test_kubernetes_pod_main.py`

**Interfaces:**
- Consumes: `RemoteTaskResult` (`..models`), `get_sql_session` (`..orch_context`), `execute_task`/`serialize_task_result` (`.runner`), `RunnerResult` (`.execution_worker`).
- Produces (used by Tasks 2–3):
  - `async def write_task_run_result(task_id: int, run_epoch: int, success: bool, result_ref: dict | None, error: str | None) -> None`
  - `async def read_task_run_result(task_id: int, run_epoch: int) -> RunnerResult | None`
  - `def collect_remote_result(exit_code: int, error: str | None, was_cancelled: bool, payload: RunnerResult | None, vehicle_name: str) -> RunnerResult`
  - `async def remote_entry_main(task_id: int, run_epoch: int) -> int`
  - CLI: `python -m aaiclick.orchestration.execution.remote_result --task-id N --run-epoch M`
  - `REMOTE_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.remote_result"]`

- [ ] **Step 1: Write `remote_result.py`** — move `_write_task_run_result`, `_read_task_run_result_row`, `_pod_main`, `_pod_cli` from `kubernetes_worker.py` (renamed per Interfaces above), plus the shared collect extracted from `_KubernetesVehicle.collect`:

```python
"""Cross-boundary task-result transport for container runners.

Both container runners (docker / kubernetes) run their module tasks in a
remote process that cannot hand a Python object back to the host worker.
The contract lives here: the container-side entrypoint executes the task
and writes a ``RemoteTaskResult`` row keyed by ``(task_id, run_epoch)``;
the host worker reads the row back after the container exits and folds it
into a ``RunnerResult`` via :func:`collect_remote_result`.

Reaper invariant: the entrypoint never writes terminal task status. It
writes only its own ``remote_task_results`` row; the host worker writes
the terminal ``Task`` status via ``_handle_task_result``.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from sqlmodel import select

from ..models import RemoteTaskResult, Task
from ..orch_context import get_sql_session, orch_context
from .execution_worker import RunnerResult
from .runner import execute_task, serialize_task_result

REMOTE_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.remote_result"]


async def write_task_run_result(
    task_id: int, run_epoch: int, success: bool, result_ref: dict | None, error: str | None
) -> None:
    """Upsert the per-attempt result row the host reads back. Keyed by
    ``(task_id, run_epoch)`` so a re-run under a new epoch never collides."""
    async with get_sql_session() as session:
        existing = (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task_id, RemoteTaskResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
        if existing is None:
            existing = RemoteTaskResult(task_id=task_id, run_epoch=run_epoch, success=success)
            session.add(existing)
        existing.success = success
        existing.result_ref = result_ref
        existing.error = error
        await session.commit()


async def read_task_run_result(task_id: int, run_epoch: int) -> RunnerResult | None:
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task_id, RemoteTaskResult.run_epoch == run_epoch
                )
            )
        ).scalar_one_or_none()
    if row is None:
        return None
    return RunnerResult(row.success, row.result_ref, row.error)


def collect_remote_result(
    exit_code: int, error: str | None, was_cancelled: bool, payload: RunnerResult | None, vehicle_name: str
) -> RunnerResult:
    """Fold an exited vehicle plus its result row into a ``RunnerResult``.

    A fired cancellation or vehicle error overrides whatever the container
    wrote — the host's explicit kill is the source of truth."""
    if was_cancelled:
        return RunnerResult(False, None, "cancelled")
    if error is not None:
        return RunnerResult(False, None, error)
    if payload is None:
        return RunnerResult(False, None, f"{vehicle_name} exited with code {exit_code} but wrote no result row")
    return payload


async def remote_entry_main(task_id: int, run_epoch: int) -> int:
    """Entry point invoked inside the container/Pod. Runs the task via the
    shared ``execute_task`` path and writes a ``RemoteTaskResult`` row for
    the host."""
    success, result_ref, error = False, None, None
    exit_code = 0
    # orch_context wraps both execution and the result write — the latter
    # needs an active SQL session.
    async with orch_context():
        try:
            async with get_sql_session() as session:
                task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()
            data_result = await execute_task(task)
            result_ref = serialize_task_result(data_result, task.job_id)
            success = True
        except BaseException as e:
            success, error, exit_code = False, f"{type(e).__name__}: {e}", 1

        await write_task_run_result(task_id, run_epoch, success, result_ref, error)
    return exit_code


def _remote_entry_cli() -> None:
    parser = argparse.ArgumentParser(
        prog="python -m aaiclick.orchestration.execution.remote_result",
        description="Container-side entrypoint for the docker and kubernetes runners.",
    )
    parser.add_argument("--task-id", type=int, required=True)
    parser.add_argument("--run-epoch", type=int, required=True)
    args = parser.parse_args()
    sys.exit(asyncio.run(remote_entry_main(args.task_id, args.run_epoch)))


if __name__ == "__main__":
    _remote_entry_cli()
```

Note: `orch_context` is imported at top of file here. Check for an import cycle (`kubernetes_worker.py` imported it lazily inside `_pod_main`); if a cycle exists at import time, keep the top-level import of `get_sql_session` and follow the CLAUDE.md circular-dependency ladder before resorting to a lazy import.

- [ ] **Step 2: Write `test_remote_result.py`** — merge the two deleted entrypoint test modules (they were near-identical) and add sync unit tests for `collect_remote_result`:

```python
"""Tests for the shared container-side entrypoint and result transport.

Entrypoint tests live in their own module because they exercise the same
code path the container/Pod would run — including ``orch_context()``
booting chdb; ``orch_ctx`` (not ``orch_ctx_no_ch``) matches a real
container. See the chdb single-session constraint in
``docs/designs/testing.md``.
"""

from __future__ import annotations

from sqlmodel import select

from ..factories import create_job
from ..models import TASK_RUNNING, RemoteTaskResult, Task
from ..orch_context import get_sql_session
from .execution_worker import RunnerResult
from .remote_result import collect_remote_result, remote_entry_main


async def _single_task(job_id: int) -> Task:
    async with get_sql_session() as session:
        return (await session.execute(select(Task).where(Task.job_id == job_id))).scalar_one()


async def _result_row(task: Task) -> RemoteTaskResult:
    async with get_sql_session() as session:
        return (
            await session.execute(
                select(RemoteTaskResult).where(
                    RemoteTaskResult.task_id == task.id, RemoteTaskResult.run_epoch == task.run_epoch
                )
            )
        ).scalar_one()


async def test_remote_entry_main_writes_success_row(orch_ctx):
    """The entrypoint runs a real task and writes a success RemoteTaskResult
    row the host reads back."""
    job = await create_job(
        "test_remote_entry_success",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )
    task = await _single_task(job.id)

    exit_code = await remote_entry_main(task.id, run_epoch=task.run_epoch)
    assert exit_code == 0

    row = await _result_row(task)
    assert row.success is True
    assert row.error is None
    # `simple_task` returns None; result_ref should reflect that
    assert row.result_ref is None


async def test_remote_entry_main_writes_failure_row_on_exception(orch_ctx):
    """Failed user code must produce a failure row and exit non-zero — the
    host depends on the exit code as a fast failure signal too."""
    job = await create_job(
        "test_remote_entry_failure",
        "aaiclick.orchestration.fixtures.sample_tasks.failing_task",
    )
    task = await _single_task(job.id)

    exit_code = await remote_entry_main(task.id, run_epoch=task.run_epoch)
    assert exit_code == 1

    row = await _result_row(task)
    assert row.success is False
    assert row.result_ref is None
    assert "intentionally" in (row.error or "")


async def test_remote_entry_main_does_not_write_terminal_task_status(orch_ctx):
    """Reaper invariant: the entrypoint never flips the Task to a terminal
    status — only run_ids / run_statuses are appended (TASK_RUNNING for the
    attempt)."""
    job = await create_job(
        "test_remote_entry_invariant",
        "aaiclick.orchestration.fixtures.sample_tasks.simple_task",
    )
    task = await _single_task(job.id)
    original_status = task.status

    await remote_entry_main(task.id, run_epoch=task.run_epoch)

    async with get_sql_session() as session:
        task_after = (await session.execute(select(Task).where(Task.id == task.id))).scalar_one()
    assert task_after.status == original_status
    assert task_after.error is None
    assert len(task_after.run_ids) == 1
    assert task_after.run_statuses == [TASK_RUNNING]


def test_collect_remote_result_returns_payload():
    payload = RunnerResult(True, {"foo": 1}, None)
    result = collect_remote_result(0, None, False, payload, "container")
    assert result == payload


def test_collect_remote_result_synthesizes_failure_when_row_missing():
    result = collect_remote_result(137, None, False, None, "container")
    assert result.success is False
    assert result.error and "exited with code 137" in result.error


def test_collect_remote_result_cancellation_overrides_payload():
    """Even if the container managed to write a success row before being
    killed, a cancellation flag must override it — the host's explicit
    kill is the source of truth."""
    payload = RunnerResult(True, {}, None)
    result = collect_remote_result(137, None, True, payload, "pod")
    assert result.success is False
    assert result.error == "cancelled"


def test_collect_remote_result_vehicle_error_overrides_payload():
    """Timeout error from the vehicle wait takes precedence over the row —
    same reasoning as cancellation."""
    result = collect_remote_result(-1, "Task timed out after 60.0s", False, None, "container")
    assert result.success is False
    assert "timed out" in (result.error or "")
```

- [ ] **Step 3: Delete the two superseded test modules** (`test_docker_container_main.py`, `test_kubernetes_pod_main.py`). Note the new module still imports nothing from `docker_worker`/`kubernetes_worker`, so it runs before Tasks 2–3 land.

- [ ] **Step 4: Run the new tests**

Run: `uv run pytest aaiclick/orchestration/execution/test_remote_result.py -q --no-cov -p no:cacheprovider`
Expected: 7 passed.

- [ ] **Step 5: Commit** — `refactor: extract shared remote result transport for container runners`

### Task 2: Kubernetes worker on `remote_result`

**Files:**
- Modify: `aaiclick/orchestration/execution/kubernetes_worker.py`
- Test: `aaiclick/orchestration/execution/test_kubernetes_worker.py` (update imports if they touch moved symbols)

**Interfaces:**
- Consumes: `REMOTE_ENTRYPOINT`, `read_task_run_result`, `collect_remote_result` from Task 1.
- Produces: `kubernetes_worker.py` public surface unchanged (`_run_task_in_pod`, `build_shell_pod_spec` — both imported by `dispatch.py`).

- [ ] **Step 1: Rewire `kubernetes_worker.py`:**
  - Delete `_pod_main`, `_write_task_run_result`, `_pod_cli`, the `if __name__ == "__main__"` block, `_read_task_run_result_row`, and now-unused imports (`argparse`, `sys`, `RemoteTaskResult`, `execute_task`, `serialize_task_result`, `get_sql_session` — keep whatever is still used).
  - Replace `POD_ENTRYPOINT = ["python", "-m", "aaiclick.orchestration.execution.kubernetes_worker"]` with `from .remote_result import REMOTE_ENTRYPOINT, collect_remote_result, read_task_run_result` and use `REMOTE_ENTRYPOINT` in `_build_pod_manifest` (the module-task `command` already appends `--task-id N --run-epoch M` — unchanged CLI shape).
  - In `_KubernetesVehicle.wait`, replace `await _read_task_run_result_row(handle.task_id, handle.run_epoch)` with `await read_task_run_result(handle.task_id, handle.run_epoch)`.
  - Replace the body of `_KubernetesVehicle.collect` with `return collect_remote_result(exit_code, error, was_cancelled, payload, "pod")`.
  - Update the module docstring: the Pod-side entrypoint now lives in `remote_result.py`.
- [ ] **Step 2: Update `test_kubernetes_worker.py`** for any moved symbol (check imports; manifest assertions that expect the `kubernetes_worker` module path in the Pod command must now expect `remote_result`).
- [ ] **Step 3: Run tests**

Run: `uv run pytest aaiclick/orchestration/execution/test_kubernetes_worker.py aaiclick/orchestration/execution/test_remote_result.py aaiclick/orchestration/execution/test_dispatch.py -q --no-cov -p no:cacheprovider`
Expected: all pass.

- [ ] **Step 4: Commit** — `refactor: kubernetes worker uses shared remote result transport`

### Task 3: Docker worker on the SQL transport (drop IPC files)

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_worker.py`
- Test: `aaiclick/orchestration/execution/test_docker_worker.py`

**Interfaces:**
- Consumes: `REMOTE_ENTRYPOINT`, `read_task_run_result`, `collect_remote_result` from Task 1.
- Produces: public surface unchanged (`_run_task_in_container`, `build_shell_run_spec`, `_docker_pull_if_registered` — imported by `dispatch.py`).

- [ ] **Step 1: Rewire `docker_worker.py`:**
  - Delete `CONTAINER_IPC_DIR`, `CONTAINER_RESULT_FILE`, `_read_result_or_synthesize_failure`, `_container_main`, `_container_cli`, the `__main__` block, and now-unused imports (`argparse`, `json`, `sys`, `tempfile`, `Path`, `select`, `Task` model import stays — used in signatures, `get_sql_session` if unused).
  - `_build_docker_run_cmd(task, image_tag, env)` — drop the `ipc_dir` parameter and the `-v` mount; the container command becomes `[*REMOTE_ENTRYPOINT, "--task-id", str(task.id), "--run-epoch", str(task.run_epoch)]`. Keep `--detach` and the explicit-`docker rm` design (the `docker wait` race note still applies).
  - `_DockerHandle` becomes `NamedTuple` of `(container_id: str, task_id: int, run_epoch: int)`.
  - `_DockerVehicle.__init__(image_tag, env)` — no `ipc_dir`. `launch` returns `_DockerHandle(container_id, task.id, task.run_epoch)`.
  - `_DockerVehicle.wait` — after `_wait_for_container`, read the row: `result_row = await read_task_run_result(handle.task_id, handle.run_epoch)` and return `(exit_code, error, result_row)`; the payload type parameter becomes `RunnerResult | None` (mirror `_KubernetesVehicle`).
  - `_DockerVehicle.collect` — `return collect_remote_result(exit_code, error, was_cancelled, payload, "container")`.
  - `_run_task_in_container` — drop the `TemporaryDirectory` block; body becomes the same shape as `_run_task_in_pod`.
  - Update the module docstring: transport is now the `RemoteTaskResult` row (entrypoint in `remote_result.py`); keep the reaper-invariant paragraph, pointing at `remote_result`.
- [ ] **Step 2: Update `test_docker_worker.py`:**
  - `test_module_cmd_uses_bootstrap_shim` / `test_build_docker_run_cmd_shape`: no `ipc_dir` arg; assert the command references `aaiclick.orchestration.execution.remote_result`, `--task-id` and `--run-epoch`; assert no `-v` mount and still no `--rm`.
  - Delete the five `_read_result_*` tests (superseded by `collect_remote_result` tests in Task 1).
  - `test_run_task_in_container_cancellation_flag_overrides_result`: replace the result.json fake with `monkeypatch.setattr(docker_worker, "read_task_run_result", AsyncMock(return_value=RunnerResult(True, {}, None)))` — stale success row written before the kill; assert the cancellation still wins.
- [ ] **Step 3: Run tests**

Run: `uv run pytest aaiclick/orchestration/execution/test_docker_worker.py aaiclick/orchestration/execution/test_dispatch.py aaiclick/orchestration/execution/test_remote_result.py -q --no-cov -p no:cacheprovider`
Expected: all pass.

- [ ] **Step 4: Commit** — `refactor: docker runner reads results from remote_task_results instead of IPC files`

### Task 4: `run_job_tasks` delegates status writes to `claiming`

**Files:**
- Modify: `aaiclick/orchestration/execution/runner.py:613-724`

**Interfaces:**
- Consumes: `update_task_status`, `update_job_status` from `.claiming` (already imported nearby — `runner.py` currently imports nothing from `claiming`; add the import at top).
- Produces: `run_job_tasks(job)` signature and contract unchanged (single-job, fail-fast, sets job COMPLETED/FAILED, refreshes the in-memory `job`).

- [ ] **Step 1: Rewrite the task-execution body.** Keep `_READY_TASK_SQL` and the job-start block; replace the hand-rolled RUNNING/COMPLETED/FAILED session mutations with the shared helpers (they perform the identical writes: `started_at`/`completed_at`, `error`/`result`, last `run_statuses` entry):

```python
    # Fetch and execute one task at a time until no more ready tasks
    while True:
        async with get_sql_session() as session:
            now = utc_now()
            result = await session.execute(
                text(_READY_TASK_SQL),
                {
                    "job_id": job.id,
                    "pending_status": TASK_PENDING,
                    "completed_status": TASK_COMPLETED,
                    "now": now,
                },
            )
            row = result.fetchone()

        if row is None:
            break

        task_id = row[0]
        await update_task_status(task_id, TASK_RUNNING)
        async with get_sql_session() as session:
            task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()

        try:
            data_result = await execute_task(task)
            result_ref = serialize_task_result(data_result, task.job_id)
            await update_task_status(task_id, TASK_COMPLETED, result=result_ref)
        except Exception as e:
            job_failed = True
            error_msg = str(e)
            logger.exception("Task %r failed: %s", task.name, e)
            await update_task_status(task_id, TASK_FAILED, error=str(e))
            break

    await update_job_status(job.id, JOB_FAILED if job_failed else JOB_COMPLETED, error=error_msg)
    async with get_sql_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        job.status = db_job.status
        job.completed_at = db_job.completed_at
        job.error = db_job.error
```

  Notes: `update_task_status(..., TASK_COMPLETED, result=...)` only stores a truthy result — same net effect as before for `None`/empty refs (column already NULL). Import `update_job_status`/`update_task_status` from `.claiming` at top of `runner.py`; drop the now-unused `JOB_RUNNING`… no — the job-start block still uses `JOB_RUNNING`; drop only imports that become unused (`TASK_RUNNING` stays, `TASK_FAILED` stays; check each).
- [ ] **Step 2: Run the harness tests**

Run: `uv run pytest aaiclick/orchestration/execution/test_execution.py aaiclick/orchestration/test_object_lifecycle_e2e.py -q --no-cov -p no:cacheprovider`
Expected: all pass (these exercise `run_job_tasks` success, failure, shell, logging, and lifecycle paths).

- [ ] **Step 3: Commit** — `refactor: run_job_tasks reuses shared task/job status transitions`

### Task 5: Docs + full-suite verification

**Files:**
- Modify: `docs/designs/kubernetes_runner.md` (result.json references, `_pod_main` location)
- Modify: `docs/designs/orchestration.md:174-193` (bootstrap-shim paragraph and implementation references)
- Modify: `docs/designs/task_log_retirement.md:65` (docker `result.json` mention)

- [ ] **Step 1: Update the three design docs** to describe the unified transport (`remote_result.py`: entrypoint + `RemoteTaskResult` row for both runners). Use the `markdown-style` skill conventions; run the `shortify` skill after editing.
- [ ] **Step 2: Full local test run**

Run: `uv run pytest aaiclick -q --no-cov -p no:cacheprovider`
Expected: no failures (skips for live/e2e markers are fine).

- [ ] **Step 3: Commit and push** — `docs: document unified container result transport`; push branch `claude/project-simplification-eval-mzkuj4` with `git push -u origin`.

## Self-Review

- Spec coverage: transport unification (Tasks 1–3), `run_job_tasks` dedup (Task 4), docs (Task 5). `_execute_in_process` intentionally untouched (constraint).
- Placeholders: none — all code inline.
- Type consistency: `RunnerResult | None` payload type used by both vehicles; `collect_remote_result` signature identical at both call sites; `REMOTE_ENTRYPOINT` list shape matches both `_build_pod_manifest` command splicing and `_build_docker_run_cmd` extension.
