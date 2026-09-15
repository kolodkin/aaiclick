"""Direct unit tests for try_complete_job.

Pins the contract independently of the integration paths in BackgroundWorker
and worker: when all tasks are terminal, a job transitions to COMPLETED
(or FAILED if any task failed); otherwise it stays untouched.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.orchestration.background.handler import (
    GROUP_SIBLING_ABORTED_ERROR,
    JOB_FAILED_ERROR,
    UPSTREAM_FAILED_ERROR,
    roll_up_job,
    try_complete_job,
)

from ...datetime_utils import utc_now
from .conftest import insert_job


async def _insert_tasks(engine, job_id, statuses):
    """Insert one task per status for a given job."""
    async with AsyncSession(engine) as session:
        for idx, status in enumerate(statuses):
            await session.execute(
                text(
                    "INSERT INTO tasks (id, job_id, entrypoint, name, kwargs, "
                    "status, created_at, max_retries, attempt, run_statuses) "
                    "VALUES (:id, :job_id, 'test.func', 'test', '{}', "
                    ":status, :now, 0, 0, '[]')"
                ),
                {
                    "id": job_id * 100 + idx,
                    "job_id": job_id,
                    "status": status,
                    "now": utc_now(),
                },
            )
        await session.commit()


async def _insert_task(engine, *, task_id, job_id, status, group_id=None):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO tasks (id, job_id, group_id, entrypoint, name, kwargs, "
                "status, created_at, max_retries, attempt, run_statuses) "
                "VALUES (:id, :job_id, :group_id, 'test.func', 'test', '{}', "
                ":status, :now, 0, 0, '[]')"
            ),
            {
                "id": task_id,
                "job_id": job_id,
                "group_id": group_id,
                "status": status,
                "now": utc_now(),
            },
        )
        await session.commit()


async def _insert_group(engine, *, group_id, job_id, name="g"):
    async with AsyncSession(engine) as session:
        await session.execute(
            text("INSERT INTO groups (id, job_id, name, created_at) VALUES (:id, :job_id, :name, :now)"),
            {"id": group_id, "job_id": job_id, "name": name, "now": utc_now()},
        )
        await session.commit()


async def _insert_dependency(engine, *, previous_id, previous_type, next_id, next_type):
    async with AsyncSession(engine) as session:
        await session.execute(
            text(
                "INSERT INTO dependencies (previous_id, previous_type, next_id, next_type, created_at) "
                "VALUES (:pid, :pt, :nid, :nt, :now)"
            ),
            {"pid": previous_id, "pt": previous_type, "nid": next_id, "nt": next_type, "now": utc_now()},
        )
        await session.commit()


async def _get_task(engine, task_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT status, error FROM tasks WHERE id = :id"),
            {"id": task_id},
        )
        row = result.fetchone()
        assert row is not None
        return row


async def _get_job(engine, job_id):
    async with AsyncSession(engine) as session:
        result = await session.execute(
            text("SELECT status, completed_at, error FROM jobs WHERE id = :id"),
            {"id": job_id},
        )
        row = result.fetchone()
        assert row is not None
        return row


async def _run_try_complete(engine, job_id):
    async with AsyncSession(engine) as session:
        await try_complete_job(session, job_id)
        await session.commit()


async def _run_roll_up(engine, job_id):
    async with AsyncSession(engine) as session:
        await roll_up_job(session, job_id)
        await session.commit()


async def test_try_complete_job_empty_job_is_noop(bg_db):
    """Job with no tasks is a no-op — stays as-is."""
    await insert_job(bg_db, 1)

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "RUNNING"
    assert completed_at is None
    assert error is None


async def test_try_complete_job_all_completed_marks_completed(bg_db):
    """All tasks COMPLETED → job COMPLETED with completed_at set."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "COMPLETED", "COMPLETED"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "COMPLETED"
    assert completed_at is not None
    assert error is None


async def test_try_complete_job_any_failed_marks_failed(bg_db):
    """Any FAILED task among terminal statuses → job FAILED with error message."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "FAILED", "COMPLETED"])

    await _run_try_complete(bg_db, 1)

    status, completed_at, error = await _get_job(bg_db, 1)
    assert status == "FAILED"
    assert completed_at is not None
    assert error == JOB_FAILED_ERROR


@pytest.mark.parametrize("non_terminal", ["PENDING", "CLAIMED", "RUNNING", "PENDING_CLEANUP"])
async def test_try_complete_job_non_terminal_is_noop(bg_db, non_terminal):
    """Any non-terminal task → job stays RUNNING, no completed_at set."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", non_terminal])

    await _run_try_complete(bg_db, 1)

    status, completed_at, _ = await _get_job(bg_db, 1)
    assert status == "RUNNING"
    assert completed_at is None


async def test_try_complete_job_cancelled_counts_as_terminal_non_failed(bg_db):
    """CANCELLED tasks are terminal and not failed → job COMPLETED."""
    await insert_job(bg_db, 1)
    await _insert_tasks(bg_db, 1, ["COMPLETED", "CANCELLED"])

    await _run_try_complete(bg_db, 1)

    status, _, _ = await _get_job(bg_db, 1)
    assert status == "COMPLETED"


# --- Cascade UPSTREAM_FAILED tests ---


async def test_roll_up_job_all_completed_marks_completed(bg_db):
    """The shared worker recipe: all tasks terminal → job COMPLETED."""
    await insert_job(bg_db, 61)
    await _insert_tasks(bg_db, 61, ["COMPLETED", "COMPLETED"])

    await _run_roll_up(bg_db, 61)

    status, completed_at, error = await _get_job(bg_db, 61)
    assert status == "COMPLETED"
    assert completed_at is not None
    assert error is None


async def test_roll_up_job_any_failed_marks_failed(bg_db):
    await insert_job(bg_db, 62)
    await _insert_tasks(bg_db, 62, ["COMPLETED", "FAILED"])

    await _run_roll_up(bg_db, 62)

    status, _, error = await _get_job(bg_db, 62)
    assert status == "FAILED"
    assert error == JOB_FAILED_ERROR


async def test_roll_up_job_non_terminal_is_noop(bg_db):
    await insert_job(bg_db, 63)
    await _insert_tasks(bg_db, 63, ["COMPLETED", "RUNNING"])

    await _run_roll_up(bg_db, 63)

    status, completed_at, _ = await _get_job(bg_db, 63)
    assert status == "RUNNING"
    assert completed_at is None


async def test_roll_up_job_does_not_cascade(bg_db):
    """Rollup-only by contract: a PENDING task stranded behind a FAILED
    upstream stays untouched — the UPSTREAM_FAILED sweep belongs to the
    failure-transition owners (try_complete_job), never a worker's
    success-path rollup."""
    await insert_job(bg_db, 64)
    await _insert_task(bg_db, task_id=6401, job_id=64, status="FAILED")
    await _insert_task(bg_db, task_id=6402, job_id=64, status="PENDING")
    await _insert_dependency(bg_db, previous_id=6401, previous_type="task", next_id=6402, next_type="task")

    await _run_roll_up(bg_db, 64)

    task_status, _ = await _get_task(bg_db, 6402)
    assert task_status == "PENDING"
    job_status, _, _ = await _get_job(bg_db, 64)
    assert job_status == "RUNNING"


async def test_cascade_task_to_task(bg_db):
    """A FAILED → B PENDING: B becomes UPSTREAM_FAILED, job FAILED."""
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED")
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=102, next_type="task")

    await _run_try_complete(bg_db, 1)

    status, error = await _get_task(bg_db, 102)
    assert status == "UPSTREAM_FAILED"
    assert error == UPSTREAM_FAILED_ERROR
    job_status, _, job_error = await _get_job(bg_db, 1)
    assert job_status == "FAILED"
    assert job_error == JOB_FAILED_ERROR


async def test_cascade_transitive_chain(bg_db):
    """A FAILED → B → C → D: a single try_complete_job pass marks B, C, D UPSTREAM_FAILED."""
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED")
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING")
    await _insert_task(bg_db, task_id=103, job_id=1, status="PENDING")
    await _insert_task(bg_db, task_id=104, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=102, next_type="task")
    await _insert_dependency(bg_db, previous_id=102, previous_type="task", next_id=103, next_type="task")
    await _insert_dependency(bg_db, previous_id=103, previous_type="task", next_id=104, next_type="task")

    await _run_try_complete(bg_db, 1)

    for tid in (102, 103, 104):
        status, _ = await _get_task(bg_db, tid)
        assert status == "UPSTREAM_FAILED", f"task {tid}"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


@pytest.mark.parametrize(
    "upstream_status, expected_downstream, expected_job",
    [
        # CANCELLED upstream cascades to downstream the same way FAILED does, and
        # UPSTREAM_FAILED downstream counts as a failure for the job.
        pytest.param("CANCELLED", "UPSTREAM_FAILED", "FAILED", id="cancelled-cascades"),
        # COMPLETED upstream is the success case — downstream stays PENDING.
        pytest.param("COMPLETED", "PENDING", "RUNNING", id="completed-does-not-propagate"),
    ],
)
async def test_cascade_from_upstream(bg_db, upstream_status, expected_downstream, expected_job):
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status=upstream_status)
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=102, next_type="task")

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 102)
    assert status == expected_downstream
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == expected_job


async def test_cascade_group_to_task_propagates_on_first_failure(bg_db):
    """group→task: a failure in the upstream group cascades to downstream-of-group.

    The failing group's still-running sibling (102) is aborted by the
    fail-fast sweep, so the whole job reaches a terminal FAILED state."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=102, job_id=1, status="RUNNING", group_id=500)
    await _insert_task(bg_db, task_id=103, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=500, previous_type="group", next_id=103, next_type="task")

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 103)
    assert status == "UPSTREAM_FAILED"
    # Sibling 102 is aborted (fail-fast), so the job reaches terminal FAILED.
    status_102, _ = await _get_task(bg_db, 102)
    assert status_102 == "CANCELLED"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


async def test_cascade_task_to_group(bg_db):
    """task→group: when prev task fails, tasks inside the dependent group are UPSTREAM_FAILED."""
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED")
    await _insert_group(bg_db, group_id=600, job_id=1)
    await _insert_task(bg_db, task_id=201, job_id=1, status="PENDING", group_id=600)
    await _insert_task(bg_db, task_id=202, job_id=1, status="PENDING", group_id=600)
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=600, next_type="group")

    await _run_try_complete(bg_db, 1)

    for tid in (201, 202):
        status, _ = await _get_task(bg_db, tid)
        assert status == "UPSTREAM_FAILED", f"task {tid}"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


async def test_cascade_group_to_group(bg_db):
    """group→group: a failure in upstream group cascades to tasks in downstream group."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1, name="up")
    await _insert_group(bg_db, group_id=700, job_id=1, name="down")
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=201, job_id=1, status="PENDING", group_id=700)
    await _insert_dependency(bg_db, previous_id=500, previous_type="group", next_id=700, next_type="group")

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 201)
    assert status == "UPSTREAM_FAILED"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


async def test_cascade_does_not_touch_claimed_or_running(bg_db):
    """Only PENDING tasks are cascaded. CLAIMED/RUNNING are not — they were claimed before upstream failed."""
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED")
    await _insert_task(bg_db, task_id=102, job_id=1, status="CLAIMED")
    await _insert_task(bg_db, task_id=103, job_id=1, status="RUNNING")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=102, next_type="task")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=103, next_type="task")

    await _run_try_complete(bg_db, 1)

    status_102, _ = await _get_task(bg_db, 102)
    status_103, _ = await _get_task(bg_db, 103)
    assert status_102 == "CLAIMED"
    assert status_103 == "RUNNING"
    # Job stays RUNNING because tasks 102/103 are still non-terminal.
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "RUNNING"


# --- Fail-fast group-sibling abort tests (default behavior) ---


@pytest.mark.parametrize("sibling_status", ["PENDING", "CLAIMED", "RUNNING", "PENDING_CLEANUP"])
async def test_group_sibling_abort_cancels_active_sibling(bg_db, sibling_status):
    """A FAILED group member cancels its still-active sibling regardless of how
    far the sibling had progressed."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=102, job_id=1, status=sibling_status, group_id=500)

    await _run_try_complete(bg_db, 1)

    status, error = await _get_task(bg_db, 102)
    assert status == "CANCELLED"
    assert error == GROUP_SIBLING_ABORTED_ERROR
    # 101 FAILED + 102 CANCELLED → all terminal → job FAILED.
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


async def test_group_sibling_abort_does_not_touch_completed_sibling(bg_db):
    """The completed-race: a sibling that already reached COMPLETED is terminal
    and must not be clobbered to CANCELLED by the abort sweep."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=102, job_id=1, status="COMPLETED", group_id=500)

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 102)
    assert status == "COMPLETED"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


async def test_group_sibling_abort_only_aborts_the_failing_group(bg_db):
    """Abort is scoped to the group with the failure — a healthy parallel group
    keeps running."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1, name="doomed")
    await _insert_group(bg_db, group_id=600, job_id=1, name="healthy")
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING", group_id=500)
    await _insert_task(bg_db, task_id=201, job_id=1, status="RUNNING", group_id=600)
    await _insert_task(bg_db, task_id=202, job_id=1, status="RUNNING", group_id=600)

    await _run_try_complete(bg_db, 1)

    status_102, _ = await _get_task(bg_db, 102)
    assert status_102 == "CANCELLED"
    for tid in (201, 202):
        status, _ = await _get_task(bg_db, tid)
        assert status == "RUNNING", f"task {tid}"
    # Healthy group still running → job stays RUNNING.
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "RUNNING"


async def test_group_sibling_abort_cascades_downstream(bg_db):
    """A sibling cancelled by the abort sweep propagates onward: a task depending
    on the now-CANCELLED sibling is marked UPSTREAM_FAILED in the same pass."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING", group_id=500)
    await _insert_task(bg_db, task_id=103, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=102, previous_type="task", next_id=103, next_type="task")

    await _run_try_complete(bg_db, 1)

    status_102, _ = await _get_task(bg_db, 102)
    status_103, _ = await _get_task(bg_db, 103)
    assert status_102 == "CANCELLED"
    assert status_103 == "UPSTREAM_FAILED"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"
