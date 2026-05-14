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
    JOB_FAILED_ERROR,
    UPSTREAM_FAILED_ERROR,
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


async def test_cascade_from_cancelled_upstream(bg_db):
    """CANCELLED upstream cascades to downstream the same way FAILED does."""
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="CANCELLED")
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=102, next_type="task")

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 102)
    assert status == "UPSTREAM_FAILED"
    # CANCELLED upstream + UPSTREAM_FAILED downstream → job FAILED (UPSTREAM_FAILED counts as failure).
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "FAILED"


async def test_cascade_completed_upstream_does_not_propagate(bg_db):
    """COMPLETED upstream is the success case — downstream stays PENDING."""
    await insert_job(bg_db, 1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="COMPLETED")
    await _insert_task(bg_db, task_id=102, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=101, previous_type="task", next_id=102, next_type="task")

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 102)
    assert status == "PENDING"
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "RUNNING"


async def test_cascade_group_to_task_propagates_on_first_failure(bg_db):
    """group→task: a failure in the upstream group cascades to downstream-of-group
    even while sibling group members are still running (Airflow all_success semantics)."""
    await insert_job(bg_db, 1)
    await _insert_group(bg_db, group_id=500, job_id=1)
    await _insert_task(bg_db, task_id=101, job_id=1, status="FAILED", group_id=500)
    await _insert_task(bg_db, task_id=102, job_id=1, status="RUNNING", group_id=500)
    await _insert_task(bg_db, task_id=103, job_id=1, status="PENDING")
    await _insert_dependency(bg_db, previous_id=500, previous_type="group", next_id=103, next_type="task")

    await _run_try_complete(bg_db, 1)

    status, _ = await _get_task(bg_db, 103)
    assert status == "UPSTREAM_FAILED"
    # Job stays RUNNING because sibling task 102 in the upstream group is still RUNNING.
    job_status, _, _ = await _get_job(bg_db, 1)
    assert job_status == "RUNNING"


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
