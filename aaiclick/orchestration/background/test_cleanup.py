"""Tests for background worker cleanup logic.

Verifies that _cleanup_unreferenced_tables respects table_pin_refs
and table_run_refs for table protection.
Also tests clean_task_run for crash recovery.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from aaiclick.orchestration.background.handler import BackgroundHandler
from aaiclick.orchestration.background.sqlite_handler import SqliteBackgroundHandler

from .conftest import (
    get_run_refs,
    insert_context_ref,
    insert_job,
    insert_pin_ref,
    insert_run_ref,
    insert_table_registry,
    make_worker,
)


async def _table_names(engine, table: str) -> set[str]:
    async with AsyncSession(engine) as session:
        result = await session.execute(text(f"SELECT DISTINCT table_name FROM {table}"))
        return {row[0] for row in result.fetchall()}


async def test_cleanup_skips_pinned_tables(bg_db):
    """Tables with a pin_ref are NOT dropped even when no run refs exist."""
    await insert_context_ref(bg_db, "t_unpinned", 100)
    await insert_context_ref(bg_db, "t_pinned", 200)
    await insert_pin_ref(bg_db, "t_pinned", 300)

    worker = make_worker(bg_db)

    await worker._cleanup_unreferenced_tables()

    remaining = await _table_names(bg_db, "table_context_refs")
    assert "t_pinned" in remaining, "Pinned table was dropped"
    assert "t_unpinned" not in remaining, "Unpinned table was not cleaned up"


async def test_cleanup_skips_tables_with_active_runs(bg_db):
    """Tables with run refs in table_run_refs are NOT dropped."""
    await insert_context_ref(bg_db, "t_active", 100)
    await insert_run_ref(bg_db, "t_active", "run_1")
    await insert_context_ref(bg_db, "t_empty", 200)

    worker = make_worker(bg_db)

    await worker._cleanup_unreferenced_tables()

    remaining = await _table_names(bg_db, "table_context_refs")
    assert "t_active" in remaining, "Active table was dropped"
    assert "t_empty" not in remaining, "Empty table was not cleaned up"


async def test_clean_task_run_removes_run_refs(bg_db):
    """clean_task_run deletes all table_run_refs rows for that run_id."""
    await insert_run_ref(bg_db, "t1", "run_1")
    await insert_run_ref(bg_db, "t1", "run_2")
    await insert_run_ref(bg_db, "t2", "run_1")
    await insert_run_ref(bg_db, "t3", "run_3")

    async with AsyncSession(bg_db) as session:
        await BackgroundHandler.clean_task_run(session, "run_1")
        await session.commit()

    assert await get_run_refs(bg_db, "t1") == {"run_2"}
    assert await get_run_refs(bg_db, "t2") == set()
    assert await get_run_refs(bg_db, "t3") == {"run_3"}


async def test_clean_task_runs_batch_removes_multiple_run_ids(bg_db):
    """clean_task_runs batch-deletes table_run_refs rows for multiple run_ids."""
    await insert_run_ref(bg_db, "t1", "run_1")
    await insert_run_ref(bg_db, "t1", "run_2")
    await insert_run_ref(bg_db, "t2", "run_2")
    await insert_run_ref(bg_db, "t3", "run_3")

    handler = SqliteBackgroundHandler()
    async with AsyncSession(bg_db) as session:
        await handler.clean_task_runs(session, ["run_1", "run_2"])
        await session.commit()

    assert await get_run_refs(bg_db, "t1") == set()
    assert await get_run_refs(bg_db, "t2") == set()
    assert await get_run_refs(bg_db, "t3") == {"run_3"}


async def test_clean_task_run_then_cleanup_drops_table(bg_db):
    """After clean_task_run removes run refs, cleanup drops the table."""
    await insert_context_ref(bg_db, "t_orphan", 100)
    await insert_run_ref(bg_db, "t_orphan", "crashed_run")

    async with AsyncSession(bg_db) as session:
        await BackgroundHandler.clean_task_run(session, "crashed_run")
        await session.commit()

    worker = make_worker(bg_db)

    await worker._cleanup_unreferenced_tables()

    remaining = await _table_names(bg_db, "table_context_refs")
    assert "t_orphan" not in remaining, "Orphaned table was not cleaned up"


async def test_cleanup_full_mode_skips_drop(bg_db):
    """Tables belonging to a FULL-mode job are preserved by cleanup."""
    await insert_job(bg_db, 777, preservation_mode="FULL")
    await insert_context_ref(bg_db, "t_full", 100)
    await insert_table_registry(bg_db, "t_full", job_id=777)

    worker = make_worker(bg_db)

    await worker._cleanup_unreferenced_tables()

    # Table still present, no drop was attempted on CH.
    remaining = await _table_names(bg_db, "table_context_refs")
    assert "t_full" in remaining
    worker._ch_client.command.assert_not_called()


async def test_cleanup_none_mode_drops(bg_db):
    """Tables belonging to a NONE-mode job are dropped as normal."""
    await insert_job(bg_db, 888, preservation_mode="NONE")
    await insert_context_ref(bg_db, "t_none", 100)
    await insert_table_registry(bg_db, "t_none", job_id=888)

    worker = make_worker(bg_db)

    await worker._cleanup_unreferenced_tables()

    remaining = await _table_names(bg_db, "table_context_refs")
    assert "t_none" not in remaining


async def _fail_drop_of(sql: str) -> None:
    if "t_stuck" in sql:
        raise RuntimeError("ClickHouse unavailable")


async def test_cleanup_keeps_refs_when_drop_fails(bg_db):
    """A table whose DROP failed stays registered so the next sweep retries it."""
    await insert_context_ref(bg_db, "t_stuck", 100)
    await insert_table_registry(bg_db, "t_stuck")
    await insert_context_ref(bg_db, "t_fine", 101)
    await insert_table_registry(bg_db, "t_fine")

    worker = make_worker(bg_db)
    worker._ch_client.command.side_effect = _fail_drop_of

    await worker._cleanup_unreferenced_tables()

    assert await _table_names(bg_db, "table_context_refs") == {"t_stuck"}
    assert await _table_names(bg_db, "table_registry") == {"t_stuck"}


async def test_orphan_cleanup_keeps_registry_row_when_drop_fails(bg_db):
    """The orphan sweep deletes registry rows only for tables it actually dropped."""
    await insert_table_registry(bg_db, "t_stuck")
    await insert_table_registry(bg_db, "t_fine")

    worker = make_worker(bg_db)
    worker._ch_client.command.side_effect = _fail_drop_of

    await worker._cleanup_orphaned_resources(ttl_days=0)

    assert await _table_names(bg_db, "table_registry") == {"t_stuck"}


async def test_delete_job_data_keeps_job_until_every_table_drops(bg_db):
    """Job expiry forgets the dropped tables, then raises so the job is retried next cycle."""
    await insert_job(bg_db, 998, status="COMPLETED")
    await insert_table_registry(bg_db, "t_stuck", job_id=998)
    await insert_table_registry(bg_db, "t_fine", job_id=998)

    worker = make_worker(bg_db)
    worker._ch_client.command.side_effect = _fail_drop_of

    with pytest.raises(RuntimeError, match="not dropped yet"):
        await worker._delete_job_data(998)

    assert await _table_names(bg_db, "table_registry") == {"t_stuck"}
    async with AsyncSession(bg_db) as session:
        assert (await session.execute(text("SELECT id FROM jobs WHERE id = 998"))).scalar_one() == 998


async def test_drop_tables_gives_up_after_consecutive_failures(bg_db):
    """An unreachable ClickHouse costs a bounded number of attempts per sweep, and nothing is forgotten."""
    for i in range(5):
        await insert_context_ref(bg_db, f"t_{i}", 100 + i)

    worker = make_worker(bg_db)
    worker._ch_client.command.side_effect = RuntimeError("ClickHouse unavailable")

    await worker._cleanup_unreferenced_tables()

    assert worker._ch_client.command.call_count == 3
    assert await _table_names(bg_db, "table_context_refs") == {f"t_{i}" for i in range(5)}


async def test_cleanup_skips_persistent_and_job_scoped_tables(bg_db):
    """``p_*`` and ``j_<id>_*`` tables are exempt from refcount-based cleanup."""
    await insert_context_ref(bg_db, "p_user_catalog", 100)
    await insert_context_ref(bg_db, "j_42_intermediate", 101)
    await insert_context_ref(bg_db, "t_scratch", 102)

    worker = make_worker(bg_db)

    await worker._cleanup_unreferenced_tables()

    remaining = await _table_names(bg_db, "table_context_refs")
    assert "p_user_catalog" in remaining, "Persistent global table was dropped"
    assert "j_42_intermediate" in remaining, "Job-scoped table was dropped before TTL"
    assert "t_scratch" not in remaining, "Temp table was not cleaned up"


async def test_delete_job_data_exempts_persistent_tables(bg_db):
    """``_delete_job_data`` drops ``t_*`` and ``j_*`` but never ``p_*``."""
    job_id = 555
    await insert_job(bg_db, job_id, preservation_mode="NONE")
    await insert_table_registry(bg_db, "p_user_catalog", job_id=job_id)
    await insert_table_registry(bg_db, "j_555_intermediate", job_id=job_id)
    await insert_table_registry(bg_db, "t_scratch", job_id=job_id)

    worker = make_worker(bg_db)

    await worker._delete_job_data(job_id)

    dropped = {
        call.args[0].split("IF EXISTS ", 1)[1]
        for call in worker._ch_client.command.call_args_list
        if "DROP TABLE" in call.args[0]
    }
    assert "p_user_catalog" not in dropped, "User-managed p_* table was dropped on job TTL"
    assert "j_555_intermediate" in dropped
    assert "t_scratch" in dropped


async def test_delete_job_data_purges_ch_log_tables(bg_db):
    """``_delete_job_data`` deletes the job's operation_log and task_logs rows."""
    job_id = 556
    await insert_job(bg_db, job_id, preservation_mode="NONE")

    worker = make_worker(bg_db)

    await worker._delete_job_data(job_id)

    commands = [call.args[0] for call in worker._ch_client.command.call_args_list]
    assert f"ALTER TABLE operation_log DELETE WHERE job_id = {job_id}" in commands
    assert f"ALTER TABLE task_logs DELETE WHERE job_id = {job_id}" in commands
