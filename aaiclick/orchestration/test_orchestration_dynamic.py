"""Execution tests for dynamic task creation (map) via ajob_test."""

import tempfile
from pathlib import Path
from unittest.mock import AsyncMock

from sqlalchemy import text
from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import tasks_list
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.execution.execution_worker import execution_worker_main_loop
from aaiclick.orchestration.execution.runner import run_job_tasks
from aaiclick.orchestration.models import JOB_COMPLETED, Task
from aaiclick.orchestration.operators import map
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.sql_context import _sql_engine_var

# --- Task fixtures ---


@task
async def create_test_data() -> Object:
    """Create an Object with integer values [10, 20, 30, 40, 50]."""
    return await create_object_from_value([10, 20, 30, 40, 50])


@task
async def row_writer(row, output_file: str):
    """Write each row value to a file, one per line."""
    with Path(output_file).open("a") as f:
        f.write(f"{row}\n")


@task
async def row_writer_with_factor(row, factor: int, output_file: str):
    """Write row * factor to a file."""
    with Path(output_file).open("a") as f:
        f.write(f"{row * factor}\n")


# --- Job pipelines (must be module-level for entrypoint resolution) ---


@job("test_map_basic")
def map_basic_pipeline(output_file: str):
    data = create_test_data()
    group = map(
        cbk=row_writer,
        obj=data,
        partition=5000,
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, group)


@job("test_map_kwargs_exec")
def map_kwargs_pipeline(output_file: str, factor: int):
    data = create_test_data()
    group = map(
        cbk=row_writer_with_factor,
        obj=data,
        partition=5000,
        kwargs={"factor": factor, "output_file": output_file},
    )
    return tasks_list(data, group)


@job("test_map_partitions")
def map_partitions_pipeline(output_file: str):
    data = create_test_data()
    group = map(
        cbk=row_writer,
        obj=data,
        partition=2,
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, group)


# --- Execution tests ---


async def test_map_execution_basic(orch_ctx, monkeypatch):
    """map() end-to-end: creates partitions, runs callback on each row."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await map_basic_pipeline(output_file=output_file)
        await ajob_test(j)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines) == ["10", "20", "30", "40", "50"]


async def test_map_execution_with_kwargs(orch_ctx, monkeypatch):
    """map() forwards extra kwargs to callback."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await map_kwargs_pipeline(output_file=output_file, factor=3)
        await ajob_test(j)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines, key=int) == ["30", "60", "90", "120", "150"]


async def test_map_execution_multiple_partitions(orch_ctx, monkeypatch):
    """map() with small partition size creates multiple _map_part tasks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await map_partitions_pipeline(output_file=output_file)
        await ajob_test(j)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines) == ["10", "20", "30", "40", "50"]


async def test_map_pins_tables_for_children(orch_ctx):
    """Between the expander's exit and the first ``_map_part`` run, the source
    and output tables are held only by pins for the children — so the drop
    sweep must leave both alone, and the job must still complete."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")
        j = await map_partitions_pipeline(output_file=output_file)

        # Pipeline entry, the data producer, then the expander; no child has run yet.
        assert await execution_worker_main_loop(max_tasks=3, install_signal_handlers=False, max_empty_polls=1) == 3

        async with get_sql_session() as session:
            child_ids = set(
                (
                    await session.execute(
                        select(Task.id).where(
                            Task.job_id == j.id, Task.entrypoint == "aaiclick.orchestration.operators._map_part"
                        )
                    )
                ).scalars()
            )
            rows = (await session.execute(text("SELECT table_name, task_id FROM table_pin_refs"))).fetchall()
        assert len(child_ids) == 3
        pinned = {(table, task_id) for table, task_id in rows if task_id in child_ids}
        tables = {table for table, _ in pinned}
        assert len(tables) == 2, f"expected source + output pinned, got {tables}"
        assert pinned == {(table, cid) for table in tables for cid in child_ids}

        engine = _sql_engine_var.get()
        assert engine is not None
        sweeper = BackgroundWorker()
        sweeper._engine = engine
        sweeper._ch_client = AsyncMock()
        await sweeper._cleanup_unreferenced_tables()
        async with get_sql_session() as session:
            kept = set((await session.execute(text("SELECT DISTINCT table_name FROM table_context_refs"))).scalars())
        assert tables <= kept

        await run_job_tasks(j)
        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        assert sorted(Path(output_file).read_text().split()) == ["10", "20", "30", "40", "50"]
