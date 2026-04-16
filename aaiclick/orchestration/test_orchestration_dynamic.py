"""Execution tests for dynamic task creation (map) via ajob_test."""

import tempfile
from pathlib import Path

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import tasks_list
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JobStatus
from aaiclick.orchestration.operators import map

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
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)
        output_file = str(Path(tmpdir) / "output.txt")

        j = await map_basic_pipeline(output_file=output_file)
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines) == ["10", "20", "30", "40", "50"]


async def test_map_execution_with_kwargs(orch_ctx, monkeypatch):
    """map() forwards extra kwargs to callback."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)
        output_file = str(Path(tmpdir) / "output.txt")

        j = await map_kwargs_pipeline(output_file=output_file, factor=3)
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines, key=int) == ["30", "60", "90", "120", "150"]


async def test_map_execution_multiple_partitions(orch_ctx, monkeypatch):
    """map() with small partition size creates multiple _map_part tasks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setenv("AAICLICK_LOG_DIR", tmpdir)
        output_file = str(Path(tmpdir) / "output.txt")

        j = await map_partitions_pipeline(output_file=output_file)
        await ajob_test(j)

        assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines) == ["10", "20", "30", "40", "50"]
