"""Execution tests for dynamic task creation (map) via ajob_test."""

import tempfile
from pathlib import Path

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.data.object import Object
from aaiclick.orchestration import get_job_result, task_result, tasks_list
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED
from aaiclick.orchestration.operators import map

# --- Task fixtures ---


@task
async def create_test_data() -> Object:
    """Create an Object with integer values [10, 20, 30, 40, 50].

    aai_id=True gives the partitioner a stable order_by so LIMIT/OFFSET
    slicing in _expand_map produces disjoint, complete partitions.
    """
    return await create_object_from_value([10, 20, 30, 40, 50], aai_id=True)


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


@task
async def scale(row: int, factor: int) -> int:
    return row * factor


@task
async def keep_large(row: int) -> int | None:
    return row if row >= 30 else None


@task
async def read_values(values: Object) -> list:
    return sorted(await values.data())


@task
async def create_test_records() -> Object:
    """Two-column Object; aai_id=True for stable partition slicing."""
    return await create_object_from_value({"a": [1, 2], "b": [3, 4]}, aai_id=True)


@task
async def swap(row: dict) -> dict:
    return {"a": row["b"], "b": row["a"]}


@task
async def quarter(row: int) -> float:
    return row / 4


@task
async def read_pairs(values: Object) -> list:
    data = await values.data()
    return sorted(zip(data["a"], data["b"], strict=True))


# --- Job pipelines (must be module-level for entrypoint resolution) ---


@job("test_map_basic")
def map_basic_pipeline(output_file: str):
    data = create_test_data()
    mapped = map(
        cbk=row_writer,
        obj=data,
        partition=5000,
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, mapped)


@job("test_map_kwargs_exec")
def map_kwargs_pipeline(output_file: str, factor: int):
    data = create_test_data()
    mapped = map(
        cbk=row_writer_with_factor,
        obj=data,
        partition=5000,
        kwargs={"factor": factor, "output_file": output_file},
    )
    return tasks_list(data, mapped)


@job("test_map_partitions")
def map_partitions_pipeline(output_file: str):
    data = create_test_data()
    mapped = map(
        cbk=row_writer,
        obj=data,
        partition=2,
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, mapped)


@job("test_map_output")
def map_output_pipeline(factor: int):
    data = create_test_data()
    mapped = map(cbk=scale, obj=data, partition=2, kwargs={"factor": factor})
    seen = read_values(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


@job("test_map_filter")
def map_filter_pipeline():
    data = create_test_data()
    mapped = map(cbk=keep_large, obj=data, partition=2)
    seen = read_values(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


@job("test_map_records")
def map_records_pipeline():
    data = create_test_records()
    mapped = map(cbk=swap, obj=data, partition=1)
    seen = read_pairs(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


@job("test_map_truncation")
def map_truncation_pipeline():
    data = create_test_data()
    mapped = map(cbk=quarter, obj=data, partition=2)
    return task_result(data=mapped, tasks=[data, mapped])


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


async def test_map_output_is_callback_returns(orch_ctx):
    """A consumer of map() reads the callback's return values, after every partition."""
    j = await map_output_pipeline(factor=2)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [20, 40, 60, 80, 100]


async def test_map_none_return_drops_row(orch_ctx):
    """A None return contributes no row, so map() doubles as a filter."""
    j = await map_filter_pipeline()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [30, 40, 50]


async def test_map_dict_schema_rows_are_records(orch_ctx):
    """A multi-column Object hands the callback one record per row, and the returns land as rows."""
    j = await map_records_pipeline()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [[3, 1], [4, 2]]


async def test_map_rejects_fraction_into_integer_column(orch_ctx):
    """A float with a fraction is refused rather than truncated into the integer output column."""
    j = await map_truncation_pipeline()
    await ajob_test(j)

    assert j.status == JOB_FAILED
    assert "integer column 'value'" in (j.error or "")
