"""Execution tests for dynamic task creation (map) via ajob_test."""

import tempfile
from pathlib import Path

import pytest

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.data.object import Object, View
from aaiclick.orchestration import get_job_result, task_result, tasks_list
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED
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


@task
async def create_test_view(where: str | None, offset: int | None, limit: int | None) -> View:
    """A View over [10, 20, 30, 40, 50]; the map must see only the rows it selects."""
    data = await create_object_from_value([10, 20, 30, 40, 50], aai_id=True)
    return data.view(where=where, offset=offset, limit=limit, order_by="aai_id")


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


def _map_then_read(source, cbk, reader, partition: int, kwargs: dict | None = None):
    """Map ``source`` through ``cbk`` and hand the output to ``reader``; the job result is the read."""
    data = source()
    mapped = map(cbk=cbk, obj=data, partition=partition, kwargs=kwargs or {})
    seen = reader(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


@job("test_map_output")
def map_output_pipeline(factor: int):
    return _map_then_read(create_test_data, scale, read_values, partition=2, kwargs={"factor": factor})


@job("test_map_filter")
def map_filter_pipeline():
    return _map_then_read(create_test_data, keep_large, read_values, partition=2)


@job("test_map_records")
def map_records_pipeline():
    return _map_then_read(create_test_records, swap, read_pairs, partition=1)


@job("test_map_cast")
def map_cast_pipeline():
    return _map_then_read(create_test_data, quarter, read_values, partition=2)


@job("test_map_view")
def map_view_pipeline(where: str | None, offset: int | None, limit: int | None):
    data = create_test_view(where=where, offset=offset, limit=limit)
    mapped = map(cbk=scale, obj=data, partition=1, kwargs={"factor": 2})
    seen = read_values(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


# --- Execution tests ---


async def test_map_execution_basic(orch_ctx, monkeypatch):
    """map() end-to-end: creates partitions, runs callback on each row."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await ajob_test(map_basic_pipeline, output_file=output_file)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines) == ["10", "20", "30", "40", "50"]


async def test_map_execution_with_kwargs(orch_ctx, monkeypatch):
    """map() forwards extra kwargs to callback."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await ajob_test(map_kwargs_pipeline, output_file=output_file, factor=3)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(lines, key=int) == ["30", "60", "90", "120", "150"]


async def test_map_execution_multiple_partitions(orch_ctx, monkeypatch):
    """map() with small partition size creates multiple _map_part tasks."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await ajob_test(map_partitions_pipeline, output_file=output_file)

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


async def test_map_casts_returns_to_input_column_type(orch_ctx):
    """Returns are cast to the input column type on insert: fractions truncate into an integer column."""
    j = await map_cast_pipeline()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [2, 5, 7, 10, 12]


@pytest.mark.parametrize(
    "where, offset, limit, expected",
    [
        pytest.param("value >= 30", None, None, [60, 80, 100], id="filtered"),
        pytest.param(None, 1, 3, [40, 60, 80], id="sliced"),
        pytest.param("value >= 20", 1, 2, [60, 80], id="filtered-and-sliced"),
    ],
)
async def test_map_over_view_sees_only_its_rows(orch_ctx, where, offset, limit, expected):
    """Partitions slice the View, not its base table."""
    j = await map_view_pipeline(where=where, offset=offset, limit=limit)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == expected
