"""Execution tests for dynamic task creation (map, foreach) via ajob_test."""

import tempfile
from pathlib import Path

import pytest

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.data.object import Object, View
from aaiclick.orchestration import get_job_result, task_result, tasks_list
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED
from aaiclick.orchestration.operators import foreach, map

# --- Task fixtures ---


@task
async def create_test_data() -> Object:
    """Create an Object with integer values [10, 20, 30, 40, 50].

    aai_id=True gives the partitioner a stable order_by so LIMIT/OFFSET
    slicing in _expand_map produces disjoint, complete partitions.
    """
    return await create_object_from_value([10, 20, 30, 40, 50], aai_id=True)


@task
async def make_factor() -> int:
    return 3


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


@job("test_foreach_basic")
def foreach_basic_pipeline(output_file: str):
    data = create_test_data()
    mapped = foreach(
        cbk=row_writer,
        obj=data,
        partition=5000,
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, mapped)


@job("test_foreach_kwargs")
def foreach_kwargs_pipeline(output_file: str, factor: int):
    data = create_test_data()
    mapped = foreach(
        cbk=row_writer_with_factor,
        obj=data,
        partition=5000,
        kwargs={"factor": factor, "output_file": output_file},
    )
    return tasks_list(data, mapped)


@job("test_foreach_task_arg")
def foreach_task_arg_pipeline(output_file: str):
    data = create_test_data()
    factor = make_factor()
    mapped = foreach(
        cbk=row_writer_with_factor,
        obj=data,
        partition=5000,
        args=(factor,),
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, factor, mapped)


@job("test_foreach_partitions")
def foreach_partitions_pipeline(output_file: str):
    data = create_test_data()
    mapped = foreach(
        cbk=row_writer,
        obj=data,
        partition=2,
        kwargs={"output_file": output_file},
    )
    return tasks_list(data, mapped)


@task
async def read_written(done: None, output_file: str) -> dict:
    """Consumer of foreach(): reads the file every partition has written to."""
    lines = Path(output_file).read_text().split()
    return {"done": done, "rows": sorted(int(line) for line in lines)}


@job("test_foreach_consumer")
def foreach_consumer_pipeline(output_file: str):
    data = create_test_data()
    written = foreach(cbk=row_writer, obj=data, partition=2, kwargs={"output_file": output_file})
    seen = read_written(done=written, output_file=output_file)
    return task_result(data=seen, tasks=[data, written, seen])


def _map_then_read(data, cbk, reader, partition: int, kwargs: dict | None = None):
    """Map ``data`` through ``cbk`` and hand the output to ``reader``; the job result is the read."""
    mapped = map(cbk=cbk, obj=data, partition=partition, kwargs=kwargs or {})
    seen = reader(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


@job("test_map_output")
def map_output_pipeline():
    return _map_then_read(create_test_data(), scale, read_values, partition=2, kwargs={"factor": 2})


@job("test_map_filter")
def map_filter_pipeline():
    return _map_then_read(create_test_data(), keep_large, read_values, partition=2)


@job("test_map_records")
def map_records_pipeline():
    return _map_then_read(create_test_records(), swap, read_pairs, partition=1)


@job("test_map_cast")
def map_cast_pipeline():
    return _map_then_read(create_test_data(), quarter, read_values, partition=2)


@job("test_map_view")
def map_view_pipeline(where: str | None, offset: int | None, limit: int | None):
    data = create_test_view(where=where, offset=offset, limit=limit)
    return _map_then_read(data, scale, read_values, partition=1, kwargs={"factor": 2})


# --- Execution tests ---


@pytest.mark.parametrize(
    "pipeline, pipeline_kwargs, expected",
    [
        # Creates partitions, runs the callback on each row.
        pytest.param(foreach_basic_pipeline, {}, [10, 20, 30, 40, 50], id="basic"),
        # Extra kwargs are forwarded to the callback.
        pytest.param(foreach_kwargs_pipeline, {"factor": 3}, [30, 60, 90, 120, 150], id="kwargs"),
        # A Task in args makes the expander wait for it and forwards its result.
        pytest.param(foreach_task_arg_pipeline, {}, [30, 60, 90, 120, 150], id="task-in-args"),
        # A small partition size creates multiple _map_part tasks.
        pytest.param(foreach_partitions_pipeline, {}, [10, 20, 30, 40, 50], id="multiple-partitions"),
    ],
)
async def test_foreach_execution(orch_ctx, pipeline, pipeline_kwargs, expected):
    """foreach() end-to-end: the callback runs once per row of the Object."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await ajob_test(pipeline, output_file=output_file, **pipeline_kwargs)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        lines = Path(output_file).read_text().strip().split("\n")
        assert sorted(int(line) for line in lines) == expected


async def test_foreach_consumer_waits_for_every_partition(orch_ctx):
    """A consumer of foreach() runs after every partition and receives ``None``."""
    with tempfile.TemporaryDirectory() as tmpdir:
        output_file = str(Path(tmpdir) / "output.txt")

        j = await ajob_test(foreach_consumer_pipeline, output_file=output_file)

        assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
        async with data_context():
            assert await get_job_result(j) == {"done": None, "rows": [10, 20, 30, 40, 50]}


@pytest.mark.parametrize(
    "pipeline, expected",
    [
        # A consumer reads the callback's return values, after every partition.
        pytest.param(map_output_pipeline, [20, 40, 60, 80, 100], id="returns"),
        # A None return contributes no row, so map() doubles as a filter.
        pytest.param(map_filter_pipeline, [30, 40, 50], id="none-drops-row"),
        # A multi-column Object hands the callback one record per row.
        pytest.param(map_records_pipeline, [[3, 1], [4, 2]], id="records"),
        # Returns are cast to the input column type: fractions truncate into an integer column.
        pytest.param(map_cast_pipeline, [2, 5, 7, 10, 12], id="cast"),
    ],
)
async def test_map_output(orch_ctx, pipeline, expected):
    """The consumer of map() reads the collected callback returns."""
    j = await ajob_test(pipeline)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == expected


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
    j = await ajob_test(map_view_pipeline, where=where, offset=offset, limit=limit)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == expected
