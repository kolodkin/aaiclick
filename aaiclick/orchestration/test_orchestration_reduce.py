"""Execution tests for reduce() via ajob_test."""

from sqlmodel import select

from aaiclick.data.data_context import create_object_from_value, data_context, get_ch_client
from aaiclick.data.object import Object
from aaiclick.orchestration import TaskResult
from aaiclick.orchestration.context import get_orch_session
from aaiclick.orchestration.debug_execution import ajob_test
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.models import JobStatus, Task
from aaiclick.orchestration.orch_helpers import reduce


# --- Reduction callbacks (module-level for entrypoint resolution) ---


@task
async def sum_reduce(partition: Object, output: Object) -> None:
    """Sum all values in the partition and insert result into output."""
    values = await partition.data()
    await output.insert(int(sum(values)))


# --- Job pipelines (module-level for entrypoint resolution) ---


@task
async def create_test_object(values: list) -> Object:
    return await create_object_from_value(values)


@job("test_reduce_single_layer")
def reduce_single_layer(values: list):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=100)
    return TaskResult(tasks=[data, reduced])


@job("test_reduce_multi_layer")
def reduce_multi_layer(values: list, partition_size: int):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    return TaskResult(tasks=[data, reduced])


@job("test_reduce_empty")
def reduce_empty():
    data = create_test_object(values=[])
    reduced = reduce(sum_reduce, data, partition=100)
    return TaskResult(tasks=[data, reduced])


@job("test_reduce_single_row")
def reduce_single_row():
    data = create_test_object(values=[42])
    reduced = reduce(sum_reduce, data, partition=100)
    return TaskResult(tasks=[data, reduced])


# --- Tests ---


async def _get_reduce_result(job_id: int) -> int:
    """Return the scalar value from the _expand_reduce task result."""
    async with get_orch_session() as session:
        expand_task = (await session.execute(
            select(Task).where(Task.job_id == job_id, Task.name == "_expand_reduce")
        )).scalar_one()
        table = expand_task.result["table"]
    async with data_context():
        ch = get_ch_client()
        result = await ch.query(f"SELECT value FROM {table}")
        return result.first_row[0]


async def test_reduce_single_layer(orch_ctx):
    """reduce() with all rows in one partition produces correct sum."""
    j = await reduce_single_layer(values=[1, 2, 3, 4, 5])
    await ajob_test(j)

    assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
    assert await _get_reduce_result(j.id) == 15


async def test_reduce_multi_layer(orch_ctx):
    """reduce() with partition=2 creates multiple layers for [1,2,3,4,5]."""
    # partition=2: layer 0 has ceil(5/2)=3 tasks, layer 1 has ceil(3/2)=2,
    # layer 2 has ceil(2/2)=1 → 3 layers total
    j = await reduce_multi_layer(values=[1, 2, 3, 4, 5], partition_size=2)
    await ajob_test(j)

    assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
    assert await _get_reduce_result(j.id) == 15


async def test_reduce_empty_raises(orch_ctx):
    """reduce() of empty Object without initializer fails with TypeError."""
    j = await reduce_empty()
    await ajob_test(j)

    assert j.status == JobStatus.FAILED
    assert "reduce() of empty sequence" in (j.error or "")


async def test_reduce_single_row(orch_ctx):
    """reduce() of a single-row Object returns that row unchanged."""
    j = await reduce_single_row()
    await ajob_test(j)

    assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
    assert await _get_reduce_result(j.id) == 42


async def test_reduce_native_api(orch_ctx):
    """reduce() with native API callback (partition.data() + sum()) completes successfully."""
    j = await reduce_single_layer(values=[10, 20, 30, 40])
    await ajob_test(j)

    assert j.status == JobStatus.COMPLETED, f"Job failed: {j.error}"
    assert await _get_reduce_result(j.id) == 100
