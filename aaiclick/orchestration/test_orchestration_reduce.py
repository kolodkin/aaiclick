"""Execution tests for reduce() via ajob_test."""

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.data.object import Object
from aaiclick.orchestration import get_job_result, task_result
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED
from aaiclick.orchestration.operators import reduce

# --- Reduction callbacks (module-level for entrypoint resolution) ---


@task
async def sum_reduce(partition: Object, output: Object) -> None:
    """Sum all values in the partition and insert result into output."""
    values = await partition.data()
    await output.insert(int(sum(values)))


# --- Job pipelines (module-level for entrypoint resolution) ---


@task
async def create_test_object(values: list) -> Object:
    # aai_id=True gives the partitioner a stable order_by so LIMIT/OFFSET
    # slicing in _expand_map produces disjoint, complete partitions.
    return await create_object_from_value(values, aai_id=True)


@job("test_reduce_single_layer")
def reduce_single_layer(values: list):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=100)
    return task_result(data=reduced._result_task, tasks=[data, reduced])


@job("test_reduce_multi_layer")
def reduce_multi_layer(values: list, partition_size: int):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    return task_result(data=reduced._result_task, tasks=[data, reduced])


@job("test_reduce_empty")
def reduce_empty():
    data = create_test_object(values=[])
    reduced = reduce(sum_reduce, data, partition=100)
    return task_result(data=reduced._result_task, tasks=[data, reduced])


@job("test_reduce_single_row")
def reduce_single_row():
    data = create_test_object(values=[42])
    reduced = reduce(sum_reduce, data, partition=100)
    return task_result(data=reduced._result_task, tasks=[data, reduced])


# --- Tests ---


async def test_reduce_single_layer(orch_ctx):
    """reduce() with all rows in one partition produces correct sum."""
    j = await reduce_single_layer(values=[1, 2, 3, 4, 5])
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 15


async def test_reduce_multi_layer(orch_ctx):
    """reduce() with partition=2 creates multiple layers for [1,2,3,4,5]."""
    # partition=2: layer 0 has ceil(5/2)=3 tasks, layer 1 has ceil(3/2)=2,
    # layer 2 has ceil(2/2)=1 → 3 layers total
    j = await reduce_multi_layer(values=[1, 2, 3, 4, 5], partition_size=2)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 15


async def test_reduce_empty_raises(orch_ctx):
    """reduce() of empty Object without initializer fails with TypeError."""
    j = await reduce_empty()
    await ajob_test(j)

    assert j.status == JOB_FAILED
    assert "reduce() of empty sequence" in (j.error or "")


async def test_reduce_single_row(orch_ctx):
    """reduce() of a single-row Object returns that row unchanged."""
    j = await reduce_single_row()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 42


async def test_reduce_native_api(orch_ctx):
    """reduce() with native API callback (partition.data() + sum()) completes successfully."""
    j = await reduce_single_layer(values=[10, 20, 30, 40])
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 100
