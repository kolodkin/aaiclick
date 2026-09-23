"""Execution tests for reduce() via ajob_test."""

from aaiclick.data.data_context import create_object_from_value, data_context
from aaiclick.data.object import Object, View
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
    return task_result(data=reduced, tasks=[data, reduced])


@job("test_reduce_multi_layer")
def reduce_multi_layer(values: list, partition_size: int):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    return task_result(data=reduced, tasks=[data, reduced])


@job("test_reduce_empty")
def reduce_empty():
    data = create_test_object(values=[])
    reduced = reduce(sum_reduce, data, partition=100)
    return task_result(data=reduced, tasks=[data, reduced])


@job("test_reduce_single_row")
def reduce_single_row():
    data = create_test_object(values=[42])
    reduced = reduce(sum_reduce, data, partition=100)
    return task_result(data=reduced, tasks=[data, reduced])


@task
async def read_total(total: Object) -> list:
    return await total.data()


@job("test_reduce_consumer")
def reduce_consumer(values: list, partition_size: int):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    seen = read_total(total=reduced)
    return task_result(data=seen, tasks=[data, reduced, seen])


@task
async def create_filtered_view(values: list, where: str) -> View:
    data = await create_object_from_value(values, aai_id=True)
    return data.view(where=where, order_by="aai_id")


@job("test_reduce_view")
def reduce_view_pipeline(values: list, where: str, partition_size: int):
    data = create_filtered_view(values=values, where=where)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    seen = read_total(total=reduced)
    return task_result(data=seen, tasks=[data, reduced, seen])


# --- Tests ---


async def test_reduce_single_layer(orch_ctx):
    """reduce() with all rows in one partition produces correct sum."""
    j = await ajob_test(reduce_single_layer, values=[1, 2, 3, 4, 5])

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 15


async def test_reduce_multi_layer(orch_ctx):
    """reduce() with partition=2 creates multiple layers for [1,2,3,4,5]."""
    # partition=2: layer 0 has ceil(5/2)=3 tasks, layer 1 has ceil(3/2)=2,
    # layer 2 has ceil(2/2)=1 → 3 layers total
    j = await ajob_test(reduce_multi_layer, values=[1, 2, 3, 4, 5], partition_size=2)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 15


async def test_reduce_empty_raises(orch_ctx):
    """reduce() of empty Object without initializer fails with TypeError."""
    j = await ajob_test(reduce_empty)

    assert j.status == JOB_FAILED
    assert "reduce() of empty sequence" in (j.error or "")


async def test_reduce_single_row(orch_ctx):
    """reduce() of a single-row Object returns that row unchanged."""
    j = await ajob_test(reduce_single_row)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 42


async def test_reduce_native_api(orch_ctx):
    """reduce() with native API callback (partition.data() + sum()) completes successfully."""
    j = await ajob_test(reduce_single_layer, values=[10, 20, 30, 40])

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        result_obj = await get_job_result(j)
        assert (await result_obj.data())[0] == 100


async def test_reduce_consumer_sees_filled_result(orch_ctx):
    """A consumer of reduce() runs after every layer, not after the expander alone."""
    j = await reduce_consumer(values=[1, 2, 3, 4, 5], partition_size=2)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == [15]


async def test_reduce_over_filtered_view_sums_only_its_rows(orch_ctx):
    """Layer-0 partitions slice the View, not its base table."""
    j = await reduce_view_pipeline(values=[1, 2, 3, 4, 5], where="value >= 3", partition_size=2)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == [12]
