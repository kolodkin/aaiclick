"""
Parallel operators example for aaiclick orchestration.

Demonstrates map() and reduce() over partitions of an Object:
1. map(cbk, obj, partition): one child task per partition, cbk applied to
   each row and its return values collected into the output Object
2. reduce(cbk, obj, partition): layered reduction — each layer reduces
   partitions until a single row remains

Both return the expander Task; its result is the output Object, and
consumers wait for the partition tasks.
"""

import asyncio

from aaiclick import Object, create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.orchestration import (
    JOB_COMPLETED,
    ajob_test,
    get_job_result,
    job,
    map,
    orch_context,
    reduce,
    task,
    task_result,
)


@task
async def create_values() -> Object:
    """Five rows; aai_id=True gives the partitioner a stable row order."""
    return await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)


@task
async def double(row: int) -> int:
    """map() callback: called once per row; the return value lands in the output."""
    return row * 2


@task
async def show_doubled(doubled: Object) -> None:
    """Consumer of map(): runs after every partition task."""
    print(f"Doubled: {sorted(await doubled.data())}")  # → [2, 4, 6, 8, 10]


@job("map_example")
def map_job():
    """map() creates one _map_part child per partition of the Object."""
    values = create_values()
    doubled = map(double, values, partition=2)
    shown = show_doubled(doubled=doubled)
    return task_result(data=doubled, tasks=[values, doubled, shown])


@task
async def sum_partition(partition: Object, output: Object) -> None:
    """reduce() callback: fold one partition into a single row of ``output``."""
    values = await partition.data()
    await output.insert(int(sum(values)))


@task
async def show_total(total: Object) -> None:
    """Consumer of reduce(): runs after every layer, so the Object is filled."""
    print(f"Reduced total: {(await total.data())[0]}")  # → 15


@job("reduce_example")
def reduce_job():
    """reduce() with partition=2 builds layers of 3, 2 and 1 tasks for 5 rows."""
    values = create_values()
    total = reduce(sum_partition, values, partition=2)
    shown = show_total(total=total)
    return task_result(data=total, tasks=[values, total, shown])


async def amain():
    """Run the parallel operator examples."""
    print("=" * 50)
    print("aaiclick Parallel Operators Example")
    print("=" * 50)

    async with data_context():
        print("\nmap(): callback per row, returns collected into the output")
        print("-" * 50)
        job1 = await ajob_test(map_job)
        print(f"Job status: {job1.status}")
        assert job1.status == JOB_COMPLETED, f"Expected COMPLETED, got {job1.status}: {job1.error}"

        print("\nreduce(): layered sum down to one row")
        print("-" * 50)
        job2 = await ajob_test(reduce_job)
        print(f"Job status: {job2.status}")
        assert job2.status == JOB_COMPLETED, f"Expected COMPLETED, got {job2.status}: {job2.error}"
        async with orch_context():
            total = await get_job_result(job2)
            print(f"Reduced total: {(await total.data())[0]}")  # → 15

    print("\n" + "=" * 50)
    print("Parallel operators example completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(amain())
