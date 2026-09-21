"""
Parallel operators example for aaiclick orchestration.

Demonstrates map() and reduce() over partitions of an Object:
1. map(cbk, obj, partition): one child task per partition, cbk applied to
   each row
2. reduce(cbk, obj, partition): layered reduction — each layer reduces
   partitions until a single row remains

Both return a Group; the expander task inside it creates the partition
tasks at runtime, once the Object's row count is known.
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
async def print_row(row: int) -> None:
    """map() callback: called once per row of a partition."""
    print(f"  map: row {row}")


@job("map_example")
def map_job():
    """map() creates one _map_part child per partition of the Object."""
    values = create_values()
    return [values, map(print_row, values, partition=2)]


@task
async def sum_partition(partition: Object, output: Object) -> None:
    """reduce() callback: fold one partition into a single row of ``output``."""
    values = await partition.data()
    await output.insert(int(sum(values)))


@job("reduce_example")
def reduce_job():
    """reduce() with partition=2 builds layers of 3, 2 and 1 tasks for 5 rows."""
    values = create_values()
    total = reduce(sum_partition, values, partition=2)
    return task_result(data=total._result_task, tasks=[values, total])


async def amain():
    """Run the parallel operator examples."""
    print("=" * 50)
    print("aaiclick Parallel Operators Example")
    print("=" * 50)

    async with data_context():
        print("\nmap(): callback per row, one task per partition")
        print("-" * 50)
        job1 = await map_job()
        await ajob_test(job1)
        print(f"Job status: {job1.status}")
        assert job1.status == JOB_COMPLETED, f"Expected COMPLETED, got {job1.status}: {job1.error}"

        print("\nreduce(): layered sum down to one row")
        print("-" * 50)
        job2 = await reduce_job()
        await ajob_test(job2)
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
