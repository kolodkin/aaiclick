"""``map()`` and ``reduce()`` pipelines with a consumer task, for the web e2e graph tests.

Module-level so a worker can import every task by entrypoint. Each job's
result is the consumer's read of the operator output, which is only correct
if the consumer ran after the finalize task.
"""

from aaiclick import Object, create_object_from_value
from aaiclick.orchestration import job, map, reduce, task, task_result


@task
async def create_values() -> Object:
    """Five rows; aai_id=True gives the partitioner a stable row order."""
    return await create_object_from_value([1, 2, 3, 4, 5], aai_id=True)


@task
async def double(row: int) -> int:
    return row * 2


@task
async def sum_partition(partition: Object, output: Object) -> None:
    await output.insert(int(sum(await partition.data())))


@task
async def read_values(values: Object) -> list:
    """The consumer: reads the operator's output once every partition task has run."""
    return sorted(await values.data())


@job("e2e_map")
def map_pipeline():
    values = create_values()
    doubled = map(double, values, partition=2)
    seen = read_values(values=doubled)
    return task_result(data=seen, tasks=[values, doubled, seen])


@job("e2e_reduce")
def reduce_pipeline():
    values = create_values()
    total = reduce(sum_partition, values, partition=2)
    seen = read_values(values=total)
    return task_result(data=seen, tasks=[values, total, seen])
