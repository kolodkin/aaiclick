"""
Group example for aaiclick orchestration.

Demonstrates Groups — tasks that belong together:
1. Building a Group with add_task()
2. Passing a Group as a kwarg: the consumer waits for every member and
   receives their results as a list (fan-in)
3. Ordering edges on a whole Group: task >> group and group >> task
"""

import asyncio

from aaiclick.data.data_context import data_context
from aaiclick.orchestration import JOB_COMPLETED, Group, ajob_test, job, task


@task
async def produce(value: int) -> int:
    print(f"  produce: {value}")
    return value


@task
async def sum_results(results: list[int]) -> int:
    """Consume a whole Group: ``results`` holds the members' results in task order."""
    print(f"  sum_results: received {results}")  # → [1, 2, 3]
    print(f"  sum_results: total {sum(results)}")  # → 6
    return sum(results)


@job("group_fan_in")
def fan_in_job():
    """A Group passed as a kwarg is both a dependency and a fan-in of its results.

    ``sum_results`` waits for every member and receives their results as a
    list — no explicit ``group >> sum_results`` needed.
    """
    group = Group(name="producers")
    for value in (1, 2, 3):
        group.add_task(produce(value=value))
    return [group, sum_results(results=group)]


@task
async def prepare() -> str:
    print("  prepare: running first")
    return "ready"


@task
async def finish() -> str:
    print("  finish: running after every member")
    return "done"


@job("group_ordering")
def ordering_job():
    """Ordering edges apply to the whole Group.

    ``prepare >> group`` runs every member after ``prepare``;
    ``group >> finish`` runs ``finish`` once every member has completed.
    """
    first = prepare()
    group = Group(name="workers")
    for value in (10, 20):
        group.add_task(produce(value=value))
    last = finish()
    first >> group >> last
    return [first, group, last]


async def amain():
    """Run the group orchestration examples."""
    print("=" * 50)
    print("aaiclick Group Orchestration Example")
    print("=" * 50)

    async with data_context():
        print("\nGroup as a kwarg (fan-in of member results)")
        print("-" * 50)
        job1 = await fan_in_job()
        await ajob_test(job1)
        print(f"Job status: {job1.status}")
        assert job1.status == JOB_COMPLETED, f"Expected COMPLETED, got {job1.status}: {job1.error}"

        print("\nOrdering edges on a Group (task >> group >> task)")
        print("-" * 50)
        job2 = await ordering_job()
        await ajob_test(job2)
        print(f"Job status: {job2.status}")
        assert job2.status == JOB_COMPLETED, f"Expected COMPLETED, got {job2.status}: {job2.error}"

    print("\n" + "=" * 50)
    print("Group example completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(amain())
