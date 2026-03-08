"""
Dynamic task creation example for aaiclick orchestration.

Demonstrates dynamic task creation patterns:
1. @task returning Task objects for dynamic registration
2. @task returning a list of Tasks
3. Using map() to partition data and apply a callback in parallel

Note: This requires running PostgreSQL and ClickHouse servers.
"""

import asyncio

from aaiclick import create_object_from_value
from aaiclick.data.data_context import data_context
from aaiclick.orchestration import JobStatus, ajob_test, job, map, task


# --- Dynamic task creation: tasks returning tasks ---


@task
async def step_a() -> int:
    print("  step_a: running")
    return 42


@task
async def step_b(x: int) -> int:
    print(f"  step_b: received {x}")
    return x * 2


@task
async def orchestrator():
    """A task that dynamically creates child tasks at runtime.

    Demonstrates:
    - Returning a list of Tasks for dynamic registration
    - Implicit dependency via upstream ref (b depends on a)
    - Explicit dependency via >> operator (c runs after b)
    """
    a = step_a()
    b = step_b(x=a)  # implicit dependency: b depends on a (via upstream ref)
    c = step_a()
    b >> c  # explicit dependency: c runs after b
    return [a, b, c]  # returned tasks are registered to the current job


@job("dynamic_tasks_example")
def dynamic_tasks_job():
    """Job whose entry point creates child tasks dynamically."""
    entry = orchestrator()
    return [entry]


# --- Map operator ---


@task
async def load_numbers():
    """Load sample data into an Object."""
    return await create_object_from_value(list(range(1, 11)))


async def print_row(row):
    """Callback applied to each row by map_part."""
    print(f"  map callback: row={row}")


@job("map_example")
def map_job():
    """Job that partitions data and processes partitions in parallel."""
    data = load_numbers()
    mapped = map(cbk=print_row, obj=data, partition=5)
    return [data, mapped]


async def amain():
    """Run all dynamic orchestration examples."""
    print("=" * 50)
    print("aaiclick Dynamic Orchestration Examples")
    print("=" * 50)

    async with data_context():
        # Example 1: Dynamic task creation
        print("\nExample 1: Task returning child tasks")
        print("-" * 50)

        job1 = await dynamic_tasks_job()
        print(f"Created job: {job1.name} (ID: {job1.id})")
        await ajob_test(job1)
        print(f"Job status: {job1.status}")
        if job1.error:
            print(f"Error: {job1.error}")
        assert job1.status == JobStatus.COMPLETED, (
            f"Expected COMPLETED, got {job1.status}: {job1.error}"
        )

        # Example 2: Map operator
        print(f"\nExample 2: map() operator")
        print("-" * 50)

        job2 = await map_job()
        print(f"Created job: {job2.name} (ID: {job2.id})")
        await ajob_test(job2)
        print(f"Job status: {job2.status}")
        if job2.error:
            print(f"Error: {job2.error}")
        assert job2.status == JobStatus.COMPLETED, (
            f"Expected COMPLETED, got {job2.status}: {job2.error}"
        )

    print("\n" + "=" * 50)
    print("Dynamic examples completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(amain())
