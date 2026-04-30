"""
Dynamic task creation example for aaiclick orchestration.

Demonstrates dynamic task creation patterns:
1. @task returning Task objects for dynamic registration
2. Implicit dependencies via upstream refs
3. Explicit dependencies via >> operator
4. Mixed [Task, Task, ...] returns

Note: map() example is not included here because it requires distributed
workers with lifecycle handlers for Object table management. See
aaiclick/orchestration/operators.py for the map() implementation.

Note: This requires running PostgreSQL and ClickHouse servers.
"""

import asyncio

from aaiclick.data.data_context import data_context
from aaiclick.orchestration import JOB_COMPLETED, JobStatus, ajob_test, job, task, tasks_list


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
    return tasks_list(a, b, c)


@job("dynamic_tasks_example")
def dynamic_tasks_job():
    """Job whose entry point creates child tasks dynamically."""
    entry = orchestrator()
    return entry


# --- Test/example pipelines for dynamic task registration ---


@task
def child_task_a():
    """A child task that returns a native value."""
    return "result_a"


@task
def child_task_b():
    """A child task that returns a native value."""
    return "result_b"


@task
def task_returning_tasks():
    """A task that returns child tasks for dynamic registration."""
    a = child_task_a()
    b = child_task_b()
    return tasks_list(a, b)


@task
def step_two():
    """Second step in a chain."""
    return "step_two_done"


@task
def step_one():
    """First step that returns a child task for chaining."""
    return step_two()


@job("dynamic_pipeline")
def dynamic_pipeline():
    """Entry point that spawns child tasks."""
    a = child_task_a()
    b = child_task_b()
    return tasks_list(a, b)


@job("chain_pipeline")
def chain_pipeline():
    """Entry point that returns a task which itself returns a task."""
    return step_one()


async def amain():
    """Run dynamic orchestration example."""
    print("=" * 50)
    print("aaiclick Dynamic Orchestration Example")
    print("=" * 50)

    async with data_context():
        print("\nTask returning child tasks (implicit + explicit deps)")
        print("-" * 50)

        job1 = await dynamic_tasks_job()
        print(f"Created job: {job1.name} (ID: {job1.id})")
        await ajob_test(job1)
        print(f"Job status: {job1.status}")
        if job1.error:
            print(f"Error: {job1.error}")
        assert job1.status == JOB_COMPLETED, f"Expected COMPLETED, got {job1.status}: {job1.error}"

    print("\n" + "=" * 50)
    print("Dynamic example completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(amain())
