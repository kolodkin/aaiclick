"""
Basic orchestration example for aaiclick.

This example demonstrates how to create and execute jobs using the
orchestration backend. It shows the basic workflow of:
1. Creating a job with a task
2. Executing the job using job.test() for synchronous testing

Note: This requires a running PostgreSQL server for job/task state storage.
"""

import asyncio

from aaiclick.orchestration import JobStatus, create_job, create_task
from aaiclick.orchestration.context import OrchContext


# Define sample task functions
async def simple_arithmetic():
    """A simple task that does basic arithmetic and prints the result."""
    a = 1
    b = 2
    c = a + b
    print(f"Computing: {a} + {b} = {c}")


async def task_with_params(x: int, y: int):
    """A task that takes parameters and prints their product."""
    result = x * y
    print(f"Computing: {x} * {y} = {result}")


async def example_simple_job():
    """Example: Create and run a simple job with one task."""
    print("\n" + "=" * 50)
    print("Example 1: Simple Job with Callable Function")
    print("-" * 50)

    # Create a job using a callable function directly
    # The function is automatically converted to its module path string
    job = await create_job("simple_arithmetic_job", simple_arithmetic)

    print(f"Created job: {job.name} (ID: {job.id})")
    print(f"Initial status: {job.status}")

    return job


async def example_job_with_task():
    """Example: Create a job using a Task object with parameters."""
    print("\n" + "=" * 50)
    print("Example 2: Job with Task Object and Parameters")
    print("-" * 50)

    # Create a task with callable and parameters
    task = create_task(task_with_params, {"x": 5, "y": 7})

    # Create job with the task
    job = await create_job("parametrized_job", task)

    print(f"Created job: {job.name} (ID: {job.id})")
    print(f"Task entrypoint: {task.entrypoint}")
    print(f"Task kwargs: {task.kwargs}")
    print(f"Initial status: {job.status}")

    return job


async def async_main():
    """
    Async entry point that creates jobs and tests them using async methods.

    This is suitable for calling from async test contexts where asyncio.run()
    cannot be used (e.g., pytest-asyncio).

    Note: Only tests jobs without parameters since Object/View parameter
    deserialization is not yet fully implemented.
    """
    from aaiclick.orchestration.execution import run_job_tasks

    print("=" * 50)
    print("aaiclick Orchestration Basic Example")
    print("=" * 50)
    print("\nNote: This example requires:")
    print("      - Running PostgreSQL server (localhost:5432)")
    print("      - Database migrations applied (python -m aaiclick migrate)")
    print()

    async with OrchContext():
        # Example 1: Create a simple job (no parameters)
        job1 = await example_simple_job()

        # Test execution using async method (within the context)
        print("\n" + "=" * 50)
        print("Testing Job Execution (Async Mode)")
        print("-" * 50)

        print(f"\nRunning job: {job1.name}")
        await run_job_tasks(job1)
        print(f"Job status: {job1.status}")
        assert job1.status == JobStatus.COMPLETED, f"Expected COMPLETED, got {job1.status}"

        # Note: example_job_with_task() is not tested here because it uses
        # native Python parameters, but Object/View deserialization is not
        # yet implemented. See main() for demonstration of task creation.

    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("=" * 50)


async def main():
    """
    Main entry point demonstrating job.test() synchronous mode.

    This uses job.test() which internally calls asyncio.run(), so it cannot
    be called from within an already-running event loop. For async contexts,
    use async_main() instead.

    Note: Only tests jobs without parameters since Object/View parameter
    deserialization is not yet fully implemented.
    """
    print("=" * 50)
    print("aaiclick Orchestration Basic Example")
    print("=" * 50)
    print("\nNote: This example requires:")
    print("      - Running PostgreSQL server (localhost:5432)")
    print("      - Database migrations applied (python -m aaiclick migrate)")
    print()

    async with OrchContext():
        # Example 1: Create a simple job (no parameters)
        job1 = await example_simple_job()

        # Example 2: Demonstrate task creation with parameters
        # Note: This task cannot be executed yet because Object/View
        # parameter deserialization is not yet implemented
        _ = await example_job_with_task()

    # Test execution (runs outside OrchContext, creates its own)
    # Note: job.test() uses asyncio.run() internally, so this only works
    # when not already inside an event loop
    print("\n" + "=" * 50)
    print("Testing Job Execution (Synchronous Test Mode)")
    print("-" * 50)

    print(f"\nRunning job: {job1.name}")
    job1.test()  # Blocks until job completes
    print(f"Job status after test(): {job1.status}")
    assert job1.status == JobStatus.COMPLETED, f"Expected COMPLETED, got {job1.status}"

    # Note: job2 (parametrized) not tested - Object deserialization not yet implemented

    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
