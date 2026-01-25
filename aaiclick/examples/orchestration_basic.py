"""
Basic orchestration example for aaiclick.

This example demonstrates how to create and execute jobs using the
orchestration backend. It shows the basic workflow of:
1. Creating a job with a task
2. Executing the job using job_test() for synchronous testing
3. Using ajob_test() for async contexts

Note: This requires a running PostgreSQL server for job/task state storage.
"""

import asyncio

from aaiclick.orchestration import (
    JobStatus,
    OrchContext,
    ajob_test,
    create_job,
    create_task,
)


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
    Async entry point using ajob_test() for async contexts.

    Use this when already inside an async context (e.g., pytest-asyncio tests,
    async web handlers). ajob_test() can be awaited without creating a new
    event loop.

    Note: Only tests jobs without parameters since Object/View parameter
    deserialization is not yet fully implemented.
    """
    print("=" * 50)
    print("aaiclick Orchestration Basic Example (Async)")
    print("=" * 50)
    print("\nNote: This example requires:")
    print("      - Running PostgreSQL server (localhost:5432)")
    print("      - Database migrations applied (python -m aaiclick migrate)")
    print()

    async with OrchContext():
        # Example 1: Create a simple job (no parameters)
        job1 = await example_simple_job()

    # Test execution using ajob_test() - the async variant
    # This creates its own OrchContext internally
    print("\n" + "=" * 50)
    print("Testing Job Execution (Async Mode with ajob_test)")
    print("-" * 50)

    print(f"\nRunning job: {job1.name}")
    await ajob_test(job1)  # Async - can be awaited
    print(f"Job status: {job1.status}")
    assert job1.status == JobStatus.COMPLETED, f"Expected COMPLETED, got {job1.status}"

    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("=" * 50)


async def main():
    """
    Main entry point demonstrating ajob_test() in async context.

    This example shows how to use ajob_test() from an async context:
    1. Create job within OrchContext
    2. Run ajob_test() which can be awaited

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

    # Test execution using ajob_test() - the async variant
    # This creates its own OrchContext internally
    print("\n" + "=" * 50)
    print("Testing Job Execution with ajob_test()")
    print("-" * 50)

    print(f"\nRunning job: {job1.name}")
    await ajob_test(job1)  # Async - can be awaited
    print(f"Job status: {job1.status}")
    assert job1.status == JobStatus.COMPLETED, f"Expected COMPLETED, got {job1.status}"

    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
