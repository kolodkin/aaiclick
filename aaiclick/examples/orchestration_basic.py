"""
Basic orchestration example for aaiclick.

This example demonstrates how to create and execute jobs using the
orchestration backend. It shows the basic workflow of:
1. Creating a job with a task
2. Executing the job using job.test() for synchronous testing

Note: This requires a running PostgreSQL server for job/task state storage.
"""

import asyncio

from aaiclick.orchestration import Job, JobStatus, create_job, create_task
from aaiclick.orchestration.context import OrchContext


# Define sample task functions
async def simple_arithmetic():
    """A simple task that does basic arithmetic and prints the result."""
    a = 1
    b = 2
    c = a + b
    print(f"Computing: {a} + {b} = {c}")
    return c


async def task_with_params(x: int, y: int) -> int:
    """A task that takes parameters and returns their product."""
    result = x * y
    print(f"Computing: {x} * {y} = {result}")
    return result


async def example_simple_job():
    """Example: Create and run a simple job with one task."""
    print("\n" + "=" * 50)
    print("Example 1: Simple Job with Callback String")
    print("-" * 50)

    # Create a job using a callback string
    # The callback string references a function in this module
    job = await create_job(
        "simple_arithmetic_job",
        "aaiclick.examples.orchestration_basic.simple_arithmetic",
    )

    print(f"Created job: {job.name} (ID: {job.id})")
    print(f"Initial status: {job.status}")

    return job


async def example_job_with_task():
    """Example: Create a job using a Task object with parameters."""
    print("\n" + "=" * 50)
    print("Example 2: Job with Task Object and Parameters")
    print("-" * 50)

    # Create a task with parameters
    task = create_task(
        "aaiclick.examples.orchestration_basic.task_with_params",
        {"x": 5, "y": 7},
    )

    # Create job with the task
    job = await create_job("parametrized_job", task)

    print(f"Created job: {job.name} (ID: {job.id})")
    print(f"Task entrypoint: {task.entrypoint}")
    print(f"Task kwargs: {task.kwargs}")
    print(f"Initial status: {job.status}")

    return job


async def main():
    """Main entry point that creates context and runs examples."""
    print("=" * 50)
    print("aaiclick Orchestration Basic Example")
    print("=" * 50)
    print("\nNote: This example requires:")
    print("      - Running PostgreSQL server (localhost:5432)")
    print("      - Database migrations applied (python -m aaiclick migrate)")
    print()

    async with OrchContext():
        # Example 1: Create a simple job
        job1 = await example_simple_job()

        # Example 2: Create a job with task parameters
        job2 = await example_job_with_task()

    # Test execution (runs outside OrchContext, creates its own)
    print("\n" + "=" * 50)
    print("Testing Job Execution (Synchronous Test Mode)")
    print("-" * 50)

    print(f"\nRunning job: {job1.name}")
    job1.test()  # Blocks until job completes
    print(f"Job status after test(): {job1.status}")
    assert job1.status == JobStatus.COMPLETED, f"Expected COMPLETED, got {job1.status}"

    print(f"\nRunning job: {job2.name}")
    job2.test()  # Blocks until job completes
    print(f"Job status after test(): {job2.status}")
    assert job2.status == JobStatus.COMPLETED, f"Expected COMPLETED, got {job2.status}"

    print("\n" + "=" * 50)
    print("All examples completed successfully!")
    print("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
