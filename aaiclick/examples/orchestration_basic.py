"""
Basic orchestration example for aaiclick.

This example demonstrates how to create and execute jobs using the
orchestration backend with @task and @job decorators. It shows:
1. Defining tasks with the @task decorator
2. Defining a workflow with the @job decorator
3. Executing the job using job_test() for synchronous testing
4. Using ajob_test() for async contexts

Note: This requires a running PostgreSQL server for job/task state storage.
"""

import asyncio

from aaiclick.orchestration import JobStatus, TaskResult, ajob_test, job, task


# Define sample task functions using @task decorator
@task
async def simple_arithmetic() -> int:
    """A simple task that does basic arithmetic and prints the result."""
    a = 1
    b = 2
    c = a + b
    print(f"Computing: {a} + {b} = {c}")
    return c


@task
async def multiply(x: int, y: int) -> int:
    """A task that takes parameters and prints their product."""
    result = x * y
    print(f"Computing: {x} * {y} = {result}")
    return result


# Define workflows using @job decorator
@job("simple_arithmetic_job")
def simple_job():
    """A simple job with one task."""
    result = simple_arithmetic()
    return TaskResult(tasks=[result])


@job("parametrized_job")
def parametrized_job(x: int, y: int):
    """A job with a parametrized task."""
    result = multiply(x=x, y=y)
    return TaskResult(tasks=[result])


@job("chained_job")
def chained_job(x: int, y: int):
    """A job demonstrating task chaining with automatic dependencies."""
    # First task computes arithmetic
    sum_result = simple_arithmetic()

    # Second task uses parameters - both tasks run in parallel since
    # multiply doesn't depend on simple_arithmetic
    product = multiply(x=x, y=y)

    return TaskResult(tasks=[sum_result, product])


async def amain():
    """
    Async entry point using ajob_test() for async contexts.

    Use this when already inside an async context (e.g., pytest-asyncio tests,
    async web handlers). ajob_test() can be awaited without creating a new
    event loop.
    """
    print("=" * 50)
    print("aaiclick Orchestration Basic Example (Async)")
    print("=" * 50)
    print("\nNote: This example requires:")
    print("      - Running PostgreSQL server (localhost:5432)")
    print("      - Database migrations applied (python -m aaiclick migrate)")
    print()

    # Example 1: Simple job with one task
    print("\n" + "=" * 50)
    print("Example 1: Simple Job with @task and @job")
    print("-" * 50)

    job1 = await simple_job()

    print(f"Created job: {job1.name} (ID: {job1.id})")

    # Test execution using ajob_test() - the async variant
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


if __name__ == "__main__":
    asyncio.run(amain())
