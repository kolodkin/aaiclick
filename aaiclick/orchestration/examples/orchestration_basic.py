"""
Basic orchestration example for aaiclick.

Demonstrates @task and @job decorators:
1. Defining tasks with @task
2. Defining a workflow with @job
3. Task chaining with automatic dependencies
4. Returning Object data from tasks
5. Running the job inline with ajob_test()
"""

import asyncio

from aaiclick import Object, create_object_from_value
from aaiclick.orchestration import JOB_COMPLETED, JobStatus, ajob_test, job, task, tasks_list


@task
async def simple_arithmetic() -> int:
    """A simple task that does basic arithmetic."""
    a = 1
    b = 2
    c = a + b
    print(f"Computing: {a} + {b} = {c}")  # → 3
    return c


@task
async def multiply(x: int, y: int) -> int:
    """A task that takes parameters."""
    result = x * y
    print(f"Computing: {x} * {y} = {result}")  # → 30
    return result


@task
async def create_sales() -> Object:
    """Create a sales dataset and return as Object."""
    sales = await create_object_from_value(
        {
            "product": ["Widget", "Gadget", "Gizmo"],
            "quantity": [10, 5, 8],
            "price": [9.99, 24.99, 14.99],
        }
    )
    count = await (await sales.count()).data()
    print(f"Sales: {count} rows")  # → 3
    return sales


@job("basic_orchestration")
def basic_pipeline(x: int = 5, y: int = 6):
    """Pipeline with arithmetic tasks and Object data."""
    arith = simple_arithmetic()
    product = multiply(x=x, y=y)
    sales = create_sales()
    return tasks_list(arith, product, sales)


async def amain():
    """Run the basic orchestration example."""
    pipeline = await basic_pipeline()
    print(f"Created job: {pipeline.name} (ID: {pipeline.id})")

    await ajob_test(pipeline)
    assert pipeline.status == JOB_COMPLETED, f"Expected COMPLETED, got {pipeline.status}"
    print(f"Job status: {pipeline.status}")


if __name__ == "__main__":
    asyncio.run(amain())
