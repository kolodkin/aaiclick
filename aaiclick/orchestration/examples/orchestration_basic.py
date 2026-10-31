"""
Basic orchestration example for aaiclick.

Demonstrates @task and @job decorators:
1. Defining tasks with @task
2. Defining a workflow with @job
3. Passing one task's result into another (automatic dependency)
4. An independent task running alongside that chain
5. A report task joining both flows, returned as the job's final task
6. Running the job inline with ajob_test()
"""

import asyncio

from aaiclick import Object, create_object_from_value
from aaiclick.orchestration import JOB_COMPLETED, ajob_test, job, task


# --8<-- [start:tasks]
@task
async def simple_arithmetic() -> int:
    """A simple task that does basic arithmetic."""
    c = 1 + 2
    print(f"Computing: 1 + 2 = {c}")  # → 3
    return c


@task
async def multiply(x: int, y: int) -> int:
    """Receives ``x`` from simple_arithmetic's result."""
    result = x * y
    print(f"Computing: {x} * {y} = {result}")  # → 18
    return result


@task
async def create_sales() -> Object:
    """Independent flow: takes no input from other tasks."""
    return await create_object_from_value(
        {
            "product": ["Widget", "Gadget", "Gizmo"],
            "quantity": [10, 5, 8],
            "price": [9.99, 24.99, 14.99],
        }
    )


@task
async def report(factor: int, sales: Object) -> dict:
    """Joins both flows; runs after multiply and create_sales complete."""
    revenue = round(await (sales["quantity"] * sales["price"]).sum().data(), 2)
    summary = {"factor": factor, "revenue": revenue, "scaled": round(revenue * factor, 2)}
    print(f"Report: {summary}")  # → {'factor': 18, 'revenue': 344.77, 'scaled': 6205.86}
    return summary
    # --8<-- [end:tasks]


# --8<-- [start:job]
@job("basic_orchestration")
def basic_pipeline(y: int = 6):
    """Two flows — arithmetic chain and sales data — joined by a report."""
    arith = simple_arithmetic()
    product = multiply(x=arith, y=y)  # passing a Task makes multiply wait for it
    sales = create_sales()  # no inputs, so it runs in parallel with the chain
    return report(factor=product, sales=sales)
    # --8<-- [end:job]


async def amain():
    """Run the basic orchestration example."""
    # --8<-- [start:run]
    pipeline = await ajob_test(basic_pipeline)
    print(f"Ran job: {pipeline.name} (ID: {pipeline.id})")
    assert pipeline.status == JOB_COMPLETED, f"Expected COMPLETED, got {pipeline.status}: {pipeline.error}"
    print(f"Job status: {pipeline.status}")
    # --8<-- [end:run]


if __name__ == "__main__":
    asyncio.run(amain())
