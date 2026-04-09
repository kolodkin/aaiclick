"""End-to-end test for Object lifecycle with per-consumer pins.

Verifies that Objects passed between tasks survive cleanup:
- Single consumer: A → B
- Multi-consumer fan-out: A → B, A → C → D
- Chain: A → B → C
- Diamond: A → (B, C) → D
"""

from aaiclick.data.data_context import create_object_from_value
from aaiclick.data.object import Object
from aaiclick.orchestration import tasks_list
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.models import JobStatus
from aaiclick.orchestration.orch_context import get_sql_session


# --- Task fixtures (module-level for entrypoint resolution) ---


@task
async def produce() -> Object:
    return await create_object_from_value([10, 20, 30])


@task
async def double(data: Object) -> Object:
    return await (data * 2)


@task
async def add_ten(data: Object) -> Object:
    return await (data + 10)


@task
async def add_objects(a: Object, b: Object) -> Object:
    return await (a + b)


@task
async def read_sum(data: Object) -> dict:
    result = await data.sum()
    return {"total": await result.data()}


# --- Job pipelines (module-level for entrypoint resolution) ---


@job("lifecycle_single")
def single_consumer_pipeline():
    data = produce()
    return read_sum(data=data)


@job("lifecycle_fan_out")
def fan_out_pipeline():
    data = produce()
    left = double(data=data)
    right = add_ten(data=data)
    merged = add_objects(a=left, b=right)
    return read_sum(data=merged)


@job("lifecycle_chain")
def chain_pipeline():
    data = produce()
    doubled = double(data=data)
    return read_sum(data=doubled)


@job("lifecycle_diamond")
def diamond_pipeline():
    data = produce()
    left = double(data=data)
    right = add_ten(data=data)
    merged = add_objects(a=left, b=right)
    return read_sum(data=merged)


# --- Tests ---


async def _run_and_assert_completed(pipeline_fn):
    job_obj = await pipeline_fn()
    await ajob_test(job_obj)

    from sqlmodel import select
    from aaiclick.orchestration.models import Job

    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job_obj.id))
        db_job = result.scalar_one()
        assert db_job.status == JobStatus.COMPLETED, f"Job failed: {db_job.error}"


async def test_single_consumer(orch_ctx):
    """A → B: Object survives for single consumer."""
    await _run_and_assert_completed(single_consumer_pipeline)


async def test_fan_out(orch_ctx):
    """A → (B, C) → D: Object survives for both consumers."""
    await _run_and_assert_completed(fan_out_pipeline)


async def test_chain(orch_ctx):
    """A → B → C: Each intermediate Object survives."""
    await _run_and_assert_completed(chain_pipeline)


async def test_diamond(orch_ctx):
    """A → (B, C) → D: Diamond dependency with shared Object."""
    await _run_and_assert_completed(diamond_pipeline)
