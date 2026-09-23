"""Tests for job_test / ajob_test debug execution."""

import pytest

from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.jobs import get_job_result
from aaiclick.orchestration.models import JOB_COMPLETED
from aaiclick.orchestration.result import task_result


@task
async def add(a: int, b: int) -> int:
    return a + b


@job("test_debug_add")
def add_pipeline(a: int, b: int):
    total = add(a=a, b=b)
    return task_result(data=total, tasks=[total])


async def test_ajob_test_accepts_job_factory(orch_ctx):
    """A @job factory is created with the given kwargs, then run."""
    j = await ajob_test(add_pipeline, a=3, b=4)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == 7


async def test_ajob_test_rejects_kwargs_with_created_job(orch_ctx):
    j = await add_pipeline(a=3, b=4)

    with pytest.raises(TypeError, match="only accepted with a @job factory"):
        await ajob_test(j, a=1)
