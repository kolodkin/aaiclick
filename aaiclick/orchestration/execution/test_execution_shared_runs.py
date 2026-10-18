"""run_job_tasks tests that each assert one aspect of a job run shared per module."""

import pytest
from sqlalchemy import select

from aaiclick.data.object_context import object_context
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.execution.runner import run_job_tasks
from aaiclick.orchestration.factories import create_job, create_task
from aaiclick.orchestration.fixtures.dict_return_pipelines import spawn_dicts
from aaiclick.orchestration.jobs import get_job_result
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, JOB_FAILED, TASK_FAILED, Dependency, Job, Task
from aaiclick.orchestration.orch_context import get_sql_session


@pytest.fixture(scope="module")
async def failing_job(orch_shared_ctx) -> Job:
    job = await create_job("test_job_fail", "aaiclick.orchestration.fixtures.sample_tasks.failing_task")
    await run_job_tasks(job)
    return job


@pytest.fixture(scope="module")
async def dict_return_job(orch_shared_ctx) -> Job:
    job = await create_job("dict_return", create_task(spawn_dicts, {"n": 1}, name="dict_return"))
    await run_job_tasks(job)
    return job


async def test_run_job_tasks_failing_task(failing_job):
    """Test running a job with a failing task."""
    assert failing_job.status == JOB_FAILED
    assert failing_job.error is not None
    assert "intentionally" in failing_job.error

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == failing_job.id))
        tasks = list(result.scalars().all())
        assert len(tasks) == 1
        assert tasks[0].status == TASK_FAILED
        assert tasks[0].error is not None


async def test_run_job_tasks_failing_task_logs_traceback(failing_job):
    """The run's own log ends with the exception and its traceback on stderr
    — each retry keeps why it failed, not only the task's latest error."""
    task = (await get_tasks_for_job(failing_job.id))[0]
    errors = [line.text for line in (await get_task_logs(task.id)).lines if line.stream == "stderr"]
    assert "Traceback (most recent call last):" in errors
    assert errors[-1] == "ValueError: This task failed intentionally"


async def test_run_job_tasks_dict_return_spawns_children(dict_return_job):
    """A task returning ``task`` dicts with handles spawns the children and
    its result is the finalize child's: double(1) + double(2) = 6."""
    assert dict_return_job.status == JOB_COMPLETED, dict_return_job.error
    async with object_context():
        assert await get_job_result(dict_return_job) == 6


async def test_run_job_tasks_dict_return_wires_data_flow_edges(dict_return_job):
    """A handle in a child's kwargs is a dependency edge, as a Task kwarg is in
    Python — otherwise a second worker claims ``fin`` before ``w1``/``w2``
    complete (an internal check: ``run_job_tasks`` runs children in id order
    and cannot show the race)."""
    tasks = await get_tasks_for_job(dict_return_job.id)
    doubles = [t for t in tasks if t.entrypoint.endswith(".double")]
    fin = next(t for t in tasks if t.entrypoint.endswith(".total"))
    assert len(doubles) == 2
    async with get_sql_session() as session:
        rows = (await session.execute(select(Dependency.previous_id).where(Dependency.next_id == fin.id))).all()
    assert {r[0] for r in rows} >= {d.id for d in doubles}
