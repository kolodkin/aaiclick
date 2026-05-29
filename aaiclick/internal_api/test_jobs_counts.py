from __future__ import annotations

from sqlmodel import select

from aaiclick.internal_api.jobs import list_jobs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import TASK_COMPLETED, Task
from aaiclick.orchestration.orch_context import get_sql_session


async def _mark_completed(task_id: int) -> None:
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.id == task_id))).scalar_one()
        task.status = TASK_COMPLETED
        s.add(task)
        await s.commit()


async def test_list_jobs_reports_task_counts(orch_ctx):
    job = await create_job("counts_job", simple_task)
    tasks = await get_tasks_for_job(job.id)
    await _mark_completed(tasks[0].id)

    page = await list_jobs()
    view = next(j for j in page.items if j.id == job.id)

    assert view.total_tasks == len(tasks)
    assert view.completed_tasks == 1
