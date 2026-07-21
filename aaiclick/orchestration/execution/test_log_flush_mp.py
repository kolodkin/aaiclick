"""Tests for the shell-log flush child.

Dedicated mp module: the parent holds no chdb session (``orch_ctx_no_ch``)
so the spawned flush/reader children can open chdb themselves.
"""

from aaiclick.orchestration.execution.log_flush import flush_shell_logs
from aaiclick.orchestration.execution.mp_log_reader import read_logs_via_child
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job


async def test_flush_shell_logs_lands_in_clickhouse(orch_ctx_no_ch):
    job = await create_job("shell_flush", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    await flush_shell_logs(task.id, job.id, 7, "line one\nline two\n")

    assert read_logs_via_child(task.id, 7) == ["line one", "line two"]


async def test_flush_shell_logs_empty_text_is_noop(orch_ctx_no_ch):
    await flush_shell_logs(1, 1, 1, "")
