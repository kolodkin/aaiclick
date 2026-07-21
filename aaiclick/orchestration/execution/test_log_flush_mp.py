"""Tests for the shell-log flush child.

Dedicated mp module: the parent holds no chdb session (``orch_ctx_no_ch``)
so the spawned flush/reader children can open chdb themselves.
"""

import asyncio
import multiprocessing

from aaiclick.orchestration.execution.log_flush import flush_shell_logs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import read_task_logs
from aaiclick.orchestration.orch_context import orch_context

_mp_ctx = multiprocessing.get_context("spawn")


def _read_logs_child_target(task_id: int, run_id: int, queue: multiprocessing.Queue) -> None:
    async def _run() -> None:
        async with orch_context():
            lines = await read_task_logs(task_id, run_id)
            queue.put([line.text for line in lines])

    asyncio.run(_run())


def _read_logs_via_child(task_id: int, run_id: int) -> list[str]:
    queue = _mp_ctx.Queue()
    proc = _mp_ctx.Process(target=_read_logs_child_target, args=(task_id, run_id, queue), daemon=True)
    proc.start()
    texts = queue.get(timeout=60)
    proc.join()
    return texts


async def test_flush_shell_logs_lands_in_clickhouse(orch_ctx_no_ch):
    job = await create_job("shell_flush", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    await flush_shell_logs(task.id, job.id, 7, "line one\nline two\n")

    assert _read_logs_via_child(task.id, 7) == ["line one", "line two"]


async def test_flush_shell_logs_empty_text_is_noop(orch_ctx_no_ch):
    await flush_shell_logs(1, 1, 1, "")
