from __future__ import annotations

from datetime import datetime

import pytest
from sqlmodel import select

from aaiclick.internal_api.errors import NotFound
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import flush_task_logs
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.view_models import STDERR_STREAM, STDOUT_STREAM, LogLine


def _out(*texts: str) -> list[LogLine]:
    return [LogLine(stream=STDOUT_STREAM, text=t) for t in texts]


def _texts(lines: list[LogLine]) -> list[str]:
    return [line.text for line in lines]


async def _set_run_ids(task_id: int, run_ids: list[int]) -> None:
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.id == task_id))).scalar_one()
        task.run_ids = run_ids
        s.add(task)
        await s.commit()


async def test_logs_unavailable_when_task_never_ran(orch_ctx):
    job = await create_job("logs_none", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_read_from_clickhouse(orch_ctx):
    job = await create_job("logs_ch", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 42
    await flush_task_logs(task.id, job.id, run_id, _out("line one", "line two"))
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    assert result.available is True
    assert _texts(result.lines) == ["line one", "line two"]


async def test_logs_preserve_stream_tags(orch_ctx):
    job = await create_job("logs_streams", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 5
    await flush_task_logs(
        task.id,
        job.id,
        run_id,
        [LogLine(stream=STDOUT_STREAM, text="to stdout"), LogLine(stream=STDERR_STREAM, text="to stderr")],
    )
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    assert [(line.stream, line.text) for line in result.lines] == [
        (STDOUT_STREAM, "to stdout"),
        (STDERR_STREAM, "to stderr"),
    ]


async def test_logs_read_latest_run_only(orch_ctx):
    job = await create_job("logs_latest", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await flush_task_logs(task.id, job.id, 1, _out("first attempt"))
    await flush_task_logs(task.id, job.id, 2, _out("second attempt"))
    await _set_run_ids(task.id, [1, 2])

    result = await get_task_logs(task.id)

    assert result.available is True
    assert _texts(result.lines) == ["second attempt"]


async def test_logs_tail_returns_last_n_in_order(orch_ctx):
    job = await create_job("logs_tail", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 99
    await flush_task_logs(task.id, job.id, run_id, _out("a", "b", "c", "d"))
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id, tail=2)

    assert result.available is True
    assert _texts(result.lines) == ["c", "d"]


async def test_logs_unavailable_when_run_has_no_lines(orch_ctx):
    job = await create_job("logs_empty", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await _set_run_ids(task.id, [7])

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_not_found_raises(orch_ctx):
    with pytest.raises(NotFound):
        await get_task_logs(999999999)


async def test_logs_preserve_level(orch_ctx):
    job = await create_job("logs_level", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 71
    await flush_task_logs(
        task.id,
        job.id,
        run_id,
        [
            LogLine(stream=STDOUT_STREAM, level="INFO", text="info line"),
            LogLine(stream=STDERR_STREAM, level="ERROR", text="error line"),
        ],
    )
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    assert [(line.level, line.text) for line in result.lines] == [
        ("INFO", "info line"),
        ("ERROR", "error line"),
    ]


async def test_logs_preserve_per_line_created_at(orch_ctx):
    job = await create_job("logs_ts", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 72
    early = datetime(2020, 1, 1, 0, 0, 0)
    late = datetime(2020, 1, 1, 0, 0, 5)
    await flush_task_logs(
        task.id,
        job.id,
        run_id,
        [
            LogLine(stream=STDOUT_STREAM, text="first", created_at=early),
            LogLine(stream=STDOUT_STREAM, text="second", created_at=late),
        ],
    )
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    stamps = [line.created_at for line in result.lines]
    assert stamps[0].replace(microsecond=0) == early
    assert stamps[1].replace(microsecond=0) == late
