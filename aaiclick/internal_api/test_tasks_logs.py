from __future__ import annotations

import logging
from datetime import datetime

import pytest
from sqlmodel import select

from aaiclick.internal_api.errors import NotFound
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.logging import capture_task_output, flush_task_logs, read_task_logs
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


async def test_capture_records_true_level_and_restores_root(orch_ctx):
    job = await create_job("cap_levels", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 81

    root = logging.getLogger()
    before_handlers = list(root.handlers)
    before_level = root.level

    async with capture_task_output(task.id, job.id, run_id):
        logging.getLogger("sample").warning("a warning")
        logging.getLogger("sample").error("an error")
        print("plain stdout")

    assert list(root.handlers) == before_handlers
    assert root.level == before_level

    lines = await read_task_logs(task.id, run_id)
    by_text = {line.text: line.level for line in lines}
    assert by_text["WARNING:sample:a warning"] == "WARNING"
    assert by_text["ERROR:sample:an error"] == "ERROR"
    assert by_text["plain stdout"] == "INFO"


async def test_capture_no_duplicate_rows_with_preexisting_handler(orch_ctx):
    job = await create_job("cap_dedup", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 82

    noisy = logging.getLogger()
    extra = logging.StreamHandler()
    noisy.addHandler(extra)
    try:
        async with capture_task_output(task.id, job.id, run_id):
            logging.getLogger("sample").error("once only")
        assert extra in noisy.handlers  # root handlers restored on exit
    finally:
        noisy.removeHandler(extra)

    lines = await read_task_logs(task.id, run_id)
    assert [line.text for line in lines].count("ERROR:sample:once only") == 1


async def test_capture_tolerates_invalid_log_level_env(orch_ctx, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOG_LEVEL", "verbose")
    job = await create_job("cap_badenv", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 83

    root = logging.getLogger()
    before_handlers = list(root.handlers)
    before_level = root.level

    async with capture_task_output(task.id, job.id, run_id):
        logging.getLogger("sample").error("still captured")

    assert list(root.handlers) == before_handlers
    assert root.level == before_level

    lines = await read_task_logs(task.id, run_id)
    assert any(line.text == "ERROR:sample:still captured" for line in lines)
