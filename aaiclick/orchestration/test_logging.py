from __future__ import annotations

import sys
from datetime import timedelta

import pytest

from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM, LogLine
from aaiclick.orchestration.logging import (
    ChLogSink,
    capture_task_output,
    flush_task_logs,
    last_task_log_time,
    read_task_logs,
)
from aaiclick.testing import until


def test_sink_tags_lines_with_their_stream():
    sink = ChLogSink()
    sink.write(STDOUT_STREAM, "out line\n")
    sink.write(STDERR_STREAM, "err line\n")
    lines = sink.finalize()
    assert [(line.stream, line.text) for line in lines] == [
        (STDOUT_STREAM, "out line"),
        (STDERR_STREAM, "err line"),
    ]


def test_sink_drain_returns_completed_lines_and_clears():
    sink = ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    assert [line.text for line in sink.drain()] == ["a", "b"]
    assert sink.drain() == []


def test_sink_drain_holds_back_partial_line():
    sink = ChLogSink()
    sink.write(STDOUT_STREAM, "complete\npartial")
    assert [line.text for line in sink.drain()] == ["complete"]
    sink.write(STDOUT_STREAM, " tail\n")
    assert [line.text for line in sink.drain()] == ["partial tail"]


def test_sink_finalize_after_drain_flushes_partials():
    sink = ChLogSink()
    sink.write(STDOUT_STREAM, "done\nhalf")
    sink.drain()
    assert [line.text for line in sink.finalize()] == ["half"]


# capture_task_output / read_task_logs round trips through ClickHouse


@pytest.mark.parametrize(
    "emit, stream",
    [
        # Lambdas resolve sys.stdout / sys.stderr at call time, after capture swaps them.
        pytest.param(lambda text: print(text), STDOUT_STREAM, id="stdout"),
        pytest.param(lambda text: print(text, file=sys.stderr), STDERR_STREAM, id="stderr"),
    ],
)
async def test_capture_task_output_stream(orch_ctx, emit, stream):
    """Text printed inside the capture scope lands in CH task_logs under its stream."""
    task_id, job_id, run_id = 12345, 99, 555

    async with capture_task_output(task_id, job_id, run_id):
        emit("Hello, world!")

    lines = await read_task_logs(task_id, run_id)
    assert any(line.text == "Hello, world!" and line.stream == stream for line in lines)


async def test_capture_task_output_streams_mid_run(orch_ctx, monkeypatch):
    """Completed lines are readable from task_logs while the task body is still running."""
    monkeypatch.setattr("aaiclick.orchestration.logging.LOG_FLUSH_INTERVAL", 0.05)
    task_id, job_id, run_id = 71, 1, 9101
    async with capture_task_output(task_id, job_id, run_id):
        print("early line")
        flushed = await until(lambda: read_task_logs(task_id, run_id), timeout=30, what="'early line' in task_logs")
        mid_run_lines = [line.text for line in flushed]
        print("late line")
    assert mid_run_lines == ["early line"]
    final = [line.text for line in await read_task_logs(task_id, run_id)]
    assert final == ["early line", "late line"]


async def test_last_task_log_time_is_newest_line(orch_ctx):
    task_id, job_id, run_id = 777, 99, 556
    assert await last_task_log_time(task_id, run_id) is None
    await flush_task_logs(task_id, job_id, run_id, [LogLine(stream=STDOUT_STREAM, text="first")])
    await flush_task_logs(task_id, job_id, run_id, [LogLine(stream=STDOUT_STREAM, text="second")], seq_offset=1)

    newest = await last_task_log_time(task_id, run_id)
    assert newest is not None

    lines = await read_task_logs(task_id, run_id)
    assert newest.utcoffset() == timedelta(0)
    assert newest.replace(tzinfo=None) == lines[-1].created_at >= lines[0].created_at
