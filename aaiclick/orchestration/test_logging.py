from __future__ import annotations

import asyncio
import io
import logging
import sys
import time
from datetime import datetime

import pytest

from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM
from aaiclick.orchestration.logging import ChLogSink, capture_task_output, read_task_logs


def test_sink_tags_lines_with_their_stream():
    sink = ChLogSink()
    sink.write(STDOUT_STREAM, "out line\n")
    sink.write(STDERR_STREAM, "err line\n")
    lines = sink.finalize()
    assert [(line.stream, line.text) for line in lines] == [
        (STDOUT_STREAM, "out line"),
        (STDERR_STREAM, "err line"),
    ]


def test_sink_stamps_each_line_with_created_at():
    sink = ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    lines = sink.finalize()
    assert all(isinstance(line.created_at, datetime) for line in lines)


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
    mid_run_lines: list[str] = []
    async with capture_task_output(task_id, job_id, run_id):
        print("early line")
        deadline = time.monotonic() + 30
        while not (mid_run_lines := [line.text for line in await read_task_logs(task_id, run_id)]):
            assert time.monotonic() < deadline, "'early line' was never flushed to task_logs"
            await asyncio.sleep(0.05)
        print("late line")
    assert mid_run_lines == ["early line"]
    final = [line.text for line in await read_task_logs(task_id, run_id)]
    assert final == ["early line", "late line"]


async def test_capture_leaves_logging_untouched(orch_ctx):
    """Only stdout / stderr are captured: ``logging`` keeps its own handlers,
    so a record sent to a handler bound elsewhere stays out of the task log."""
    task_id, job_id, run_id = 81, 1, 81
    root = logging.getLogger()
    handlers_before, level_before = root.handlers[:], root.level
    elsewhere = logging.StreamHandler(io.StringIO())
    log = logging.getLogger("sample")
    log.addHandler(elsewhere)
    try:
        async with capture_task_output(task_id, job_id, run_id):
            assert root.handlers == handlers_before and root.level == level_before
            log.error("routed elsewhere")
            print("plain stdout")
    finally:
        log.removeHandler(elsewhere)

    assert [line.text for line in await read_task_logs(task_id, run_id)] == ["plain stdout"]
