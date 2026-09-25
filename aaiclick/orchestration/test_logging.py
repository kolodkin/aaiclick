from __future__ import annotations

import asyncio
import logging
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime

import pytest

from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM
from aaiclick.orchestration.logging import _ChLogSink, capture_task_output, read_task_logs


@contextmanager
def _root_logger_restored() -> Iterator[None]:
    """Assert the root logger's handlers and level are unchanged when the block exits."""
    root = logging.getLogger()
    before_handlers = list(root.handlers)
    before_level = root.level
    yield
    assert list(root.handlers) == before_handlers
    assert root.level == before_level


def test_sink_default_levels_per_stream():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "out line\n")
    sink.write(STDERR_STREAM, "err line\n")
    lines = sink.finalize()
    assert [(line.stream, line.level, line.text) for line in lines] == [
        (STDOUT_STREAM, "INFO", "out line"),
        (STDERR_STREAM, "WARNING", "err line"),
    ]


def test_sink_record_applies_level_and_splits_multiline():
    sink = _ChLogSink()
    sink.record("WARNING", "first\nsecond")
    lines = sink.finalize()
    assert [(line.level, line.text) for line in lines] == [
        ("WARNING", "first"),
        ("WARNING", "second"),
    ]
    assert all(line.stream == STDERR_STREAM for line in lines)


def test_sink_stamps_each_line_with_created_at():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    lines = sink.finalize()
    assert all(isinstance(line.created_at, datetime) for line in lines)


@pytest.mark.parametrize(
    "message, expected",
    [
        pytest.param("msg\n", ["msg"], id="drops-trailing-newline"),
        pytest.param("a\n\nb", ["a", "", "b"], id="preserves-internal-blank-lines"),
    ],
)
def test_sink_record_splits_lines(message, expected):
    sink = _ChLogSink()
    sink.record("INFO", message)
    lines = sink.finalize()
    assert [line.text for line in lines] == expected


def test_sink_drain_returns_completed_lines_and_clears():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    assert [line.text for line in sink.drain()] == ["a", "b"]
    assert sink.drain() == []


def test_sink_drain_holds_back_partial_line():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "complete\npartial")
    assert [line.text for line in sink.drain()] == ["complete"]
    sink.write(STDOUT_STREAM, " tail\n")
    assert [line.text for line in sink.drain()] == ["partial tail"]


def test_sink_finalize_after_drain_flushes_partials():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "done\nhalf")
    sink.drain()
    assert [line.text for line in sink.finalize()] == ["half"]


def test_sink_record_shares_one_timestamp_per_call():
    sink = _ChLogSink()
    sink.record("WARNING", "first\nsecond")
    lines = sink.finalize()
    assert lines[0].created_at == lines[1].created_at


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


async def test_capture_records_true_level_and_restores_root(orch_ctx):
    task_id, job_id, run_id = 81, 1, 81

    with _root_logger_restored():
        async with capture_task_output(task_id, job_id, run_id):
            logging.getLogger("sample").warning("a warning")
            logging.getLogger("sample").error("an error")
            print("plain stdout")

    lines = await read_task_logs(task_id, run_id)
    by_text = {line.text: line.level for line in lines}
    assert by_text["WARNING:sample:a warning"] == "WARNING"
    assert by_text["ERROR:sample:an error"] == "ERROR"
    assert by_text["plain stdout"] == "INFO"


async def test_capture_no_duplicate_rows_with_preexisting_handler(orch_ctx):
    task_id, job_id, run_id = 82, 1, 82

    root = logging.getLogger()
    extra = logging.StreamHandler()
    root.addHandler(extra)
    try:
        with _root_logger_restored():
            async with capture_task_output(task_id, job_id, run_id):
                logging.getLogger("sample").error("once only")
    finally:
        root.removeHandler(extra)

    lines = await read_task_logs(task_id, run_id)
    assert [line.text for line in lines].count("ERROR:sample:once only") == 1


async def test_capture_tolerates_invalid_log_level_env(orch_ctx, monkeypatch):
    monkeypatch.setenv("AAICLICK_LOG_LEVEL", "verbose")
    task_id, job_id, run_id = 83, 1, 83

    with _root_logger_restored():
        async with capture_task_output(task_id, job_id, run_id):
            logging.getLogger("sample").error("still captured")

    lines = await read_task_logs(task_id, run_id)
    assert any(line.text == "ERROR:sample:still captured" for line in lines)
