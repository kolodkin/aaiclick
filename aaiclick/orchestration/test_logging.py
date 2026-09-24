from __future__ import annotations

from datetime import datetime

import pytest

from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM
from aaiclick.orchestration.logging import _ChLogSink


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
