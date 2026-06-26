from __future__ import annotations

from datetime import datetime

from aaiclick.orchestration.logging import _ChLogSink
from aaiclick.view_models import STDERR_STREAM, STDOUT_STREAM


def test_sink_default_levels_per_stream():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "out line\n")
    sink.write(STDERR_STREAM, "err line\n")
    lines = sink.finalize()
    assert [(l.stream, l.level, l.text) for l in lines] == [
        (STDOUT_STREAM, "INFO", "out line"),
        (STDERR_STREAM, "ERROR", "err line"),
    ]


def test_sink_record_applies_level_and_splits_multiline():
    sink = _ChLogSink()
    sink.record("WARNING", "first\nsecond")
    lines = sink.finalize()
    assert [(l.level, l.text) for l in lines] == [
        ("WARNING", "first"),
        ("WARNING", "second"),
    ]
    assert all(l.stream == STDERR_STREAM for l in lines)


def test_sink_stamps_each_line_with_created_at():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    lines = sink.finalize()
    assert all(isinstance(l.created_at, datetime) for l in lines)


def test_sink_record_drops_trailing_newline():
    sink = _ChLogSink()
    sink.record("INFO", "msg\n")
    lines = sink.finalize()
    assert [l.text for l in lines] == ["msg"]


def test_sink_record_preserves_internal_blank_lines():
    sink = _ChLogSink()
    sink.record("INFO", "a\n\nb")
    lines = sink.finalize()
    assert [l.text for l in lines] == ["a", "", "b"]


def test_sink_record_shares_one_timestamp_per_call():
    sink = _ChLogSink()
    sink.record("WARNING", "first\nsecond")
    lines = sink.finalize()
    assert lines[0].created_at == lines[1].created_at
