from __future__ import annotations

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
    assert all(l.created_at is not None for l in lines)
