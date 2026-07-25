from __future__ import annotations

from datetime import datetime

from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM
from aaiclick.orchestration.logging import _ChLogSink, shell_text_to_lines


def test_sink_default_levels_per_stream():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "out line\n")
    sink.write(STDERR_STREAM, "err line\n")
    lines = sink.finalize()
    assert [(line.stream, line.level, line.text) for line in lines] == [
        (STDOUT_STREAM, "INFO", "out line"),
        (STDERR_STREAM, "ERROR", "err line"),
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


def test_sink_record_drops_trailing_newline():
    sink = _ChLogSink()
    sink.record("INFO", "msg\n")
    lines = sink.finalize()
    assert [line.text for line in lines] == ["msg"]


def test_sink_record_preserves_internal_blank_lines():
    sink = _ChLogSink()
    sink.record("INFO", "a\n\nb")
    lines = sink.finalize()
    assert [line.text for line in lines] == ["a", "", "b"]


def test_sink_record_shares_one_timestamp_per_call():
    sink = _ChLogSink()
    sink.record("WARNING", "first\nsecond")
    lines = sink.finalize()
    assert lines[0].created_at == lines[1].created_at


def test_shell_text_to_lines_splits_and_tags():
    lines = shell_text_to_lines("a\nb\n")
    assert [(line.stream, line.level, line.text) for line in lines] == [
        (STDOUT_STREAM, "INFO", "a"),
        (STDOUT_STREAM, "INFO", "b"),
    ]


def test_shell_text_to_lines_keeps_unterminated_tail():
    assert [line.text for line in shell_text_to_lines("a\nb")] == ["a", "b"]


def test_shell_text_to_lines_preserves_internal_blank_lines():
    assert [line.text for line in shell_text_to_lines("a\n\nb\n")] == ["a", "", "b"]


def test_shell_text_to_lines_empty_text():
    assert shell_text_to_lines("") == []
