from __future__ import annotations

import logging

from aaiclick.view_models import LogLine, normalize_level


def test_normalize_level_exact_standard_levels():
    assert normalize_level(logging.DEBUG) == "DEBUG"
    assert normalize_level(logging.INFO) == "INFO"
    assert normalize_level(logging.WARNING) == "WARNING"
    assert normalize_level(logging.ERROR) == "ERROR"
    assert normalize_level(logging.CRITICAL) == "CRITICAL"


def test_normalize_level_buckets_custom_levels_down():
    assert normalize_level(25) == "INFO"  # between INFO and WARNING
    assert normalize_level(45) == "ERROR"  # between ERROR and CRITICAL
    assert normalize_level(100) == "CRITICAL"  # above CRITICAL


def test_normalize_level_below_debug_is_debug():
    assert normalize_level(0) == "DEBUG"  # NOTSET
    assert normalize_level(5) == "DEBUG"


def test_logline_defaults_level_info_and_stamps_created_at():
    line = LogLine(stream="stdout", text="hi")
    assert line.level == "INFO"
    assert line.created_at is not None
