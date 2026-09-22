from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import BaseModel, ValidationError

from aaiclick.log_models import MAX_PAGE_LIMIT, LogLine, PageLimit, PageOffset, UtcDateTime, normalize_level


class _Paged(BaseModel):
    limit: PageLimit = 50
    offset: PageOffset = 0


class _Stamped(BaseModel):
    at: UtcDateTime


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


@pytest.mark.parametrize(
    "field, value",
    [
        pytest.param("limit", 0, id="limit-zero"),
        pytest.param("limit", -5, id="limit-negative"),
        pytest.param("limit", MAX_PAGE_LIMIT + 1, id="limit-over-cap"),
        pytest.param("offset", -1, id="offset-negative"),
    ],
)
def test_page_bounds_reject_out_of_range(field, value):
    with pytest.raises(ValidationError):
        _Paged(**{field: value})


def test_page_bounds_accept_the_cap():
    assert _Paged(limit=MAX_PAGE_LIMIT, offset=0).limit == MAX_PAGE_LIMIT


@pytest.mark.parametrize(
    "value, expected",
    [
        pytest.param("2030-06-01T12:00:00Z", datetime(2030, 6, 1, 12, 0), id="utc-suffix"),
        pytest.param("2030-06-01T12:00:00+02:00", datetime(2030, 6, 1, 10, 0), id="offset-shifted-to-utc"),
        pytest.param(datetime(2030, 6, 1, 12, 0), datetime(2030, 6, 1, 12, 0), id="naive-taken-as-utc"),
        pytest.param(
            datetime(2030, 6, 1, 12, 0, tzinfo=timezone(timedelta(hours=-3))), datetime(2030, 6, 1, 15, 0), id="aware"
        ),
    ],
)
def test_utc_datetime_normalizes_to_naive_utc(value, expected):
    at = _Stamped.model_validate({"at": value}).at
    assert at == expected and at.tzinfo is None
