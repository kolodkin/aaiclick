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


@pytest.mark.parametrize(
    "level, expected",
    [
        pytest.param(logging.DEBUG, "DEBUG", id="debug"),
        pytest.param(logging.INFO, "INFO", id="info"),
        pytest.param(logging.WARNING, "WARNING", id="warning"),
        pytest.param(logging.ERROR, "ERROR", id="error"),
        pytest.param(logging.CRITICAL, "CRITICAL", id="critical"),
        # Custom levels bucket down to the nearest standard level.
        pytest.param(25, "INFO", id="custom-between-info-and-warning"),
        pytest.param(45, "ERROR", id="custom-between-error-and-critical"),
        pytest.param(100, "CRITICAL", id="custom-above-critical"),
        # Anything below DEBUG, NOTSET included, is DEBUG.
        pytest.param(logging.NOTSET, "DEBUG", id="notset"),
        pytest.param(5, "DEBUG", id="below-debug"),
    ],
)
def test_normalize_level(level, expected):
    assert normalize_level(level) == expected


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


def test_log_line_created_at_read_in_server_zone_is_utc():
    """chdb returns a zone-less DateTime64 column in the host's zone, so a
    non-UTC host reads task_logs.created_at as aware local time."""
    tokyo = timezone(timedelta(hours=9))
    line = LogLine(stream="stdout", text="x", created_at=datetime(2030, 6, 1, 21, 0, tzinfo=tokyo))
    assert line.created_at == datetime(2030, 6, 1, 12, 0) and line.created_at.tzinfo is None
