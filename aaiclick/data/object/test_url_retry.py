"""
Tests for ``_url_retry`` — retryable-error predicate and retry wrapper.

Covers:
    * predicate decisions for HTTP status codes, urllib errors, network
      exceptions, and clickhouse-connect / chdb driver-style messages
    * ``with_url_retry`` retries transient failures, fails fast on
      non-retryable, exhausts attempt budget, and applies exponential backoff
"""

from __future__ import annotations

import socket
import urllib.error
from unittest.mock import AsyncMock, patch

import pytest

from aaiclick.data.object._url_retry import (
    DEFAULT_BACKOFF_FACTOR,
    DEFAULT_RETRIES,
    _is_retryable_url_error,
    with_url_retry,
)


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(
        url="http://example.com", code=code, msg="x", hdrs=None, fp=None  # type: ignore[arg-type]
    )


@pytest.mark.parametrize(
    "exc,expected",
    [
        # Retryable HTTP status codes
        (_http_error(429), True),
        (_http_error(500), True),
        (_http_error(502), True),
        (_http_error(503), True),
        (_http_error(504), True),
        # Non-retryable HTTP status codes
        (_http_error(400), False),
        (_http_error(401), False),
        (_http_error(403), False),
        (_http_error(404), False),
        (_http_error(410), False),
        # Network-level exceptions
        (urllib.error.URLError(reason=ConnectionResetError()), True),
        (urllib.error.URLError(reason=TimeoutError()), True),
        (urllib.error.URLError(reason=socket.gaierror(8, "nodename nor servname provided")), True),
        (TimeoutError("read timeout"), True),
        (ConnectionResetError("reset"), True),
        (ConnectionRefusedError("refused"), True),
        (socket.timeout("socket timeout"), True),
        (socket.gaierror(8, "DNS lookup failed"), True),
        # CH-server-side wrapped errors (clickhouse-connect / chdb message shapes)
        (RuntimeError("HTTP/1.1 502 Bad Gateway from upstream"), True),
        (RuntimeError("Received 503 Service Unavailable"), True),
        (RuntimeError("Got 504 Gateway Timeout"), True),
        (RuntimeError("HTTP 429 Too Many Requests"), True),
        (RuntimeError("Code: 86. DB::Exception: RECEIVED_ERROR_FROM_REMOTE_IO_SERVER"), True),
        (RuntimeError("Code: 210. DB::Exception: NETWORK_ERROR while reading"), True),
        # Non-retryable: unknown / non-network errors
        (ValueError("bad arg"), False),
        (RuntimeError("Code: 47. DB::Exception: Unknown identifier"), False),
        (RuntimeError("syntax error at line 1"), False),
    ],
)
def test_is_retryable_url_error(exc, expected):
    assert _is_retryable_url_error(exc) is expected


async def test_with_url_retry_returns_first_success():
    """No failure → single call, no sleep."""
    fn = AsyncMock(return_value="ok")
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        result = await with_url_retry(fn, retries=4, backoff_factor=2.0)
    assert result == "ok"
    assert fn.call_count == 1
    sleep_mock.assert_not_called()


async def test_with_url_retry_retries_transient_then_succeeds():
    """Transient failure on attempts 1 and 2, success on 3 → 3 calls, 2 sleeps."""
    fn = AsyncMock(
        side_effect=[
            _http_error(503),
            ConnectionResetError("reset"),
            "ok",
        ]
    )
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        result = await with_url_retry(fn, retries=4, backoff_factor=2.0)
    assert result == "ok"
    assert fn.call_count == 3
    # Sleeps before attempts 2 and 3: 2.0**1 = 2, 2.0**2 = 4
    assert [call.args[0] for call in sleep_mock.call_args_list] == [2.0, 4.0]


async def test_with_url_retry_fails_fast_on_non_retryable():
    """Non-retryable error on attempt 1 → no retry."""
    fn = AsyncMock(side_effect=ValueError("bad arg"))
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        with pytest.raises(ValueError, match="bad arg"):
            await with_url_retry(fn, retries=4, backoff_factor=2.0)
    assert fn.call_count == 1
    sleep_mock.assert_not_called()


async def test_with_url_retry_exhausts_budget_and_reraises():
    """All retryable failures → exactly ``retries`` calls then re-raises last."""
    fn = AsyncMock(side_effect=_http_error(502))
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        with pytest.raises(urllib.error.HTTPError) as exc_info:
            await with_url_retry(fn, retries=3, backoff_factor=2.0)
    assert exc_info.value.code == 502
    assert fn.call_count == 3
    # Two sleeps between three attempts: 2.0**1, 2.0**2.
    assert [call.args[0] for call in sleep_mock.call_args_list] == [2.0, 4.0]


async def test_with_url_retry_single_attempt_disables_retry():
    """``retries=1`` → no retry on transient failure."""
    fn = AsyncMock(side_effect=_http_error(503))
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        with pytest.raises(urllib.error.HTTPError):
            await with_url_retry(fn, retries=1, backoff_factor=2.0)
    assert fn.call_count == 1
    sleep_mock.assert_not_called()


async def test_with_url_retry_custom_backoff_factor():
    """Backoff factor controls sleep duration: factor ** attempt."""
    fn = AsyncMock(side_effect=[_http_error(503), _http_error(503), "ok"])
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        result = await with_url_retry(fn, retries=4, backoff_factor=3.0)
    assert result == "ok"
    # Sleeps: 3.0**1 = 3, 3.0**2 = 9
    assert [call.args[0] for call in sleep_mock.call_args_list] == [3.0, 9.0]


async def test_with_url_retry_zero_backoff():
    """``backoff_factor=0`` produces zero-second sleeps (still calls sleep)."""
    fn = AsyncMock(side_effect=[_http_error(503), "ok"])
    with patch("aaiclick.data.object._url_retry.asyncio.sleep") as sleep_mock:
        await with_url_retry(fn, retries=4, backoff_factor=0)
    # 0 ** 1 = 0
    assert [call.args[0] for call in sleep_mock.call_args_list] == [0]


@pytest.mark.parametrize(
    "retries,backoff_factor,match",
    [
        (0, 2.0, "retries must be >= 1"),
        (-1, 2.0, "retries must be >= 1"),
        (4, -1.0, "backoff_factor must be >= 0"),
    ],
)
async def test_with_url_retry_validates_args(retries, backoff_factor, match):
    fn = AsyncMock(return_value="ok")
    with pytest.raises(ValueError, match=match):
        await with_url_retry(fn, retries=retries, backoff_factor=backoff_factor)


def test_default_constants():
    """Defaults match the future.md spec: 4 attempts, 2x backoff (2/4/8 s)."""
    assert DEFAULT_RETRIES == 4
    assert DEFAULT_BACKOFF_FACTOR == 2.0
