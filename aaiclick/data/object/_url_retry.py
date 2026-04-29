"""
aaiclick.data.object._url_retry - Retry helper for URL-fetching operations.

``create_object_from_url`` and ``Object.insert_from_url`` issue
``INSERT … FROM url(…)`` queries that are routinely killed by transient
upstream blips (502/503/504, socket resets, DNS hiccups) on public datasets
like Wikidata SPARQL or HuggingFace CDN. This module provides a thin retry
wrapper around those calls with exponential backoff for a bounded set of
retryable errors.

Backend differences:
    * **chdb**: HTTP fetch happens in Python via ``urllib.request.urlretrieve``
      (see ``chdb_client._rewrite_external_urls``); errors are
      ``urllib.error.HTTPError`` / ``URLError`` / ``OSError``.
    * **clickhouse-connect**: HTTP fetch happens server-side; errors come back
      wrapped in driver exceptions whose message text carries the original
      status code or ClickHouse error code.

The predicate inspects both shapes.
"""

from __future__ import annotations

import asyncio
import re
import socket
import urllib.error
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar("T")

DEFAULT_RETRIES = 4
DEFAULT_BACKOFF_FACTOR = 2.0

_RETRYABLE_HTTP_CODES = frozenset({429, 500, 502, 503, 504})

# clickhouse-connect / chdb wrap server-side HTTP failures into driver
# exceptions whose ``str()`` includes either the upstream HTTP status line or
# a ClickHouse error code/name. Detecting both keeps the predicate uniform
# across backends.
_HTTP_STATUS_RE = re.compile(r"\b(429|500|502|503|504)\b")
_RETRYABLE_CH_ERROR_NAMES = (
    "NETWORK_ERROR",  # Code: 210 — generic network failure
    "SOCKET_TIMEOUT",  # Code: 209
    "CANNOT_READ_FROM_SOCKET",  # Code: 210 family
    "ATTEMPT_TO_READ_AFTER_EOF",  # Code: 32 — truncated upstream response
    "RECEIVED_ERROR_FROM_REMOTE_IO_SERVER",  # Code: 86
    "RECEIVED_ERROR_TOO_MANY_REQUESTS",  # 429-class
    "POCO_EXCEPTION",  # Generic Poco/HTTP error wrapper
)


def _is_retryable_url_error(exc: BaseException) -> bool:
    """Return True if ``exc`` represents a transient upstream failure.

    Retry on:
        * HTTPError 429 / 500 / 502 / 503 / 504
        * Connection-level errors (resets, refused, timeouts, DNS hiccups)
        * Driver exceptions whose message names a transient HTTP status or
          ClickHouse network error code

    Don't retry on:
        * 4xx other than 429
        * SSL/TLS errors
        * Anything we don't recognize as transient — fail fast
    """
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in _RETRYABLE_HTTP_CODES

    if isinstance(exc, urllib.error.URLError):
        return _is_retryable_url_error(exc.reason) if isinstance(exc.reason, BaseException) else True

    if isinstance(exc, (TimeoutError, ConnectionError, socket.timeout, socket.gaierror)):
        return True

    msg = str(exc)
    if _HTTP_STATUS_RE.search(msg) and (
        "Bad Gateway" in msg
        or "Service Unavailable" in msg
        or "Gateway Timeout" in msg
        or "Too Many Requests" in msg
        or "Internal Server Error" in msg
    ):
        return True
    if any(name in msg for name in _RETRYABLE_CH_ERROR_NAMES):
        return True

    return False


async def with_url_retry(
    fn: Callable[[], Awaitable[T]],
    *,
    retries: int = DEFAULT_RETRIES,
    backoff_factor: float = DEFAULT_BACKOFF_FACTOR,
) -> T:
    """Run ``fn()`` with retry on transient upstream failures.

    Args:
        fn: Zero-arg coroutine factory — called fresh on each attempt so
            URL fetches are reissued cleanly.
        retries: Total attempts. ``1`` disables retry (single attempt).
            Must be >= 1.
        backoff_factor: Base for the exponential backoff. Sleep before
            attempt ``n`` (1-indexed) is ``backoff_factor ** (n - 1)``
            seconds. With the default ``2.0`` and ``retries=4`` this gives
            sleeps of 2, 4, 8 seconds.

    Returns:
        Whatever ``fn()`` returns on the first successful attempt.

    Raises:
        ValueError: If ``retries < 1`` or ``backoff_factor < 0``.
        Exception: The last exception raised by ``fn()`` if all attempts
            fail or the first non-retryable exception encountered.
    """
    if retries < 1:
        raise ValueError(f"retries must be >= 1, got {retries}")
    if backoff_factor < 0:
        raise ValueError(f"backoff_factor must be >= 0, got {backoff_factor}")

    for attempt in range(1, retries + 1):
        try:
            return await fn()
        except Exception as exc:
            if attempt == retries or not _is_retryable_url_error(exc):
                raise
            await asyncio.sleep(backoff_factor ** attempt)

    raise AssertionError("unreachable: with_url_retry exited loop without returning or raising")
