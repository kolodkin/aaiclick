"""
aaiclick.data.object._url_retry - Retry helper for URL-fetching operations.

``create_object_from_url`` and ``Object.insert_from_url`` issue
``INSERT … FROM url(…)`` queries that are routinely killed by transient
upstream blips (502/503/504, socket resets, DNS hiccups) on public datasets
like Wikidata SPARQL or HuggingFace CDN. This module provides a thin retry
wrapper around those calls with exponential backoff for a bounded set of
retryable errors.

Both backends fetch over HTTP inside the engine, so an upstream failure
arrives as a driver exception whose message text carries the original status
code or ClickHouse error name (``HTTPException ... HTTP status code: 503``,
``RECEIVED_ERROR_FROM_REMOTE_IO_SERVER``). The predicate also recognizes the
Python-level socket and urllib error shapes, so a caller fetching in Python
keeps the same retry semantics.
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

# Engine path: the engine wraps an upstream HTTP failure into a driver
# exception whose ``str()`` includes either the upstream HTTP status line or a
# ClickHouse error code/name. We pattern-match both because the message gets
# rephrased ("Received error from remote server", "HTTP/1.1 502 Bad Gateway",
# "Code: 86. RECEIVED_ERROR_FROM_REMOTE_IO_SERVER", etc.).
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

    The predicate dispatches by exception shape:

    * Both engines fetch server-side and raise driver exceptions whose
      ``str()`` carries the upstream status code and CH error name — branch 4
      below (``_HTTP_STATUS_RE`` + ``_RETRYABLE_CH_ERROR_NAMES``).
    * Branches 1–3 cover Python-level fetch failures (``HTTPError`` /
      ``URLError`` / ``TimeoutError`` / ``ConnectionError`` / ``socket.*``).

    Don't retry on:
        * 4xx other than 429
        * SSL/TLS errors
        * Anything we don't recognize as transient — fail fast
    """
    # ── Python-level fetch ───────────────────────────────────────────────
    # urllib.urlopen raises HTTPError on non-2xx responses (4xx/5xx).
    if isinstance(exc, urllib.error.HTTPError):
        return exc.code in _RETRYABLE_HTTP_CODES

    # urllib also raises URLError when the request never gets a response
    # (DNS failure, connection refused, TLS handshake failure, etc.); the
    # underlying cause is in .reason.
    if isinstance(exc, urllib.error.URLError):
        return _is_retryable_url_error(exc.reason) if isinstance(exc.reason, BaseException) else True

    # Bare socket / connection errors that bubble up from urllib (or from
    # any future direct-Python fetch).
    if isinstance(exc, (TimeoutError, ConnectionError, socket.timeout, socket.gaierror)):
        return True

    # ── Engine-side fetch ───────────────────────────────────────────────
    # The engine made the HTTP call; we get its driver exception with the
    # upstream failure encoded in the message text.
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
            # Python 3.14 backs HTTPError's response body with a tempfile;
            # close it explicitly so gc cleanup never fires ResourceWarning
            # (which ``filterwarnings=["error"]`` would escalate).
            if isinstance(exc, urllib.error.HTTPError):
                exc.close()
            if attempt == retries or not _is_retryable_url_error(exc):
                raise
            await asyncio.sleep(backoff_factor**attempt)

    raise AssertionError("unreachable: with_url_retry exited loop without returning or raising")
