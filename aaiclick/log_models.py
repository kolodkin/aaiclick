"""Shared leaf primitives used on both sides of the view_models/orchestration boundary.

``SnowflakeId`` and the log vocabulary (streams, levels, ``LogLine``) are
needed by ``aaiclick.view_models`` (the CLI/REST/MCP contract) and by
``aaiclick.orchestration`` internals alike. They live here — a leaf module
with no aaiclick imports beyond ``datetime_utils`` — so imports stay
one-directional and either package can initialize first.
``aaiclick.view_models`` re-exports them for the contract surface.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, PlainSerializer

from .datetime_utils import utc_now

# Snowflake ids are 64-bit, exceeding JavaScript's safe-integer range (2^53-1),
# so a JSON number would silently lose precision in the browser. Serialize id
# fields as strings on the wire (the OpenAPI schema follows, so generated SPA
# types are honest). ``when_used="json"`` scopes this to JSON output only: the
# Python attribute and ``model_dump()`` stay ``int``, so the CLI and internal
# logic are unaffected, and request bodies still coerce a numeric string back
# to ``int`` on validation.
SnowflakeId = Annotated[int, PlainSerializer(lambda v: str(v), return_type=str, when_used="json")]

# Captured task output streams.
STDOUT_STREAM = "stdout"
STDERR_STREAM = "stderr"
LogStream = Literal["stdout", "stderr"]

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# highest-first so the first threshold a level clears wins; each string appears
# once mapped to its logging constant and type-checks as LogLevel. logging owns
# the level names/numbers, so we do not re-declare LEVEL_* string constants.
_LEVEL_THRESHOLDS: tuple[tuple[int, LogLevel], ...] = (
    (logging.CRITICAL, "CRITICAL"),
    (logging.ERROR, "ERROR"),
    (logging.WARNING, "WARNING"),
    (logging.INFO, "INFO"),
    (logging.DEBUG, "DEBUG"),
)


def normalize_level(levelno: int) -> LogLevel:
    """Bucket any logging level number to the nearest standard LogLevel name."""
    for threshold, name in _LEVEL_THRESHOLDS:
        if levelno >= threshold:
            return name
    return "DEBUG"  # below DEBUG (incl. NOTSET)


class LogLine(BaseModel):
    """One captured output line tagged with its stream, level, and emit time."""

    stream: LogStream
    level: LogLevel = "INFO"
    text: str
    created_at: datetime = Field(default_factory=utc_now)
