"""Internal datetime helpers.

Storage convention is naive UTC: SQLAlchemy ``DateTime()`` columns hold
timezone-naive datetimes whose values are always UTC. ``utc_now()`` is a
drop-in replacement for the deprecated ``datetime.utcnow()``, and
``utc_field()`` declares a table model's datetime field.
"""

import sqlite3
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

from pydantic_core import PydanticUndefined, PydanticUndefinedType
from sqlalchemy import DateTime
from sqlmodel import Field

# Python 3.12 deprecated sqlite3's default datetime/date adapters. SQLAlchemy
# ``text()`` with bound datetime params falls through to the DBAPI adapter, so
# we register adapters explicitly to avoid the DeprecationWarning (which
# ``filterwarnings=["error"]`` would otherwise escalate to a failure).
#
# Format must match SQLAlchemy's SQLite DateTime column adapter — space
# separator, not ``T`` — otherwise lexicographic ``WHERE retry_after <= :now``
# comparisons across raw text() params and ORM-stored columns break.
sqlite3.register_adapter(datetime, str)
sqlite3.register_adapter(date, str)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_naive_utc(value: datetime) -> datetime:
    """Normalize an input datetime to the storage convention.

    An aware value is converted to UTC and stripped; a naive one is taken as
    UTC already. Comparing an aware value against ``utc_now()`` or a stored
    column raises ``TypeError``, so every datetime that enters from the wire
    passes through here first.
    """
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def utc_field(
    *,
    default: datetime | None | PydanticUndefinedType = PydanticUndefined,
    default_factory: Callable[[], datetime] | None = None,
    index: bool = False,
) -> Any:  # sqlmodel.Field's own return type, so any annotation accepts it
    """``Field`` for a naive-UTC datetime column on a table model.

    sqlmodel >= 0.0.45 maps a bare ``datetime`` field to a tz-aware type that
    rejects naive values; this pins the column to naive ``DateTime()``.
    """
    return Field(default=default, default_factory=default_factory, index=index, sa_type=DateTime())
