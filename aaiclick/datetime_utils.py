"""Internal datetime helpers.

Storage convention is naive UTC: SQLAlchemy ``DateTime()`` columns hold
timezone-naive datetimes whose values are always UTC. ``utc_now()`` is a
drop-in replacement for the deprecated ``datetime.utcnow()``.
"""

import sqlite3
from datetime import date, datetime, timezone

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
