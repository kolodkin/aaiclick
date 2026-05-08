"""Internal datetime helpers.

Storage convention is naive UTC: SQLAlchemy ``DateTime()`` columns hold
timezone-naive datetimes whose values are always UTC. ``utc_now()`` is a
drop-in replacement for the deprecated ``datetime.utcnow()``.
"""

from datetime import datetime, timezone


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)
