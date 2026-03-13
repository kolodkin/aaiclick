"""
aaiclick.backend - Backend selection for local vs distributed mode.

Controls whether aaiclick uses local embedded engines (chdb + SQLite)
or distributed servers (ClickHouse + PostgreSQL).

Set via AAICLICK_BACKEND env var:
  - "local" (default): chdb + SQLite, zero infrastructure
  - "distributed": ClickHouse server + PostgreSQL server
"""

import os


def get_backend() -> str:
    """Return the active backend name ('local' or 'distributed')."""
    backend = os.getenv("AAICLICK_BACKEND", "local").lower()
    if backend not in ("local", "distributed"):
        raise ValueError(
            f"Invalid AAICLICK_BACKEND='{backend}'. "
            f"Must be 'local' or 'distributed'."
        )
    return backend


def is_local() -> bool:
    """True when using embedded local engines (chdb + SQLite)."""
    return get_backend() == "local"
