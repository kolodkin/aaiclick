"""SQL text helpers shared across orchestration modules."""

from collections.abc import Iterator, Sequence
from typing import TypeVar

T = TypeVar("T")

# asyncpg and SQLite cap one statement at 32,767 bind parameters; stay well under.
IN_BATCH_SIZE = 10_000


def in_clause(ids: Sequence, prefix: str) -> tuple[str, dict]:
    """Build a parameterized IN clause compatible with both SQLite and PostgreSQL.

    Returns (placeholder_string, params_dict) e.g. (":p0, :p1", {"p0": 1, "p1": 2}).
    Callers with unbounded ``ids`` split them with ``batched`` first.
    """
    params = {f"{prefix}{i}": v for i, v in enumerate(ids)}
    placeholders = ", ".join(f":{k}" for k in params)
    return placeholders, params


def batched(items: Sequence[T], size: int = IN_BATCH_SIZE) -> Iterator[Sequence[T]]:
    """Yield ``items`` in slices of at most ``size``."""
    for start in range(0, len(items), size):
        yield items[start : start + size]
