"""Shared list+count pagination for ``internal_api.list_*`` functions.

Every ``list_*`` runs the same dance: build a ``COUNT(*)`` query and a
``SELECT`` that share the same ``WHERE`` clauses, then execute both
inside a single SQL session. ``paginate()`` factors that out so each
call site only has to assemble the predicates and ORDER BY.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Generic, NamedTuple, TypeVar

from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import col, func, or_, select

from aaiclick.orchestration.orch_context import get_sql_session

T = TypeVar("T")

_LIKE_ESCAPE = "\\"
_MAX_ID = 2**63 - 1


def name_like(column: str, pattern: str) -> ColumnElement[bool]:
    """``column LIKE pattern`` where only ``%`` is a wildcard; ``_`` matches itself."""
    escaped = pattern.replace(_LIKE_ESCAPE, _LIKE_ESCAPE * 2).replace("_", _LIKE_ESCAPE + "_")
    return col(column).like(escaped, escape=_LIKE_ESCAPE)


def name_or_id_like(name_column: str, id_column: int, pattern: str) -> ColumnElement[bool]:
    """:func:`name_like`, or the row whose id is ``pattern`` when that is one whole number.

    Lets one search box take a name or a pasted id. The id match is exact, so
    it stays on the primary key.
    """
    by_name = name_like(name_column, pattern)
    digits = pattern.strip("%")
    # The length check comes first: int() refuses a number of thousands of digits.
    if not digits.isascii() or not digits.isdigit() or len(digits) > len(str(_MAX_ID)) or int(digits) > _MAX_ID:
        return by_name
    return or_(by_name, col(id_column) == int(digits))


class PageRows(NamedTuple, Generic[T]):
    """``(total, rows)`` pair returned by :func:`paginate`."""

    total: int
    rows: list[T]


async def paginate(
    model: type[T],
    *,
    where: Sequence[ColumnElement[bool]] = (),
    order_by: ColumnElement,
    limit: int,
    offset: int,
) -> PageRows[T]:
    """Run ``COUNT(*)`` + paginated ``SELECT`` for ``model`` in one session.

    ``order_by`` must carry an explicit direction (``col(...).asc()`` or
    ``.desc()``) — pass `ColumnElement` shapes only.
    """
    count_query = select(func.count()).select_from(model)
    list_query = select(model)
    for predicate in where:
        count_query = count_query.where(predicate)
        list_query = list_query.where(predicate)
    list_query = list_query.order_by(order_by).limit(limit).offset(offset)

    async with get_sql_session() as session:
        total = (await session.execute(count_query)).scalar_one()
        rows = (await session.execute(list_query)).scalars().all()

    return PageRows(total=total, rows=list(rows))
