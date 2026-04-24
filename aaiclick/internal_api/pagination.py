"""Shared list+count pagination for ``internal_api.list_*`` functions.

Every ``list_*`` runs the same dance: build a ``COUNT(*)`` query and a
``SELECT`` that share the same ``WHERE`` clauses, then execute both
inside a single SQL session. ``paginate()`` factors that out so each
call site only has to assemble the predicates and ORDER BY.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic, TypeVar

from sqlalchemy.sql.elements import ColumnElement
from sqlmodel import func, select

from aaiclick.orchestration.orch_context import get_sql_session

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class PageRows(Generic[T]):
    """Pair returned by :func:`paginate` — explicit so callers don't unpack positionally."""

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
