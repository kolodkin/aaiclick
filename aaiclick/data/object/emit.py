"""Shared result-table emission for database-level operations.

Every operator/aggregation/copy helper ends the same way: create the result
table from a schema, run ``INSERT INTO … <select>`` capturing
:class:`QueryStats`, optionally record an oplog sample. That sequence lives
here so the emitters in ``operators.py`` / ``ingest.py`` / ``join.py``
supply only their schema and SELECT.
"""

from __future__ import annotations

from aaiclick.oplog.oplog_api import oplog_record_sample

from ..data_context import create_object
from ..data_context.ch_client import execute_for_stats
from ..models import Schema
from ..scope import NamedScope


async def emit_result(
    schema: Schema,
    select_sql: str,
    ch_client,
    *,
    insert_cols: str | None = None,
    name: str | None = None,
    scope: NamedScope | None = None,
    oplog_op: str | None = None,
    oplog_kwargs: dict | None = None,
):
    """Create the result table for ``schema`` and fill it from ``select_sql``.

    Args:
        schema: Result table schema (also defines the default insert columns).
        select_sql: Complete ``SELECT …`` statement producing the rows.
        ch_client: ClickHouse client instance.
        insert_cols: Explicit insert column list; defaults to ``schema.columns``.
        name: Optional result table name (forwarded to ``create_object``).
        scope: Optional result table scope (forwarded to ``create_object``).
        oplog_op: When set, record an oplog sample for the result under this op.
        oplog_kwargs: kwargs for the oplog sample.

    Returns:
        The new Object, with ``_stats`` populated from the INSERT.
    """
    result = await create_object(schema, name=name, scope=scope)
    cols = insert_cols if insert_cols is not None else ", ".join(schema.columns)
    result._stats = await execute_for_stats(f"INSERT INTO {result.table} ({cols}) {select_sql}", client=ch_client)
    if oplog_op is not None:
        oplog_record_sample(result.table, oplog_op, kwargs=oplog_kwargs or {})
    return result
