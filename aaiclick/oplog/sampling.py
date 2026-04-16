"""
aaiclick.oplog.sampling - Strategy-driven oplog sampling.

A ``SamplingStrategy`` is a ``dict[str, str]`` mapping table names to WHERE
clauses. When a recorded operation produces a result in a table matched by
the strategy — or consumes a source matched by the strategy — the matched
``aai_id``s are looked up and persisted alongside the oplog row so that
downstream lineage tracing can walk the exact rows the user cares about.

Everything is best-effort: a failed lookup logs and returns empty arrays.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from aaiclick.data.data_context.ch_client import ChClient

logger = logging.getLogger(__name__)


SamplingStrategy = dict[str, str]
"""Maps ClickHouse table name → raw WHERE clause.

Keys reference fully-qualified table names as they appear in the oplog
(``kwargs`` values and ``result_table``). Explicit clauses are inlined
into the driving query; inherited clauses use ``{name:Type}`` parameter
binding so the typed id arrays travel as binary payload.
"""


class _Driver(NamedTuple):
    """A resolved sampling driver: which source table limits which rows.

    ``clause`` is a raw SQL WHERE clause evaluated against ``table``.
    ``parameters`` carries any ``{name:Type}`` placeholder bindings the
    clause references, or ``None`` for a plain user-supplied WHERE.
    """

    table: str
    clause: str
    parameters: dict[str, Any] | None


async def apply_strategy(
    ch_client: ChClient,
    result_table: str,
    kwargs: dict[str, str],
    strategy: SamplingStrategy,
    *,
    job_id: int | None = None,
) -> tuple[dict[str, list[int]], list[int]]:
    """Look up the aai_ids that match ``strategy`` for one operation.

    Returns ``(kwargs_aai_ids, result_aai_ids)`` — both empty when
    neither the explicit strategy nor any inherited match from an
    upstream op touches this operation.

    Matching runs in two stages:

    1. **Explicit** — a strategy key equal to one of this op's source
       tables (or the result table) drives the sampling via a WHERE
       clause. This is the Phase 0 behavior.
    2. **Inherited** — when no explicit key matches, we check the
       oplog for prior rows in the same ``job_id`` that populated
       ``result_aai_ids`` for any of this op's source tables. Those
       aai_ids become the implicit driver set, propagating the
       strategy forward through count-preserving ops (so the
       downstream oplog carries row-level lineage even for
       operations that don't themselves mention a strategy key).

    Sources and results align positionally via
    ``row_number() OVER (ORDER BY aai_id)`` so that the i-th source row
    maps to the i-th result row.
    """
    if not strategy:
        return {}, []

    sources = [(role, table) for role, table in kwargs.items() if table]

    driver = _pick_driver(result_table, sources, strategy)
    if driver is None and sources and job_id is not None:
        driver = await _pick_inherited_driver(ch_client, sources, job_id)

    if driver is None:
        return {}, []

    try:
        if not sources:
            rows = await _select_ids(ch_client, result_table, driver)
            return {}, rows
        return await _apply_positional(ch_client, result_table, sources, driver)
    except Exception:
        logger.error("Failed to apply strategy for %s", result_table, exc_info=True)
        return {}, []


def _pick_driver(
    result_table: str,
    sources: list[tuple[str, str]],
    strategy: SamplingStrategy,
) -> _Driver | None:
    """Return a driver for the first strategy-matched table.

    Source-side matches win — source targeting is the common case
    ("these are the rows I care about at the input"). Insertion order in
    ``sources`` decides which source clause wins when multiple match.
    Falls back to the result-table clause, then returns ``None``.
    """
    for _, src_table in sources:
        clause = strategy.get(src_table)
        if clause:
            return _Driver(table=src_table, clause=clause, parameters=None)
    result_clause = strategy.get(result_table)
    if result_clause:
        return _Driver(table=result_table, clause=result_clause, parameters=None)
    return None


async def _pick_inherited_driver(
    ch_client: ChClient,
    sources: list[tuple[str, str]],
    job_id: int,
) -> _Driver | None:
    """Look for a source table whose earlier oplog row in ``job_id`` has a
    populated ``result_aai_ids`` array, and return a driver whose clause
    binds those ids as a typed ``Array(UInt64)`` parameter.

    The first source that produced matched rows upstream wins, mirroring
    ``_pick_driver``'s source-first ordering. Returns ``None`` when no
    source has upstream matches.

    One batched query fetches the most recent populated oplog row per
    source table (``argMax`` over ``created_at``), then Python walks
    ``sources`` in order to pick the first match — so source priority
    is preserved with a single round-trip regardless of how many input
    roles the op has.
    """
    if not sources:
        return None
    src_tables = [table for _, table in sources]
    try:
        rows = await ch_client.query(
            "SELECT result_table, argMax(result_aai_ids, created_at) AS ids "
            "FROM operation_log "
            "WHERE job_id = {job_id:UInt64} "
            "  AND result_table IN {src_tables:Array(String)} "
            "  AND length(result_aai_ids) > 0 "
            "GROUP BY result_table",
            parameters={"job_id": job_id, "src_tables": src_tables},
        )
    except Exception:
        logger.error(
            "Failed to look up inherited matches for %s", src_tables,
            exc_info=True,
        )
        return None

    ids_by_table: dict[str, list[int]] = {
        row[0]: list(row[1]) for row in rows.result_rows
    }
    for _, src_table in sources:
        inherited_ids = ids_by_table.get(src_table)
        if inherited_ids:
            return _Driver(
                table=src_table,
                clause="aai_id IN {inherited_ids:Array(UInt64)}",
                parameters={"inherited_ids": inherited_ids},
            )
    return None


async def _apply_positional(
    ch_client: ChClient,
    result_table: str,
    sources: list[tuple[str, str]],
    driver: _Driver,
) -> tuple[dict[str, list[int]], list[int]]:
    """Positional join across all sources and the result table.

    Emits one query of the form::

        SELECT s0.aai_id, ..., r.aai_id
        FROM (row-numbered sources) sN
        INNER JOIN (row-numbered result) r ON r.rn = s0.rn
        WHERE <driver_alias>.aai_id IN (SELECT aai_id FROM <driver> WHERE <clause>)

    Handles 1..N sources uniformly. The driver's clause may reference
    ``{name:Type}`` placeholders bound via ``driver.parameters``.
    """
    parts: list[str] = []
    selects: list[str] = []
    driver_alias: str | None = None
    for i, (_, src_table) in enumerate(sources):
        alias = f"s{i}"
        selects.append(f"{alias}.aai_id")
        subquery = (
            f"(SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn "
            f"FROM {src_table}) {alias}"
        )
        parts.append(subquery if i == 0 else f"INNER JOIN {subquery} ON {alias}.rn = s0.rn")
        if src_table == driver.table and driver_alias is None:
            driver_alias = alias

    selects.append("r.aai_id")
    parts.append(
        f"INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn "
        f"FROM {result_table}) r ON r.rn = s0.rn"
    )
    if driver.table == result_table:
        driver_alias = "r"

    sql = (
        f"SELECT {', '.join(selects)} FROM " + " ".join(parts) +
        f" WHERE {driver_alias}.aai_id IN ("
        f"SELECT aai_id FROM {driver.table} WHERE {driver.clause})"
    )
    rows = await ch_client.query(sql, parameters=driver.parameters)

    kwargs_aai_ids: dict[str, list[int]] = {role: [] for role, _ in sources}
    result_ids: list[int] = []
    for row in rows.result_rows:
        for i, (role, _) in enumerate(sources):
            kwargs_aai_ids[role].append(row[i])
        result_ids.append(row[len(sources)])
    return kwargs_aai_ids, result_ids


async def _select_ids(ch_client: ChClient, table: str, driver: _Driver) -> list[int]:
    """Return all ``aai_id``s from ``table`` that match ``driver.clause``."""
    rows = await ch_client.query(
        f"SELECT aai_id FROM {table} WHERE {driver.clause}",
        parameters=driver.parameters,
    )
    return [row[0] for row in rows.result_rows]
