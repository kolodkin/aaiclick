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

logger = logging.getLogger(__name__)


SamplingStrategy = dict[str, str]
"""Maps ClickHouse table name → raw WHERE clause.

Values are inlined into ``SELECT aai_id FROM <table> WHERE <clause>`` and
evaluated by ClickHouse. Keys reference fully-qualified table names as they
appear in the oplog (``kwargs`` values and ``result_table``).
"""


async def apply_strategy(
    ch_client: object,
    result_table: str,
    kwargs: dict[str, str],
    strategy: SamplingStrategy,
) -> tuple[dict[str, list[int]], list[int]]:
    """Look up the aai_ids that match ``strategy`` for one operation.

    Returns ``(kwargs_aai_ids, result_aai_ids)`` — both empty when the
    strategy does not touch this operation's tables. Sources and results
    are aligned positionally via ``row_number() OVER (ORDER BY aai_id)``
    so that the i-th source row maps to the i-th result row.

    Args:
        ch_client: Async ClickHouse client.
        result_table: Table the operation wrote to.
        kwargs: ``{role: source_table}`` map from the oplog payload.
        strategy: Active sampling strategy.

    Returns:
        ``(kwargs_aai_ids, result_aai_ids)`` — ``kwargs_aai_ids`` is keyed
        by the same roles as ``kwargs``. Empty arrays mean "nothing to
        track at this step".
    """
    if not strategy:
        return {}, []

    sources = [(role, table) for role, table in kwargs.items() if table]
    driver = _pick_driver(result_table, sources, strategy)
    if driver is None:
        return {}, []

    try:
        if not sources:
            rows = await _select_ids(ch_client, result_table, driver[1])
            return {}, rows
        return await _apply_positional(
            ch_client, result_table, sources, driver,
        )
    except Exception:
        logger.error("Failed to apply strategy for %s", result_table, exc_info=True)
        return {}, []


def _pick_driver(
    result_table: str,
    sources: list[tuple[str, str]],
    strategy: SamplingStrategy,
) -> tuple[str, str] | None:
    """Return ``(table, clause)`` for the first matched table.

    Source-side matches win — source targeting is the common case
    ("these are the rows I care about at the input"). Insertion order in
    ``sources`` decides which source clause wins when multiple match.
    Falls back to the result-table clause, then returns ``None``.
    """
    for _, src_table in sources:
        clause = strategy.get(src_table)
        if clause:
            return src_table, clause
    result_clause = strategy.get(result_table)
    if result_clause:
        return result_table, result_clause
    return None


async def _apply_positional(
    ch_client: object,
    result_table: str,
    sources: list[tuple[str, str]],
    driver: tuple[str, str],
) -> tuple[dict[str, list[int]], list[int]]:
    """Positional join across all sources and the result table.

    Emits one query of the form::

        SELECT s0.aai_id, ..., r.aai_id
        FROM (row-numbered sources) sN
        INNER JOIN (row-numbered result) r ON r.rn = s0.rn
        WHERE <driver_alias>.aai_id IN (SELECT aai_id FROM <driver> WHERE <clause>)

    Handles 1..N sources uniformly.
    """
    driver_table, driver_clause = driver

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
        if src_table == driver_table and driver_alias is None:
            driver_alias = alias

    selects.append("r.aai_id")
    parts.append(
        f"INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn "
        f"FROM {result_table}) r ON r.rn = s0.rn"
    )
    if driver_table == result_table:
        driver_alias = "r"

    sql = (
        f"SELECT {', '.join(selects)} FROM " + " ".join(parts) +
        f" WHERE {driver_alias}.aai_id IN ("
        f"SELECT aai_id FROM {driver_table} WHERE {driver_clause})"
    )
    rows = await ch_client.query(sql)

    kwargs_aai_ids: dict[str, list[int]] = {role: [] for role, _ in sources}
    result_ids: list[int] = []
    for row in rows.result_rows:
        for i, (role, _) in enumerate(sources):
            kwargs_aai_ids[role].append(row[i])
        result_ids.append(row[len(sources)])
    return kwargs_aai_ids, result_ids


async def _select_ids(ch_client: object, table: str, clause: str) -> list[int]:
    """Return all ``aai_id``s from ``table`` that match ``clause``."""
    rows = await ch_client.query(
        f"SELECT aai_id FROM {table} WHERE {clause}"
    )
    return [row[0] for row in rows.result_rows]
