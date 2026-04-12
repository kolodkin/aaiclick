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
    touched_result = result_table in strategy
    touched_source = any(table in strategy for _, table in sources)
    if not touched_result and not touched_source:
        return {}, []

    try:
        if not sources:
            return await _apply_nullary(ch_client, result_table, strategy)
        if len(sources) == 1:
            return await _apply_unary(ch_client, result_table, sources[0], strategy)
        return await _apply_nary(ch_client, result_table, sources, strategy)
    except Exception:
        logger.error("Failed to apply strategy for %s", result_table, exc_info=True)
        return {}, []


async def _apply_nullary(
    ch_client: object,
    result_table: str,
    strategy: SamplingStrategy,
) -> tuple[dict[str, list[int]], list[int]]:
    """Operation with no kwarg sources (e.g. create_from_value).

    Only ``result_table`` can drive the match.
    """
    clause = strategy.get(result_table)
    if not clause:
        return {}, []
    result_ids = await _select_ids(ch_client, result_table, clause)
    return {}, result_ids


async def _apply_unary(
    ch_client: object,
    result_table: str,
    source: tuple[str, str],
    strategy: SamplingStrategy,
) -> tuple[dict[str, list[int]], list[int]]:
    """Operation with one source kwarg.

    Positional alignment: i-th source row ↔ i-th result row.
    """
    role, source_table = source
    source_clause = strategy.get(source_table)
    result_clause = strategy.get(result_table)

    # Prefer the source clause when present — source targeting is the
    # common case ("these are the rows I care about at the input").
    if source_clause:
        driver_table, driver_clause = source_table, source_clause
    elif result_clause:
        driver_table, driver_clause = result_table, result_clause
    else:
        return {}, []

    rows = await ch_client.query(f"""
        SELECT s.aai_id, r.aai_id
        FROM (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
              FROM {source_table}) s
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn
                    FROM {result_table}) r
        ON s.rn = r.rn
        WHERE {'s' if driver_table == source_table else 'r'}.aai_id IN (
            SELECT aai_id FROM {driver_table} WHERE {driver_clause}
        )
    """)
    source_ids = [row[0] for row in rows.result_rows]
    result_ids = [row[1] for row in rows.result_rows]
    return {role: source_ids}, result_ids


async def _apply_nary(
    ch_client: object,
    result_table: str,
    sources: list[tuple[str, str]],
    strategy: SamplingStrategy,
) -> tuple[dict[str, list[int]], list[int]]:
    """Operation with two or more source kwargs (e.g. concat, binary ops).

    Positional join across all sources plus the result table.
    """
    # Pick the first driver clause that matches — caller's strategy
    # determines priority by insertion order.
    driver: tuple[str, str] | None = None  # (table, clause)
    for _, src_table in sources:
        clause = strategy.get(src_table)
        if clause:
            driver = (src_table, clause)
            break
    if driver is None and result_table in strategy:
        driver = (result_table, strategy[result_table])
    if driver is None:
        return {}, []

    driver_table, driver_clause = driver

    # Build a positional join across every source and the result.
    joins = []
    selects = []
    for i, (_, src_table) in enumerate(sources):
        alias = f"s{i}"
        selects.append(f"{alias}.aai_id")
        if i == 0:
            joins.append(
                f"(SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn "
                f"FROM {src_table}) {alias}"
            )
        else:
            joins.append(
                f"INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn "
                f"FROM {src_table}) {alias} ON {alias}.rn = s0.rn"
            )
    selects.append("r.aai_id")
    joins.append(
        f"INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn "
        f"FROM {result_table}) r ON r.rn = s0.rn"
    )

    # Find which alias corresponds to the driver table (or use r for result).
    driver_alias = "r" if driver_table == result_table else next(
        f"s{i}" for i, (_, src) in enumerate(sources) if src == driver_table
    )

    sql = (
        f"SELECT {', '.join(selects)} FROM " + " ".join(joins) +
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
