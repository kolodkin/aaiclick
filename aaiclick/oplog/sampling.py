"""
aaiclick.oplog.sampling - Lineage sampling queries for oplog.

Picks N aai_ids from source tables (preferring those already in oplog)
and finds corresponding result aai_ids via positional alignment.
"""

from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)


def _sample_size() -> int:
    return int(os.environ.get("AAICLICK_OPLOG_SAMPLE_SIZE", "10"))


async def sample_lineage(
    ch_client: object,
    result_table: str,
    kwargs: dict[str, str],
    n: int | None = None,
) -> tuple[dict[str, list[int]], list[int]]:
    """Sample lineage aai_ids for an operation.

    Returns (kwargs_aai_ids, result_aai_ids).
    """
    if n is None:
        n = _sample_size()
    sources = list(kwargs.values())
    roles = list(kwargs.keys())
    if len(sources) == 1:
        return await _sample_unary(ch_client, result_table, roles[0], sources[0], n)
    elif len(sources) == 2:
        return await _sample_binary(
            ch_client, result_table, roles[0], sources[0], roles[1], sources[1], n,
        )
    else:
        return await _sample_nary(ch_client, result_table, roles, sources, n)


async def _sample_unary(ch_client, result_table, role, source_table, n):
    source_ids = await _pick_aai_ids(ch_client, source_table, n)
    if not source_ids:
        return {}, []
    ids_list = ", ".join(str(i) for i in source_ids)
    result = await ch_client.query(f"""
        SELECT s.aai_id, r.aai_id
        FROM (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {source_table}) s
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {result_table}) r
        ON s.rn = r.rn WHERE s.aai_id IN ({ids_list})
    """)
    return {role: [r[0] for r in result.result_rows]}, [r[1] for r in result.result_rows]


async def _sample_binary(ch_client, result_table, role_a, table_a, role_b, table_b, n):
    a_ids = await _pick_aai_ids(ch_client, table_a, n)
    if not a_ids:
        return {}, []
    ids_list = ", ".join(str(i) for i in a_ids)
    result = await ch_client.query(f"""
        SELECT a.aai_id, b.aai_id, r.aai_id
        FROM (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {table_a}) a
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {table_b}) b ON a.rn = b.rn
        INNER JOIN (SELECT aai_id, row_number() OVER (ORDER BY aai_id) AS rn FROM {result_table}) r ON a.rn = r.rn
        WHERE a.aai_id IN ({ids_list})
    """)
    rows = result.result_rows
    return {role_a: [r[0] for r in rows], role_b: [r[1] for r in rows]}, [r[2] for r in rows]


async def _sample_nary(ch_client, result_table, roles, source_tables, n):
    result = await ch_client.query(
        f"SELECT aai_id FROM {result_table} ORDER BY rand() LIMIT {n}"
    )
    result_ids = [r[0] for r in result.result_rows]
    if not result_ids:
        return {}, []
    kwargs_aai_ids = {}
    for role, src in zip(roles, source_tables):
        ids = await _pick_aai_ids(ch_client, src, n)
        if ids:
            kwargs_aai_ids[role] = ids
    return kwargs_aai_ids, result_ids


async def _pick_aai_ids(ch_client, table, n):
    """Pick N aai_ids from a table, preferring those already in oplog lineage."""
    try:
        result = await ch_client.query(f"""
            SELECT aai_id FROM {table}
            WHERE aai_id IN (
                SELECT arrayJoin(result_aai_ids) FROM operation_log
                WHERE result_table = '{table}'
            ) LIMIT {n}
        """)
        known = [r[0] for r in result.result_rows]
    except Exception:
        logger.error("Failed to query lineage aai_ids for %s", table, exc_info=True)
        known = []
    if len(known) >= n:
        return known[:n]
    remaining = n - len(known)
    exclude = ", ".join(str(i) for i in known) if known else "0"
    try:
        result = await ch_client.query(f"""
            SELECT aai_id FROM {table}
            WHERE aai_id NOT IN ({exclude})
            ORDER BY rand() LIMIT {remaining}
        """)
        return known + [r[0] for r in result.result_rows]
    except Exception:
        logger.error("Failed to query random aai_ids for %s", table, exc_info=True)
        return known
