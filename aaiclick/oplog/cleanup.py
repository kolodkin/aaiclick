"""
aaiclick.oplog.cleanup - Lineage-aware table cleanup.

When dropping a table, preserves rows whose aai_ids appear in oplog lineage
(kwargs_aai_ids or result_aai_ids). Falls back to a random sample if no
lineage references exist.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

DEFAULT_FALLBACK_SAMPLE = 10


async def lineage_aware_drop(ch_client: object, table_name: str) -> None:
    """Replace a table with its lineage-referenced rows, then drop the original.

    1. Query operation_log for aai_ids referenced by this table
    2. If found, create a sample table with those rows
    3. If not found, fall back to LIMIT 10 random sample
    4. Drop the original table

    Best effort — exceptions are logged but do not propagate.
    """
    try:
        referenced_ids = await _get_lineage_aai_ids(ch_client, table_name)

        if referenced_ids:
            ids_list = ", ".join(str(i) for i in referenced_ids)
            await ch_client.command(
                f"CREATE TABLE IF NOT EXISTS {table_name}_sample "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT * FROM {table_name} WHERE aai_id IN ({ids_list})"
            )
        else:
            await ch_client.command(
                f"CREATE TABLE IF NOT EXISTS {table_name}_sample "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT * FROM {table_name} LIMIT {DEFAULT_FALLBACK_SAMPLE}"
            )
    except Exception:
        logger.debug("Failed to create sample for %s", table_name, exc_info=True)

    try:
        await ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        logger.debug("Failed to drop %s", table_name, exc_info=True)


async def _get_lineage_aai_ids(ch_client: object, table_name: str) -> list[int]:
    """Get all aai_ids from a table that are referenced in oplog lineage."""
    table_escaped = table_name.replace("'", "\\'")

    # Collect aai_ids that appear as result_aai_ids for this table
    # or as kwargs_aai_ids values where this table is a source
    result = await ch_client.query(f"""
        SELECT DISTINCT id FROM (
            SELECT arrayJoin(result_aai_ids) AS id
            FROM operation_log
            WHERE result_table = '{table_escaped}'
              AND length(result_aai_ids) > 0
            UNION ALL
            SELECT arrayJoin(arrayJoin(mapValues(kwargs_aai_ids))) AS id
            FROM operation_log
            WHERE hasAny(mapValues(kwargs), ['{table_escaped}'])
              AND length(mapKeys(kwargs_aai_ids)) > 0
        )
    """)
    return [row[0] for row in result.result_rows]
