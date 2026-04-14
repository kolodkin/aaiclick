"""
aaiclick.oplog.cleanup - Lineage-aware table cleanup.

When dropping a table, preserves rows whose aai_ids appear in oplog lineage
(``kwargs_aai_ids`` or ``result_aai_ids``). Under ``PreservationMode.STRATEGY``
those arrays contain the strategy-matched rows the user asked to track;
under ``PreservationMode.NONE`` they stay empty and the table is dropped
without creating a sample.
"""

from __future__ import annotations

import logging
from typing import NamedTuple

from aaiclick.data.sql_utils import escape_sql_string

logger = logging.getLogger(__name__)


class TableOwner(NamedTuple):
    """Ownership metadata for a table, copied from table_registry."""

    job_id: int | None = None
    task_id: int | None = None
    run_id: int | None = None


async def lineage_aware_drop(
    ch_client: object,
    table_name: str,
    owner: TableOwner | None = None,
) -> None:
    """Replace a table with its lineage-referenced rows, then drop the original.

    1. Query operation_log for ``aai_id``s referenced by this table
    2. If any are found, create a sample table with just those rows and
       register it in ``table_registry`` so it gets cleaned up with the job
    3. Drop the original table

    When no lineage references exist (the common ``NONE`` mode), the table
    is dropped with no sample created — there is nothing to preserve.

    Best effort — exceptions are logged but do not propagate.

    Args:
        ch_client: Async ClickHouse client.
        table_name: Table to drop.
        owner: Ownership metadata (job_id, task_id, run_id) from the
            original table's registry entry. When set, the sample table
            is registered in table_registry so it gets cleaned up when
            the job expires.
    """
    sample_created = False
    sample_name = f"{table_name}_sample"
    try:
        referenced_ids = await _get_lineage_aai_ids(ch_client, table_name)

        if referenced_ids:
            ids_list = ", ".join(str(i) for i in referenced_ids)
            await ch_client.command(
                f"CREATE TABLE IF NOT EXISTS {sample_name} "
                f"ENGINE = MergeTree() ORDER BY tuple() "
                f"AS SELECT * FROM {table_name} WHERE aai_id IN ({ids_list})"
            )
            sample_created = True
    except Exception:
        logger.debug("Failed to create sample for %s", table_name, exc_info=True)

    if sample_created and owner is not None and owner.job_id is not None:
        sample_escaped = escape_sql_string(sample_name)
        job_val = str(owner.job_id)
        task_val = str(owner.task_id) if owner.task_id is not None else "NULL"
        run_val = str(owner.run_id) if owner.run_id is not None else "NULL"
        try:
            await ch_client.command(
                "INSERT INTO table_registry (table_name, job_id, task_id, run_id, created_at) "
                f"VALUES ('{sample_escaped}', {job_val}, {task_val}, {run_val}, now64(3))"
            )
        except Exception:
            logger.debug("Failed to register sample %s", sample_name, exc_info=True)

    try:
        await ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        logger.debug("Failed to drop %s", table_name, exc_info=True)


async def _get_lineage_aai_ids(ch_client: object, table_name: str) -> list[int]:
    """Get all aai_ids from a table that are referenced in oplog lineage."""
    table_escaped = escape_sql_string(table_name)

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
