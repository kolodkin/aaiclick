"""
aaiclick.oplog.cleanup - Table cleanup helpers.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from aaiclick.data.data_context.ch_client import ChClient

logger = logging.getLogger(__name__)


class TableOwner(NamedTuple):
    """Ownership metadata for a table, copied from table_registry."""

    job_id: int | None = None
    task_id: int | None = None
    run_id: int | None = None


async def lineage_aware_drop(
    ch_client: ChClient,
    table_name: str,
    owner: TableOwner | None = None,
) -> None:
    """Drop a table.

    Best effort — exceptions are logged but do not propagate.

    Args:
        ch_client: Async ClickHouse client.
        table_name: Table to drop.
        owner: Ownership metadata (reserved for future use).
    """
    try:
        await ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
    except Exception:
        logger.debug("Failed to drop %s", table_name, exc_info=True)
