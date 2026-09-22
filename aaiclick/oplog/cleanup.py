"""
aaiclick.oplog.cleanup - Table cleanup helpers.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
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
    """Drop a table. Exceptions propagate.

    Args:
        ch_client: Async ClickHouse client.
        table_name: Table to drop.
        owner: Ownership metadata (reserved for future use).
    """
    await ch_client.command(f"DROP TABLE IF EXISTS {table_name}")


async def drop_tables(ch_client: ChClient, owners: Mapping[str, TableOwner | None]) -> list[str]:
    """Best-effort drop of several tables; returns the names actually dropped.

    A sweep deletes registry and ref rows only for the returned names, so a
    table whose DROP failed is seen again on the next pass instead of
    leaking. Each failure is logged at warning level.
    """
    dropped: list[str] = []
    for table_name, owner in owners.items():
        try:
            await lineage_aware_drop(ch_client, table_name, owner=owner)
        except Exception:
            logger.warning("Failed to drop CH table %s", table_name, exc_info=True)
            continue
        dropped.append(table_name)
    return dropped
