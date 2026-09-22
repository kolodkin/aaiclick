"""
aaiclick.oplog.cleanup - Table cleanup helpers.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, NamedTuple

if TYPE_CHECKING:
    from aaiclick.data.data_context.ch_client import ChClient


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

    A failing DROP propagates so the caller can keep the table's registry
    row and retry on a later sweep.

    Args:
        ch_client: Async ClickHouse client.
        table_name: Table to drop.
        owner: Ownership metadata (reserved for future use).
    """
    await ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
