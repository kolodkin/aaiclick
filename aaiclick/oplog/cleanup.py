"""
aaiclick.oplog.cleanup - Table cleanup helpers.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from aaiclick.data.data_context.ch_client import ChClient

logger = logging.getLogger(__name__)

# A sweep gives up after this many failed drops in a row: an unreachable
# ClickHouse would otherwise cost one connect timeout per eligible table per
# poll. The untried tables stay registered and the next sweep picks them up.
MAX_CONSECUTIVE_DROP_FAILURES = 3


async def drop_tables(ch_client: ChClient, table_names: Iterable[str]) -> list[str]:
    """Best-effort ``DROP TABLE IF EXISTS`` over ``table_names``; returns the names dropped.

    Callers delete registry and ref rows only for the returned names, so a
    table whose DROP failed is seen again on the next pass instead of leaking.
    The first failure of a run is logged with its traceback, later ones at debug.
    """
    dropped: list[str] = []
    consecutive_failures = 0
    for table_name in table_names:
        try:
            await ch_client.command(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            consecutive_failures += 1
            level = logging.WARNING if consecutive_failures == 1 else logging.DEBUG
            logger.log(level, "Failed to drop CH table %s", table_name, exc_info=True)
            if consecutive_failures >= MAX_CONSECUTIVE_DROP_FAILURES:
                logger.warning("Giving up on this sweep after %d consecutive failed drops", consecutive_failures)
                break
            continue
        consecutive_failures = 0
        dropped.append(table_name)
    return dropped
