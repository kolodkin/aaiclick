"""``LazyChClient``: lazy connect, no reconnect after close, refusal."""

from __future__ import annotations

import pytest

from aaiclick.data.object_context.ch_client import LazyChClient, ch_refused, get_ch_client


async def test_close_before_first_use_blocks_reconnect():
    client = LazyChClient()
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        await client.query("SELECT 1")


async def test_ch_refused_raises_on_any_call():
    with ch_refused("no clickhouse here"):
        with pytest.raises(RuntimeError, match="no clickhouse here"):
            await get_ch_client().query("SELECT 1")
