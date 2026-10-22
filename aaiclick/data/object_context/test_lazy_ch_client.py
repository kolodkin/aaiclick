"""``LazyChClient``: lazy connect, no reconnect after close."""

from __future__ import annotations

import pytest

from aaiclick.data.object_context.ch_client import LazyChClient


async def test_close_before_first_use_blocks_reconnect():
    client = LazyChClient()
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        await client.query("SELECT 1")
