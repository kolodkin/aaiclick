"""Tests for Azure Blob Storage: ``az://container/path`` URLs on
``Object.export()`` and ``open_object_from_url()``.

See docs/designs/blob_storage.md — "Azure".
"""

from __future__ import annotations

import pytest

from aaiclick import open_object_from_url
from aaiclick.data.url_utils import AzUrl, parse_az_url


@pytest.mark.parametrize(
    "url, connection, match",
    [
        ("az://container", "lake", "blob path"),
        ("az:///path.parquet", "lake", "container"),
        ("az://c/p.parquet?sig=abc", "lake", "query parameters"),
        ("s3://b/k.parquet", "lake", "connection"),
        ("/tmp/x.parquet", "lake", "connection"),
        ("az://c/p.parquet", "bad name", "connection"),
    ],
)
async def test_open_rejects_bad_azure_arguments(ctx, url, connection, match):
    with pytest.raises(ValueError, match=match):
        await open_object_from_url(url, connection=connection)


def test_parse_az_url():
    assert parse_az_url("az://reports/2026/daily.parquet") == AzUrl("reports", "2026/daily.parquet")
