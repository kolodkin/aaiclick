"""Tests for local files as located Objects, a chdb-only path.

Kept out of ``test_blob.py``: its dist CI job fails on any skipped test.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from aaiclick import create_object_from_value, open_object_from_url
from aaiclick.backend import is_chdb

SAMPLE = {
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "score": [10.5, 20.0, 30.25],
}

_chdb_only = pytest.mark.skipif(not is_chdb(), reason="local file locations need the chdb backend")


@_chdb_only
@pytest.mark.parametrize("extension", [".parquet", ".arrow", ".csv", ".csv.zst", ".jsonl.lz4"])
async def test_export_open_local_round_trip(ctx, tmp_path, extension):
    obj = await create_object_from_value(SAMPLE)
    path = await obj.export(str(tmp_path / f"sample{extension}"))
    opened = await open_object_from_url(path)
    assert await opened.data() == SAMPLE
    with pytest.raises(ValueError, match="read-only"):
        await opened.insert({"id": [4], "name": ["dave"], "score": [1.0]})


@_chdb_only
async def test_open_local_glob_aggregates_in_place(ctx, tmp_path):
    for part in (1, 2):
        await (await create_object_from_value([part])).export(str(tmp_path / f"part-{part}.parquet"))
    opened = await open_object_from_url(str(tmp_path / "part-*.parquet"))
    assert await opened.sum().data() == 3


@_chdb_only
async def test_open_local_without_extension_detects_format(ctx, tmp_path):
    exported = await (await create_object_from_value(SAMPLE)).export(str(tmp_path / "sample.parquet"))
    path = str(Path(exported).rename(tmp_path / "sample"))
    assert await (await open_object_from_url(path)).data() == SAMPLE


@_chdb_only
async def test_open_local_with_any_clickhouse_format(ctx, tmp_path):
    """``format=`` is not limited to the names aaiclick maps extensions to."""
    path = await (await create_object_from_value(SAMPLE)).export(str(tmp_path / "sample.tsv"))
    opened = await open_object_from_url(path, format="TabSeparatedWithNames")
    assert await opened.data() == SAMPLE


@_chdb_only
async def test_open_local_relative_path(ctx, tmp_path, monkeypatch):
    await (await create_object_from_value([1, 2, 3])).export(str(tmp_path / "rel.parquet"))
    monkeypatch.chdir(tmp_path)
    assert await (await open_object_from_url("rel.parquet")).data() == [1, 2, 3]


@pytest.mark.skipif(is_chdb(), reason="chdb reads and writes local files")
async def test_local_paths_rejected_on_remote_server(ctx, tmp_path):
    path = str(tmp_path / "sample.parquet")
    with pytest.raises(ValueError, match="chdb backend"):
        await open_object_from_url(path)
    with pytest.raises(ValueError, match="chdb backend"):
        await (await create_object_from_value([1])).export(path)
