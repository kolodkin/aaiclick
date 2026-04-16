"""Tests for ``Object.export()`` across every supported format.

Each format ships its own validation function that opens the exported file
and asserts the contents round-trip back to the expected rows. The format
table at the top is the single source of truth for the parametrized test —
adding a new format means adding one row here.
"""

from __future__ import annotations

import gzip
import json
import lzma
from collections.abc import Callable
from pathlib import Path

import pyarrow.feather as feather
import pyarrow.orc as orc
import pyarrow.parquet as pq
import pytest

from aaiclick import create_object_from_value

# Sample data used by every test — small enough to inline-validate.
SAMPLE = {
    "id": [1, 2, 3],
    "name": ["alice", "bob", "charlie"],
    "score": [10.5, 20.0, 30.25],
}


# =============================================================================
# Per-format validators — each takes a path and asserts the contents
# =============================================================================


def _validate_csv(path: str) -> None:
    text = Path(path).read_text()
    lines = text.strip().splitlines()
    assert lines[0] == '"id","name","score"'
    assert lines[1] == '1,"alice",10.5'
    assert lines[2] == '2,"bob",20'
    assert lines[3] == '3,"charlie",30.25'


def _validate_tsv(path: str) -> None:
    text = Path(path).read_text()
    lines = text.strip().splitlines()
    assert lines[0].split("\t") == ["id", "name", "score"]
    assert lines[1].split("\t") == ["1", "alice", "10.5"]
    assert lines[3].split("\t") == ["3", "charlie", "30.25"]


def _validate_json(path: str) -> None:
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line]
    assert rows == [
        {"id": 1, "name": "alice", "score": 10.5},
        {"id": 2, "name": "bob", "score": 20.0},
        {"id": 3, "name": "charlie", "score": 30.25},
    ]


def _validate_parquet(path: str) -> None:
    table = pq.read_table(path)
    assert table.column_names == ["id", "name", "score"]
    assert table.column("id").to_pylist() == [1, 2, 3]
    assert table.column("name").to_pylist() == ["alice", "bob", "charlie"]
    assert table.column("score").to_pylist() == [10.5, 20.0, 30.25]


def _validate_arrow(path: str) -> None:
    table = feather.read_table(path)
    assert table.column_names == ["id", "name", "score"]
    assert table.column("name").to_pylist() == ["alice", "bob", "charlie"]


def _validate_orc(path: str) -> None:
    table = orc.read_table(path)
    assert table.column_names == ["id", "name", "score"]
    assert table.column("score").to_pylist() == [10.5, 20.0, 30.25]


def _validate_avro(path: str) -> None:
    # Avro container files start with magic bytes b"Obj\x01".
    head = Path(path).read_bytes()[:4]
    assert head == b"Obj\x01", f"missing Avro magic, got {head!r}"


def _validate_markdown(path: str) -> None:
    text = Path(path).read_text()
    assert "id" in text and "name" in text and "score" in text
    assert "alice" in text and "charlie" in text
    # Markdown format renders a pipe-separated table.
    assert "|" in text


def _validate_xml(path: str) -> None:
    text = Path(path).read_text()
    assert "<row>" in text and "</row>" in text
    assert "<name>alice</name>" in text
    assert "<score>30.25</score>" in text


def _validate_sql(path: str) -> None:
    text = Path(path).read_text()
    assert text.lstrip().upper().startswith("INSERT INTO")
    assert "'alice'" in text
    assert "30.25" in text


def _validate_csv_gz(path: str) -> None:
    with gzip.open(path, "rt") as f:
        _validate_csv_text(f.read())


def _validate_csv_xz(path: str) -> None:
    with lzma.open(path, "rt") as f:
        _validate_csv_text(f.read())


def _validate_parquet_gz(path: str) -> None:
    # ClickHouse writes a gzip stream wrapping a Parquet file.
    raw = gzip.decompress(Path(path).read_bytes())
    # Parquet files start with PAR1 magic bytes.
    assert raw[:4] == b"PAR1", f"expected PAR1, got {raw[:4]!r}"


def _validate_csv_text(text: str) -> None:
    lines = text.strip().splitlines()
    assert lines[0] == '"id","name","score"'
    assert lines[3] == '3,"charlie",30.25'


def _validate_json_gz(path: str) -> None:
    with gzip.open(path, "rt") as f:
        rows = [json.loads(line) for line in f.read().splitlines() if line]
    assert len(rows) == 3
    assert rows[0]["name"] == "alice"


# =============================================================================
# Format → validator table — single source of truth for parametrization
# =============================================================================


FORMATS: list[tuple[str, Callable[[str], None]]] = [
    ("data.csv", _validate_csv),
    ("data.tsv", _validate_tsv),
    ("data.json", _validate_json),
    ("data.jsonl", _validate_json),
    ("data.ndjson", _validate_json),
    ("data.parquet", _validate_parquet),
    ("data.arrow", _validate_arrow),
    ("data.orc", _validate_orc),
    ("data.avro", _validate_avro),
    ("data.md", _validate_markdown),
    ("data.xml", _validate_xml),
    ("data.sql", _validate_sql),
    # Compression: ClickHouse picks the codec from the trailing suffix
    ("data.csv.gz", _validate_csv_gz),
    ("data.csv.xz", _validate_csv_xz),
    ("data.parquet.gz", _validate_parquet_gz),
    ("data.json.gz", _validate_json_gz),
]


@pytest.mark.parametrize(
    "filename,validator",
    FORMATS,
    ids=[name for name, _ in FORMATS],
)
async def test_export_format(ctx, tmp_path, filename, validator):
    """Export the sample dataset to *filename* and run its validator."""
    obj = await create_object_from_value(SAMPLE)
    path = str(tmp_path / filename)
    written = await obj.export(path)
    assert written == str(Path(path).resolve())
    assert Path(written).exists()
    assert Path(written).stat().st_size > 0
    validator(written)


# =============================================================================
# View constraint tests — make sure where/limit/order_by survive export
# =============================================================================


async def test_export_view_constraints(ctx, tmp_path):
    """View predicates must apply to the exported rows."""
    obj = await create_object_from_value(SAMPLE)
    view = obj.view(where="score >= 20", order_by="score DESC", limit=10)
    path = str(tmp_path / "filtered.csv")
    await view.export(path)
    lines = Path(path).read_text().strip().splitlines()
    assert lines[0] == '"id","name","score"'
    # Rows are ordered by score DESC and filtered to score >= 20
    assert lines[1] == '3,"charlie",30.25'
    assert lines[2] == '2,"bob",20'
    assert len(lines) == 3  # header + 2 matching rows


async def test_export_limit(ctx, tmp_path):
    """``limit`` must propagate to the export query."""
    obj = await create_object_from_value(SAMPLE)
    path = str(tmp_path / "limited.json")
    await obj.view(limit=2).export(path)
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line]
    assert len(rows) == 2


# =============================================================================
# Error cases
# =============================================================================


async def test_export_unsupported_extension(ctx, tmp_path):
    obj = await create_object_from_value(SAMPLE)
    with pytest.raises(ValueError, match="Unsupported export extension"):
        await obj.export(str(tmp_path / "data.xls"))


async def test_export_no_extension(ctx, tmp_path):
    obj = await create_object_from_value(SAMPLE)
    with pytest.raises(ValueError, match="Unsupported export extension"):
        await obj.export(str(tmp_path / "data"))


async def test_export_returns_absolute_path(ctx, tmp_path, monkeypatch):
    """Even when given a relative path, the returned path is absolute."""
    obj = await create_object_from_value(SAMPLE)
    monkeypatch.chdir(tmp_path)
    written = await obj.export("relative.csv")
    assert Path(written).is_absolute()
    assert Path(written).exists()
