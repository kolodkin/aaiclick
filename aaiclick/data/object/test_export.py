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
from aaiclick.backend import is_chdb
from aaiclick.testing import SAMPLE_TABLE

pytestmark = pytest.mark.skipif(not is_chdb(), reason="local file export needs the chdb backend")


# =============================================================================
# Per-format validators — each takes a path and asserts the contents
# =============================================================================


def _validate_csv(path: str) -> None:
    _validate_csv_text(Path(path).read_text())


def _validate_tsv(path: str) -> None:
    text = Path(path).read_text()
    lines = text.strip().splitlines()
    assert lines[0].split("\t") == ["1", "alice", "10.5"]
    assert lines[2].split("\t") == ["3", "charlie", "30.25"]


def _validate_json(path: str) -> None:
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line]
    assert rows == [
        {"id": 1, "name": "alice", "score": 10.5},
        {"id": 2, "name": "bob", "score": 20.0},
        {"id": 3, "name": "charlie", "score": 30.25},
    ]


def _validate_json_document(path: str) -> None:
    _validate_json_document_text(Path(path).read_text())


def _validate_json_document_text(text: str) -> None:
    # ClickHouse's JSON format: one document with the rows under "data".
    document = json.loads(text)
    assert [row["name"] for row in document["data"]] == ["alice", "bob", "charlie"]
    assert document["rows"] == 3


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
    assert text.strip().splitlines() == ['1,"alice",10.5', '2,"bob",20', '3,"charlie",30.25']


def _validate_json_gz(path: str) -> None:
    with gzip.open(path, "rt") as f:
        _validate_json_document_text(f.read())


# =============================================================================
# Format → validator table — single source of truth for parametrization
# =============================================================================


FORMATS: list[tuple[str, Callable[[str], None]]] = [
    ("data.csv", _validate_csv),
    ("data.tsv", _validate_tsv),
    ("data.json", _validate_json_document),
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
    obj = await create_object_from_value(SAMPLE_TABLE)
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
    obj = await create_object_from_value(SAMPLE_TABLE)
    view = obj.view(where="score >= 20", order_by="score DESC", limit=10)
    path = str(tmp_path / "filtered.csv")
    await view.export(path)
    lines = Path(path).read_text().strip().splitlines()
    # Rows are ordered by score DESC and filtered to score >= 20
    assert lines == ['3,"charlie",30.25', '2,"bob",20']


async def test_export_limit(ctx, tmp_path):
    """``limit`` must propagate to the export query."""
    obj = await create_object_from_value(SAMPLE_TABLE)
    path = str(tmp_path / "limited.jsonl")
    await obj.view(limit=2).export(path)
    rows = [json.loads(line) for line in Path(path).read_text().splitlines() if line]
    assert len(rows) == 2


async def test_export_format_overrides_extension(ctx, tmp_path):
    obj = await create_object_from_value(SAMPLE_TABLE)
    path = await obj.export(str(tmp_path / "data.csv"), format="CSVWithNames")
    assert Path(path).read_text().splitlines()[0] == '"id","name","score"'


async def test_export_ch_settings_reach_the_statement(ctx, tmp_path):
    obj = await create_object_from_value(SAMPLE_TABLE)
    path = await obj.export(str(tmp_path / "data.csv"), ch_settings={"format_csv_delimiter": ";"})
    assert Path(path).read_text().splitlines()[0] == '1;"alice";10.5'


async def test_export_ch_settings_cannot_stop_replacing(ctx, tmp_path):
    path = str(tmp_path / "data.csv")
    await (await create_object_from_value([1, 2])).export(path)
    await (await create_object_from_value([7])).export(path, ch_settings={"engine_file_truncate_on_insert": 0})
    assert Path(path).read_text().split() == ["7"]


async def test_export_rejects_setting_name_carrying_sql(ctx, tmp_path):
    obj = await create_object_from_value([1])
    with pytest.raises(ValueError, match="setting name"):
        await obj.export(str(tmp_path / "data.csv"), ch_settings={"max_threads=1 --": 1})


async def test_failed_export_keeps_existing_file(ctx, tmp_path):
    path = str(tmp_path / "data.csv")
    await (await create_object_from_value([1, 2])).export(path)
    with pytest.raises(Exception, match="Unknown format"):
        await (await create_object_from_value([7])).export(path, format="Nope")
    assert Path(path).read_text().split() == ["1", "2"]
    assert [p.name for p in tmp_path.iterdir()] == ["data.csv"]


# =============================================================================
# Error cases
# =============================================================================


@pytest.mark.parametrize(
    "filename",
    [
        pytest.param("data.xls", id="unsupported-extension"),
        pytest.param("data", id="no-extension"),
    ],
)
async def test_export_unsupported_extension_raises(ctx, tmp_path, filename):
    obj = await create_object_from_value(SAMPLE_TABLE)
    with pytest.raises(ValueError, match="Unsupported export extension"):
        await obj.export(str(tmp_path / filename))


@pytest.mark.parametrize(
    "filename, read, expected",
    [
        pytest.param("data.csv", lambda path: Path(path).read_text().split(), ["7", "8", "9"], id="csv"),
        pytest.param(
            "data.parquet", lambda path: pq.read_table(path).column("value").to_pylist(), [7, 8, 9], id="parquet"
        ),
    ],
)
async def test_export_replaces_existing_file(ctx, tmp_path, filename, read, expected):
    path = str(tmp_path / filename)
    await (await create_object_from_value([1, 2])).export(path)
    await (await create_object_from_value([7, 8, 9])).export(path)
    assert read(path) == expected


async def test_export_returns_absolute_path(ctx, tmp_path, monkeypatch):
    """Even when given a relative path, the returned path is absolute."""
    obj = await create_object_from_value(SAMPLE_TABLE)
    monkeypatch.chdir(tmp_path)
    written = await obj.export("relative.csv")
    assert Path(written).is_absolute()
    assert Path(written).exists()
