"""
Tests for create_object_from_url() and insert_from_url().

Validation tests verify input sanitization.
Integration tests load data from sample files served by a local HTTP server.

The fileserver fixture starts Python's http.server on a random port, serving
aaiclick/url_samples/. Set AAICLICK_TEST_FILESERVER_HOST=host.docker.internal
in CI where ClickHouse runs in Docker.

JSON mode tests use a dedicated handler serving nested JSON payloads.
"""

import json
import os
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import pytest

from aaiclick import create_object_from_url
from aaiclick.data.data_context import get_ch_client
from aaiclick.data.models import FIELDTYPE_DICT, ColumnInfo
from aaiclick.data.object.url import _json_extract_expr

_NUM_ROWS = 200
_SAMPLES_DIR = str(Path(__file__).resolve().parent.parent.parent / "url_samples")
_FILESERVER_HOST = os.getenv("AAICLICK_TEST_FILESERVER_HOST", "localhost")


# =============================================================================
# Module fixture: local HTTP file server
# =============================================================================


@pytest.fixture(scope="module")
def fileserver():
    """Start a throwaway HTTP server serving url_samples/ on a random port."""
    handler = partial(SimpleHTTPRequestHandler, directory=_SAMPLES_DIR)
    handler.log_message = lambda *_args: None  # type: ignore[attr-defined]
    server = HTTPServer(("0.0.0.0", 0), handler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://{_FILESERVER_HOST}:{port}"
    server.shutdown()
    server.server_close()


# =============================================================================
# Input validation tests (no file server needed)
# =============================================================================


async def test_url_invalid_scheme(ctx):
    with pytest.raises(ValueError, match="http or https"):
        await create_object_from_url("ftp://example.com/data.parquet", columns=["col1"])


async def test_url_no_host(ctx):
    with pytest.raises(ValueError, match="valid host"):
        await create_object_from_url("http://", columns=["col1"])


async def test_url_empty_columns(ctx):
    with pytest.raises(ValueError, match="non-empty"):
        await create_object_from_url("https://example.com/data.parquet", columns=[])


async def test_url_reserved_aai_id_column(ctx):
    with pytest.raises(ValueError, match="reserved"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["aai_id"],
        )


async def test_url_unsupported_format(ctx):
    with pytest.raises(ValueError, match="Unsupported format"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["col1"],
            format="InvalidFormat",
        )


async def test_url_invalid_limit_negative(ctx):
    with pytest.raises(ValueError, match="positive integer"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["col1"],
            limit=-1,
        )


async def test_url_invalid_limit_zero(ctx):
    with pytest.raises(ValueError, match="positive integer"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["col1"],
            limit=0,
        )


async def test_url_where_with_semicolon(ctx):
    with pytest.raises(ValueError, match="must not contain"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["col1"],
            where="1=1; DROP TABLE users",
        )


# =============================================================================
# Per-format integration tests (require file server)
# =============================================================================


# Headerless formats (``CSV``, ``TSV``, ``JSONCompactEachRow``) expose columns
# as ``c1`` / ``c2`` / ``c3`` because ClickHouse has no header row to bind
# names to — that's the natural way to consume them.
@pytest.mark.parametrize(
    "filename,fmt,columns",
    [
        pytest.param("sample.parquet", "Parquet", ["id", "price", "name"], id="Parquet"),
        pytest.param("sample.csv", "CSVWithNames", ["id", "price", "name"], id="CSVWithNames"),
        pytest.param("sample_noheader.csv", "CSV", ["c1", "c2", "c3"], id="CSV-no-header"),
        pytest.param(
            "sample_withtypes.csv", "CSVWithNamesAndTypes", ["id", "price", "name"], id="CSVWithNamesAndTypes"
        ),
        pytest.param("sample.tsv", "TSVWithNames", ["id", "price", "name"], id="TSVWithNames"),
        pytest.param("sample_noheader.tsv", "TSV", ["c1", "c2", "c3"], id="TSV-no-header"),
        pytest.param(
            "sample_withtypes.tsv", "TSVWithNamesAndTypes", ["id", "price", "name"], id="TSVWithNamesAndTypes"
        ),
        pytest.param("sample.jsonl", "JSONEachRow", ["id", "price", "name"], id="JSONEachRow"),
        pytest.param("sample_compact.jsonl", "JSONCompactEachRow", ["c1", "c2", "c3"], id="JSONCompactEachRow"),
        pytest.param("sample.orc", "ORC", ["id", "price", "name"], id="ORC"),
        pytest.param("sample.avro", "Avro", ["id", "price", "name"], id="Avro"),
    ],
)
async def test_url_format(ctx, fileserver, filename, fmt, columns):
    """Load 100 rows in each supported URL input format."""
    obj = await create_object_from_url(
        f"{fileserver}/{filename}",
        columns=columns,
        format=fmt,
        limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    for col in columns:
        assert len(data[col]) == 100
    # Row 1 round-trip — id=1, price=1.5, name='item_1' (regardless of column naming).
    assert data[columns[0]][0] == 1
    assert data[columns[1]][0] == 1.5
    assert data[columns[2]][0] == "item_1"


# =============================================================================
# Functional integration tests (require file server)
# =============================================================================


async def test_url_single_column(ctx, fileserver):
    """Single column load creates an array Object (column renamed to 'value')."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["price"],
        format="CSVWithNames",
    )
    data = await obj.data()
    assert isinstance(data, list)
    assert len(data) == _NUM_ROWS
    assert not obj.stale


async def test_url_multi_column(ctx, fileserver):
    """Multi-column load creates a dict Object with original column names."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["name", "price"],
        format="Parquet",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert "name" in data
    assert "price" in data
    assert len(data["name"]) == _NUM_ROWS


async def test_url_multi_column_is_dict_fieldtype(ctx, fileserver):
    """Multi-column URL object has FIELDTYPE_DICT schema (not FIELDTYPE_ARRAY)."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["name", "price"],
        format="Parquet",
    )
    assert obj._schema.fieldtype == FIELDTYPE_DICT


async def test_url_with_limit(ctx, fileserver):
    """LIMIT restricts the number of loaded rows."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["price"],
        format="CSVWithNames",
        limit=3,
    )
    data = await obj.data()
    assert len(data) == 3


async def test_url_with_where(ctx, fileserver):
    """WHERE clause filters rows during load."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        where="price > 200",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert all(p > 200 for p in data["price"])
    assert len(data["price"]) < _NUM_ROWS


async def test_url_snowflake_ids_ordered(ctx, fileserver):
    """Snowflake IDs are monotonically increasing and unique."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["price"],
        format="Parquet",
        limit=100,
    )
    result = await get_ch_client().query(f"SELECT aai_id FROM {obj.table} ORDER BY aai_id")
    ids = [row[0] for row in result.result_rows]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)
    assert len(ids) == 100


async def test_url_ch_settings_skip_comment_line(ctx, fileserver):
    """ch_settings skips a comment header line in CSV before column headers."""
    obj = await create_object_from_url(
        f"{fileserver}/sample_commented.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        ch_settings={"input_format_csv_skip_first_lines": 1},
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert data["id"] == [1, 2, 3, 4, 5]
    assert data["price"] == pytest.approx([10.0, 20.0, 30.0, 40.0, 50.0])


async def test_url_aggregation_on_result(ctx, fileserver):
    """Aggregation operators work on Objects loaded from URL."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["price"],
        format="CSVWithNames",
        limit=10,
    )
    total = await obj.sum()
    total_data = await total.data()
    assert total_data == pytest.approx(82.5, abs=0.1)


# =============================================================================
# insert_from_url() validation tests (require file server to create initial object)
# =============================================================================


async def test_insert_from_url_invalid_scheme(ctx, fileserver):
    """insert_from_url rejects non-HTTP URLs."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["id", "price"], format="Parquet", limit=1
    )
    with pytest.raises(ValueError, match="http or https"):
        await obj.insert_from_url("ftp://example.com/data.parquet")


async def test_insert_from_url_unsupported_format(ctx, fileserver):
    """insert_from_url rejects unsupported formats."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["id", "price"], format="Parquet", limit=1
    )
    with pytest.raises(ValueError, match="Unsupported format"):
        await obj.insert_from_url(
            f"{fileserver}/sample.parquet",
            columns=["id", "price"],
            format="InvalidFormat",
        )


async def test_insert_from_url_invalid_limit(ctx, fileserver):
    """insert_from_url rejects invalid limit values."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["id", "price"], format="Parquet", limit=1
    )
    with pytest.raises(ValueError, match="positive integer"):
        await obj.insert_from_url(
            f"{fileserver}/sample.parquet",
            columns=["id", "price"],
            limit=-1,
        )


async def test_insert_from_url_where_with_semicolon(ctx, fileserver):
    """insert_from_url rejects WHERE with semicolons (SQL injection)."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["id", "price"], format="Parquet", limit=1
    )
    with pytest.raises(ValueError, match="must not contain"):
        await obj.insert_from_url(
            f"{fileserver}/sample.parquet",
            columns=["id", "price"],
            where="1=1; DROP TABLE users",
        )


# =============================================================================
# insert_from_url() integration tests (require file server)
# =============================================================================


async def test_insert_from_url_appends_data(ctx, fileserver):
    """insert_from_url appends data to existing Object."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["id", "price"],
        format="Parquet",
        limit=10,
    )
    initial_count = len((await obj.data())["id"])
    assert initial_count == 10

    await obj.insert_from_url(
        f"{fileserver}/sample.parquet",
        columns=["id", "price"],
        format="Parquet",
        limit=5,
    )
    final_count = len((await obj.data())["id"])
    assert final_count == 15


async def test_insert_from_url_auto_columns(ctx, fileserver):
    """insert_from_url uses object's columns when not specified."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        limit=5,
    )

    await obj.insert_from_url(
        f"{fileserver}/sample.csv",
        format="CSVWithNames",
        limit=5,
    )
    data = await obj.data()
    assert len(data["id"]) == 10
    assert len(data["price"]) == 10


async def test_insert_from_url_with_where(ctx, fileserver):
    """insert_from_url applies WHERE filter."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        limit=5,
    )
    initial_count = len((await obj.data())["id"])

    await obj.insert_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        where="price > 200",
    )
    data = await obj.data()

    assert len(data["id"]) > initial_count
    assert len(data["id"]) < initial_count + _NUM_ROWS


async def test_insert_from_url_snowflake_ids(ctx, fileserver):
    """insert_from_url generates unique Snowflake IDs."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["price"],
        format="Parquet",
        limit=5,
    )

    await obj.insert_from_url(
        f"{fileserver}/sample.parquet",
        columns=["price"],
        format="Parquet",
        limit=5,
    )

    result = await get_ch_client().query(f"SELECT aai_id FROM {obj.table}")
    ids = [row[0] for row in result.result_rows]
    assert len(set(ids)) == len(ids)
    assert len(ids) == 10


# =============================================================================
# JSON mode: _json_extract_expr unit tests
# =============================================================================


@pytest.mark.parametrize(
    "field, col_info, expected",
    [
        ("name", ColumnInfo("String"), "JSONExtractString(elem, 'name')"),
        ("count", ColumnInfo("Int64"), "JSONExtractInt(elem, 'count')"),
        ("count", ColumnInfo("UInt32"), "JSONExtractInt(elem, 'count')"),
        ("price", ColumnInfo("Float64"), "JSONExtractFloat(elem, 'price')"),
        ("price", ColumnInfo("Float32"), "JSONExtractFloat(elem, 'price')"),
        ("flag", ColumnInfo("Bool"), "JSONExtractBool(elem, 'flag')"),
        ("d", ColumnInfo("Date"), "JSONExtract(elem, 'd', 'Date')"),
        ("ts", ColumnInfo("DateTime"), "JSONExtract(elem, 'ts', 'DateTime')"),
        ("tags", ColumnInfo("String", array=True), "JSONExtract(elem, 'tags', 'Array(String)')"),
        ("notes", ColumnInfo("String", nullable=True), "JSONExtract(elem, 'notes', 'Nullable(String)')"),
        ("vals", ColumnInfo("Int64", nullable=True, array=True), "JSONExtract(elem, 'vals', 'Array(Nullable(Int64))')"),
        ("it's", ColumnInfo("String"), "JSONExtractString(elem, 'it\\'s')"),
    ],
    ids=[
        "string",
        "int64",
        "uint32",
        "float64",
        "float32",
        "bool",
        "date",
        "datetime",
        "array",
        "nullable",
        "nullable_array",
        "escaped",
    ],
)
def test_json_extract_expr(field, col_info, expected):
    assert _json_extract_expr(field, col_info) == expected


# =============================================================================
# JSON mode: validation tests (no server needed)
# =============================================================================


async def test_json_mode_validation_errors(ctx):
    """All JSON mode validation errors in one test."""
    with pytest.raises(ValueError, match="json_path and json_columns must both be provided"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_columns={"id": ColumnInfo("String")},
        )
    with pytest.raises(ValueError, match="json_path and json_columns must both be provided"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
        )
    with pytest.raises(ValueError, match="non-empty dict"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
            json_columns={},
        )
    with pytest.raises(ValueError, match="JSON mode requires format"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="CSV",
            json_path="data",
            json_columns={"id": ColumnInfo("String")},
        )
    with pytest.raises(ValueError, match="mutually exclusive"):
        await create_object_from_url(
            "https://example.com/api.json",
            columns=["id"],
            format="RawBLOB",
            json_path="data",
            json_columns={"id": ColumnInfo("String")},
        )
    with pytest.raises(ValueError, match="reserved"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
            json_columns={"aai_id": ColumnInfo("UInt64")},
        )
    with pytest.raises(ValueError, match="Either columns or json_path"):
        await create_object_from_url("https://example.com/api.json")


# =============================================================================
# JSON mode: integration tests (require local HTTP server)
# =============================================================================


_SAMPLE_JSON = {
    "title": "Test Catalog",
    "count": 3,
    "items": [
        {"id": "A-001", "name": "Alpha", "score": 95.5, "active": True, "tags": ["x", "y"]},
        {"id": "A-002", "name": "Beta", "score": 82.0, "active": False, "tags": ["z"]},
        {"id": "A-003", "name": "Gamma", "score": 71.3, "active": True, "tags": []},
    ],
}


class _JsonHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = json.dumps(_SAMPLE_JSON).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_args):
        pass


@pytest.fixture(scope="module")
def json_server():
    server = HTTPServer(("0.0.0.0", 0), _JsonHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://{_FILESERVER_HOST}:{port}"
    server.shutdown()
    server.server_close()


async def test_json_load_all_columns_and_schema(ctx, json_server):
    """Load all columns, verify data and schema."""
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={
            "id": ColumnInfo("String"),
            "name": ColumnInfo("String"),
            "score": ColumnInfo("Float64"),
            "tags": ColumnInfo("String", array=True),
        },
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 3
    assert set(data["id"]) == {"A-001", "A-002", "A-003"}
    assert set(data["name"]) == {"Alpha", "Beta", "Gamma"}
    schema = obj.schema
    assert schema.columns["id"].type == "String"
    assert schema.columns["score"].type == "Float64"
    assert schema.columns["tags"].array is True
    assert schema.columns["tags"].type == "String"


async def test_json_load_subset_with_limit_and_where(ctx, json_server):
    """Subset columns, limit, and where filter."""
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={
            "id": ColumnInfo("String"),
            "score": ColumnInfo("Float64"),
        },
    )
    data = await obj.data()
    assert set(data.keys()) == {"id", "score"}
    assert len(data["id"]) == 3

    obj_limited = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={"id": ColumnInfo("String")},
        limit=2,
    )
    # Single-column json_columns yields a FIELDTYPE_ARRAY Object (column
    # renamed to "value"), matching the single-column tabular contract.
    data_limited = await obj_limited.data()
    assert len(data_limited) == 2

    obj_filtered = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={
            "id": ColumnInfo("String"),
            "score": ColumnInfo("Float64"),
        },
        where="`score` > 80",
    )
    data_filtered = await obj_filtered.data()
    assert len(data_filtered["id"]) == 2
    assert all(s > 80 for s in data_filtered["score"])


async def test_json_load_array_field(ctx, json_server):
    """Array fields are correctly extracted."""
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={
            "id": ColumnInfo("String"),
            "tags": ColumnInfo("String", array=True),
        },
    )
    data = await obj.data()
    tags_by_id = dict(zip(data["id"], data["tags"], strict=False))
    assert set(tags_by_id["A-001"]) == {"x", "y"}
    assert tags_by_id["A-002"] == ["z"]
    assert tags_by_id["A-003"] == []


async def test_json_load_json_as_string_format(ctx, json_server):
    """JSONAsString format also works for JSON mode."""
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="JSONAsString",
        json_path="items",
        json_columns={"id": ColumnInfo("String")},
    )
    # Single-column → FIELDTYPE_ARRAY Object — data() returns a list.
    data = await obj.data()
    assert len(data) == 3
