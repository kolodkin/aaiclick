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
from aaiclick.backend import is_chdb
from aaiclick.data.models import FIELDTYPE_ARRAY, FIELDTYPE_DICT, ColumnInfo
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


@pytest.mark.parametrize(
    "url, kwargs, match",
    [
        pytest.param("ftp://example.com/data.parquet", {}, "http or https", id="invalid-scheme"),
        pytest.param("http://", {}, "valid host", id="no-host"),
        pytest.param("https://example.com/data.parquet", {"columns": []}, "non-empty", id="empty-columns"),
        pytest.param(
            "https://example.com/data.parquet",
            {"format": "InvalidFormat"},
            "Unsupported format",
            id="unsupported-format",
        ),
        pytest.param("https://example.com/data.parquet", {"limit": -1}, "positive integer", id="negative-limit"),
        pytest.param("https://example.com/data.parquet", {"limit": 0}, "positive integer", id="zero-limit"),
        pytest.param(
            "https://example.com/data.parquet",
            {"where": "1=1; DROP TABLE users"},
            "must not contain",
            id="where-with-semicolon",
        ),
    ],
)
async def test_url_input_validation_raises(ctx, url, kwargs, match):
    kwargs = {"columns": ["col1"], **kwargs}
    with pytest.raises(ValueError, match=match):
        await create_object_from_url(url, **kwargs)


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

# Envelope with a nested array path and nested item fields — dots in
# ``json_path`` / ``json_columns`` keys walk these paths.
_SAMPLE_JSON_NESTED = {
    "result": {
        "vulnerabilities": [
            {"cve": {"id": "CVE-1", "metrics": {"score": 9.8}}, "status": "analyzed"},
            {"cve": {"id": "CVE-2", "metrics": {"score": 5.4}}, "status": "modified"},
        ]
    }
}


class _JsonHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        doc = _SAMPLE_JSON_NESTED if self.path.endswith("/nested.json") else _SAMPLE_JSON
        body = json.dumps(doc).encode()
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
    # Multi-column json_columns produces a DICT Object — same rule as the
    # tabular CSV path (1 col → ARRAY with "value", N cols → DICT).
    assert obj.schema.fieldtype == FIELDTYPE_DICT
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 3
    assert set(data["id"]) == {"A-001", "A-002", "A-003"}
    assert set(data["name"]) == {"Alpha", "Beta", "Gamma"}
    schema = obj.schema
    assert schema.columns["id"].type == "String"
    assert schema.columns["score"].type == "Float64"
    assert schema.columns["tags"].array == 1
    assert schema.columns["tags"].type == "String"


async def test_json_load_single_column_renames_to_value(ctx, json_server):
    """Single-column json_columns produces an ARRAY Object with the column
    renamed to ``"value"`` — matches the tabular CSV path's contract so
    ``data()`` / ``extract_array_data`` find the column by name.
    """
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={"id": ColumnInfo("String")},
    )
    assert obj.schema.fieldtype == FIELDTYPE_ARRAY
    assert list(obj.schema.columns) == ["value"]
    # data() returns a list (array form) — not a dict.
    data = await obj.data()
    assert isinstance(data, list)
    assert sorted(data) == ["A-001", "A-002", "A-003"]


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


async def test_json_nested_path_extracts_array(ctx, json_server):
    """A dotted json_path walks the envelope to reach a nested array."""
    obj = await create_object_from_url(
        f"{json_server}/nested.json",
        format="RawBLOB",
        json_path="result.vulnerabilities",
        json_columns={"status": ColumnInfo("String")},
    )
    data = await obj.data()

    assert sorted(data) == ["analyzed", "modified"]


async def test_json_dotted_columns_walk_nested_fields(ctx, json_server):
    """Dotted json_columns keys extract nested item fields; data() re-nests them."""
    obj = await create_object_from_url(
        f"{json_server}/nested.json",
        format="RawBLOB",
        json_path="result.vulnerabilities",
        json_columns={
            "cve.id": ColumnInfo("String"),
            "cve.metrics.score": ColumnInfo("Float64"),
            "status": ColumnInfo("String"),
        },
    )
    assert set(obj.schema.columns) == {"cve.id", "cve.metrics.score", "status"}
    data = await obj.data()

    by_status = dict(zip(data["status"], data["cve"], strict=True))
    assert by_status["analyzed"] == {"id": "CVE-1", "metrics": {"score": 9.8}}
    assert by_status["modified"] == {"id": "CVE-2", "metrics": {"score": 5.4}}


async def test_json_single_dotted_column_renames_to_value(ctx, json_server):
    """A single dotted json_columns key still collapses to the "value" column."""
    obj = await create_object_from_url(
        f"{json_server}/nested.json",
        format="RawBLOB",
        json_path="result.vulnerabilities",
        json_columns={"cve.id": ColumnInfo("String")},
    )
    data = await obj.data()

    assert sorted(data) == ["CVE-1", "CVE-2"]


async def test_json_empty_path_segment_raises(ctx, json_server):
    """Empty segments in json_path or json_columns keys are rejected."""
    with pytest.raises(ValueError, match="empty path segment"):
        await create_object_from_url(
            f"{json_server}/nested.json",
            format="RawBLOB",
            json_path="result..vulnerabilities",
            json_columns={"status": ColumnInfo("String")},
        )
    with pytest.raises(ValueError, match="empty path segment"):
        await create_object_from_url(
            f"{json_server}/nested.json",
            format="RawBLOB",
            json_path="result.vulnerabilities",
            json_columns={".status": ColumnInfo("String")},
        )


# =============================================================================
# Compressed input: codec inferred from the URL's trailing suffix
# =============================================================================


@pytest.mark.parametrize("filename", ["sample.csv.gz", "sample.csv.xz"])
async def test_url_compressed_input(ctx, fileserver, filename):
    """ClickHouse decompresses input by the URL's trailing codec suffix.

    Real feeds ship this way — the cyber_threat_feeds example loads EPSS from
    a ``.csv.gz``. Decoded rows are the proof: the compressed bytes do not
    parse as CSV, so correct values mean decompression happened.
    """
    obj = await create_object_from_url(
        f"{fileserver}/{filename}",
        columns=["id", "price", "name"],
        format="CSVWithNames",
        limit=100,
    )
    data = await obj.data()
    assert len(data["id"]) == 100
    assert data["id"][0] == 1
    assert data["price"][0] == 1.5
    assert data["name"][0] == "item_1"


# =============================================================================
# Redirect integration: upstream 302 to the real file
# =============================================================================


# ClickHouse resolves absolute and root-relative ``Location`` headers, but
# resolves a path-relative one against the full request path instead of the
# parent directory — ``/dir/entry.parquet`` + ``sample.parquet`` becomes
# ``/dir/entry.parquet/sample.parquet``. Both backends share the engine's HTTP
# client, so this is not backend-specific.
_LOCATION_STYLES = {
    "absolute": "http://{host}/dir/sample.parquet",
    "root-relative": "/dir/sample.parquet",
    "path-relative": "sample.parquet",
}


class _RedirectHandler(BaseHTTPRequestHandler):
    """302s ``/dir/entry.parquet`` to the sample file under ``/dir/``.

    ``location_style`` selects the ``Location`` form; the absolute form echoes
    the client's own ``Host`` header so it resolves from localhost and from a
    ClickHouse container reaching the fixture via ``host.docker.internal``.
    """

    body: bytes = b""
    location_style: str = "absolute"

    def _headers_200(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(type(self).body)))
        self.end_headers()

    def do_HEAD(self):
        self._headers_200()

    def do_GET(self):
        if self.path.startswith("/dir/entry.parquet"):
            self.send_response(302)
            location = _LOCATION_STYLES[type(self).location_style]
            self.send_header("Location", location.format(host=self.headers["Host"]))
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.path == "/dir/sample.parquet":
            self._headers_200()
            self.wfile.write(type(self).body)
            return
        self.send_response(404)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *_args):
        pass


@pytest.fixture(scope="module")
def redirect_server():
    """HTTP server whose entry URL 302s to the real sample.parquet."""
    _RedirectHandler.body = (Path(_SAMPLES_DIR) / "sample.parquet").read_bytes()
    server = HTTPServer(("0.0.0.0", 0), _RedirectHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://{_FILESERVER_HOST}:{port}"
    server.shutdown()
    server.server_close()


async def _load_via_redirect(base: str, **kwargs):
    return await create_object_from_url(
        f"{base}/dir/entry.parquet",
        columns=["id", "price"],
        format="Parquet",
        limit=5,
        **kwargs,
    )


@pytest.mark.parametrize("style", ["absolute", "root-relative"])
async def test_url_follows_redirect(ctx, redirect_server, style):
    """A 302 is followed when ``max_http_get_redirects`` allows it."""
    _RedirectHandler.location_style = style
    obj = await _load_via_redirect(redirect_server, ch_settings={"max_http_get_redirects": 10})
    data = await obj.data()
    assert len(data["id"]) == 5
    assert data["id"][0] == 1


async def test_url_path_relative_redirect_unsupported(ctx, redirect_server):
    """A path-relative ``Location`` is resolved wrong by the engine and fails.

    Callers must resolve such redirects before handing the URL over — the
    cyber_threat_feeds EPSS loader does exactly that. Pinned here so the day
    the engine fixes it, this test fails and the workaround can go.
    """
    _RedirectHandler.location_style = "path-relative"
    with pytest.raises(Exception, match="[Rr]edirect"):
        await _load_via_redirect(redirect_server, ch_settings={"max_http_get_redirects": 10})


async def test_url_redirect_without_setting_raises(ctx, redirect_server):
    """An unfollowed redirect fails loudly instead of loading the 3xx body as data."""
    _RedirectHandler.location_style = "absolute"
    with pytest.raises(Exception, match="[Rr]edirect"):
        await _load_via_redirect(redirect_server)


# =============================================================================
# Retry integration: flaky upstream server returning 503 then 200
# =============================================================================


class _FlakyHandler(BaseHTTPRequestHandler):
    """Returns 503 on the first ``fail_count`` GETs, then serves the static body.

    HEAD always succeeds — ClickHouse sends a HEAD probe before each GET to
    determine content length, so we want HEAD to be invisible to the retry
    counter. It must not advertise ``Accept-Ranges``: the engine would then
    issue a Range GET, and answering that with the whole body fails the read
    with ``HTTP_RANGE_NOT_SATISFIABLE``.
    """

    fail_count = 1
    request_count = 0
    body: bytes = b""

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(type(self).body)))
        self.end_headers()

    def do_GET(self):
        type(self).request_count += 1
        if type(self).request_count <= type(self).fail_count:
            self.send_response(503)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(type(self).body)))
        self.end_headers()
        self.wfile.write(type(self).body)

    def log_message(self, *_args):
        pass


@pytest.fixture
def flaky_server():
    """Per-test flaky HTTP server. Resets counters and serves sample.parquet."""
    sample_path = Path(_SAMPLES_DIR) / "sample.parquet"
    _FlakyHandler.body = sample_path.read_bytes()
    _FlakyHandler.fail_count = 1
    _FlakyHandler.request_count = 0

    server = HTTPServer(("0.0.0.0", 0), _FlakyHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://{_FILESERVER_HOST}:{port}", _FlakyHandler
    server.shutdown()
    server.server_close()


# ``http_max_tries=1`` disables the engine's own upstream retry loop, leaving
# ``with_url_retry`` as the only thing that can reissue a failed fetch — so a
# second upstream hit is attributable to the wrapper under test.
_NO_ENGINE_RETRY: dict[str, str | int] = {"http_max_tries": 1}


@pytest.mark.skipif(
    not is_chdb(),
    reason="The real ClickHouse server issues Range requests the minimal flaky "
    "stub does not implement. Retry logic itself is covered by "
    "aaiclick/data/object/test_url_retry.py.",
)
async def test_url_retries_transient_503(ctx, flaky_server):
    """create_object_from_url retries on 503 and succeeds on the second attempt."""
    base, handler = flaky_server
    handler.fail_count = 1

    # Skip the DESCRIBE round-trip by passing column_types so request_count
    # only reflects the INSERT path.
    obj = await create_object_from_url(
        f"{base}/sample.parquet",
        columns=["id", "price"],
        format="Parquet",
        limit=5,
        column_types={"id": ColumnInfo("Int64"), "price": ColumnInfo("Float64")},
        ch_settings=_NO_ENGINE_RETRY,
        backoff_factor=0,  # zero sleep — keep test fast
    )
    data = await obj.data()
    assert len(data["id"]) == 5
    # The first GET was spent on the 503, so loading at all took a reissue.
    assert handler.request_count >= 2


@pytest.mark.skipif(
    not is_chdb(),
    reason="The real ClickHouse server issues Range requests the minimal flaky "
    "stub does not implement. Retry logic itself is covered by "
    "aaiclick/data/object/test_url_retry.py.",
)
async def test_url_exhausts_retries_on_persistent_503(ctx, flaky_server):
    """Persistent 503 exhausts retries and raises."""
    base, handler = flaky_server
    handler.fail_count = 100  # always fail

    with pytest.raises(Exception):  # noqa: B017 - either HTTPError or driver wrapper
        await create_object_from_url(
            f"{base}/sample.parquet",
            columns=["id", "price"],
            format="Parquet",
            limit=5,
            column_types={"id": ColumnInfo("Int64"), "price": ColumnInfo("Float64")},
            ch_settings=_NO_ENGINE_RETRY,
            retries=3,
            backoff_factor=0,
        )
    # 3 attempts, each failing on its first upstream GET.
    assert handler.request_count == 3
