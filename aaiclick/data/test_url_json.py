"""
Tests for JSON mode of create_object_from_url().

Unit tests verify _json_extract_expr and validation.
Integration tests load nested JSON from a local HTTP server.
"""

import json
import os
import threading
from functools import partial
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from aaiclick import create_object_from_url
from aaiclick.data.models import ColumnInfo
from aaiclick.data.url import _json_extract_expr


# =============================================================================
# Unit tests for _json_extract_expr
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
    ids=["string", "int64", "uint32", "float64", "float32", "bool", "date", "datetime", "array", "nullable", "nullable_array", "escaped"],
)
def test_json_extract_expr(field, col_info, expected):
    assert _json_extract_expr(field, col_info) == expected


# =============================================================================
# Validation tests for JSON mode (no server needed)
# =============================================================================


async def test_json_mode_validation_errors(ctx):
    """All JSON mode validation errors in one test."""
    # missing json_path
    with pytest.raises(ValueError, match="json_path and json_columns must both be provided"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_columns={"id": ColumnInfo("String")},
        )
    # missing json_columns
    with pytest.raises(ValueError, match="json_path and json_columns must both be provided"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
        )
    # empty json_columns
    with pytest.raises(ValueError, match="non-empty dict"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
            json_columns={},
        )
    # wrong format
    with pytest.raises(ValueError, match="JSON mode requires format"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="CSV",
            json_path="data",
            json_columns={"id": ColumnInfo("String")},
        )
    # mutually exclusive with columns
    with pytest.raises(ValueError, match="mutually exclusive"):
        await create_object_from_url(
            "https://example.com/api.json",
            columns=["id"],
            format="RawBLOB",
            json_path="data",
            json_columns={"id": ColumnInfo("String")},
        )
    # reserved aai_id
    with pytest.raises(ValueError, match="reserved"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
            json_columns={"aai_id": ColumnInfo("UInt64")},
        )
    # no columns and no json
    with pytest.raises(ValueError, match="Either columns or json_path"):
        await create_object_from_url("https://example.com/api.json")


# =============================================================================
# Integration tests (require local HTTP server fixture)
# =============================================================================


_FILESERVER_HOST = os.getenv("AAICLICK_TEST_FILESERVER_HOST", "localhost")

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
    # Schema validation
    schema = obj.schema
    assert schema.columns["id"].type == "String"
    assert schema.columns["score"].type == "Float64"
    assert schema.columns["tags"].array is True
    assert schema.columns["tags"].type == "String"



async def test_json_load_subset_with_limit_and_where(ctx, json_server):
    """Subset columns, limit, and where filter."""
    # Subset columns
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

    # Limit
    obj_limited = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={"id": ColumnInfo("String")},
        limit=2,
    )
    data_limited = await obj_limited.data()
    assert len(data_limited["id"]) == 2

    # Where filter
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
    tags_by_id = dict(zip(data["id"], data["tags"]))
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
    data = await obj.data()
    assert len(data["id"]) == 3
