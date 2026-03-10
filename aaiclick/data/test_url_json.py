"""
Tests for JSON mode of create_object_from_url().

Unit tests verify _json_extract_expr and validation (always run).
Integration tests load nested JSON from a local HTTP server
(require AAICLICK_URL_TEST_ENABLE=1).
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


def test_json_extract_string():
    assert _json_extract_expr("name", ColumnInfo("String")) == "JSONExtractString(elem, 'name')"


def test_json_extract_int_types():
    for t in ("Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"):
        result = _json_extract_expr("count", ColumnInfo(t))
        assert result == "JSONExtractInt(elem, 'count')"


def test_json_extract_float_types():
    for t in ("Float32", "Float64"):
        result = _json_extract_expr("price", ColumnInfo(t))
        assert result == "JSONExtractFloat(elem, 'price')"


def test_json_extract_bool():
    assert _json_extract_expr("flag", ColumnInfo("Bool")) == "JSONExtractBool(elem, 'flag')"


def test_json_extract_date():
    assert _json_extract_expr("d", ColumnInfo("Date")) == "JSONExtract(elem, 'd', 'Date')"


def test_json_extract_datetime():
    result = _json_extract_expr("ts", ColumnInfo("DateTime"))
    assert result == "JSONExtract(elem, 'ts', 'DateTime')"


def test_json_extract_array():
    result = _json_extract_expr("tags", ColumnInfo("String", array=True))
    assert result == "JSONExtract(elem, 'tags', 'Array(String)')"


def test_json_extract_nullable():
    result = _json_extract_expr("notes", ColumnInfo("String", nullable=True))
    assert result == "JSONExtract(elem, 'notes', 'Nullable(String)')"


def test_json_extract_nullable_array():
    result = _json_extract_expr("vals", ColumnInfo("Int64", nullable=True, array=True))
    assert result == "JSONExtract(elem, 'vals', 'Array(Nullable(Int64))')"


def test_json_extract_escapes_field_name():
    result = _json_extract_expr("it's", ColumnInfo("String"))
    assert result == "JSONExtractString(elem, 'it\\'s')"


# =============================================================================
# Validation tests for JSON mode (no server needed)
# =============================================================================


async def test_json_mode_missing_json_path(ctx):
    with pytest.raises(ValueError, match="json_path and json_columns must both be provided"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_columns={"id": ColumnInfo("String")},
        )


async def test_json_mode_missing_json_columns(ctx):
    with pytest.raises(ValueError, match="json_path and json_columns must both be provided"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
        )


async def test_json_mode_empty_json_columns(ctx):
    with pytest.raises(ValueError, match="non-empty dict"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
            json_columns={},
        )


async def test_json_mode_wrong_format(ctx):
    with pytest.raises(ValueError, match="JSON mode requires format"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="CSV",
            json_path="data",
            json_columns={"id": ColumnInfo("String")},
        )


async def test_json_mode_mutually_exclusive_with_columns(ctx):
    with pytest.raises(ValueError, match="mutually exclusive"):
        await create_object_from_url(
            "https://example.com/api.json",
            columns=["id"],
            format="RawBLOB",
            json_path="data",
            json_columns={"id": ColumnInfo("String")},
        )


async def test_json_mode_reserved_aai_id(ctx):
    with pytest.raises(ValueError, match="reserved"):
        await create_object_from_url(
            "https://example.com/api.json",
            format="RawBLOB",
            json_path="data",
            json_columns={"aai_id": ColumnInfo("UInt64")},
        )


async def test_no_columns_and_no_json(ctx):
    with pytest.raises(ValueError, match="Either columns or json_path"):
        await create_object_from_url("https://example.com/api.json")


# =============================================================================
# Integration tests (require AAICLICK_URL_TEST_ENABLE=1)
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


@pytest.mark.url
async def test_json_load_all_columns(ctx, json_server):
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


@pytest.mark.url
async def test_json_load_subset_columns(ctx, json_server):
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
    assert isinstance(data, dict)
    assert set(data.keys()) == {"id", "score"}
    assert len(data["id"]) == 3


@pytest.mark.url
async def test_json_load_with_limit(ctx, json_server):
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={"id": ColumnInfo("String")},
        limit=2,
    )
    data = await obj.data()
    assert len(data["id"]) == 2


@pytest.mark.url
async def test_json_load_with_where(ctx, json_server):
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={
            "id": ColumnInfo("String"),
            "score": ColumnInfo("Float64"),
        },
        where="`score` > 80",
    )
    data = await obj.data()
    assert len(data["id"]) == 2
    assert all(s > 80 for s in data["score"])


@pytest.mark.url
async def test_json_load_array_field(ctx, json_server):
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
    assert isinstance(data, dict)
    tags_by_id = dict(zip(data["id"], data["tags"]))
    assert set(tags_by_id["A-001"]) == {"x", "y"}
    assert tags_by_id["A-002"] == ["z"]
    assert tags_by_id["A-003"] == []


@pytest.mark.url
async def test_json_load_json_as_string_format(ctx, json_server):
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="JSONAsString",
        json_path="items",
        json_columns={"id": ColumnInfo("String")},
    )
    data = await obj.data()
    assert len(data["id"]) == 3


@pytest.mark.url
async def test_json_load_schema(ctx, json_server):
    obj = await create_object_from_url(
        f"{json_server}/data.json",
        format="RawBLOB",
        json_path="items",
        json_columns={
            "id": ColumnInfo("String"),
            "score": ColumnInfo("Float64"),
            "tags": ColumnInfo("String", array=True),
        },
    )
    schema = obj.schema
    assert "id" in schema.columns
    assert schema.columns["id"].type == "String"
    assert schema.columns["score"].type == "Float64"
    assert schema.columns["tags"].array is True
    assert schema.columns["tags"].type == "String"
