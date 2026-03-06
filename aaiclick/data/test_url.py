"""
Tests for create_object_from_url().

Validation tests verify input sanitization (always run).
Integration tests load data from sample files served by a local HTTP server
(require AAICLICK_URL_TEST_ENABLE=1).

The fileserver fixture starts Python's http.server on a random port, serving
aaiclick/url_samples/. Set AAICLICK_TEST_FILESERVER_HOST=host.docker.internal
in CI where ClickHouse runs in Docker.
"""

import os
import threading
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import pytest

from aaiclick import create_object_from_url

_NUM_ROWS = 200
_SAMPLES_DIR = str(Path(__file__).resolve().parent.parent / "url_samples")
_FILESERVER_HOST = os.getenv("AAICLICK_TEST_FILESERVER_HOST", "localhost")


# =============================================================================
# Module fixture: local HTTP file server
# =============================================================================


@pytest.fixture(scope="module")
def fileserver():
    """Start a throwaway HTTP server serving url_samples/ on a random port."""
    handler = partial(SimpleHTTPRequestHandler, directory=_SAMPLES_DIR)
    handler.log_message = lambda *_args: None
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


@pytest.mark.url
async def test_url_format_parquet(ctx, fileserver):
    """Load 100 rows from Parquet sample file."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["id", "price"], format="Parquet", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


@pytest.mark.url
async def test_url_format_csv_with_names(ctx, fileserver):
    """Load 100 rows from CSV sample file."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv", columns=["id", "price", "name"], format="CSVWithNames", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100
    assert len(data["name"]) == 100


@pytest.mark.url
async def test_url_format_tsv_with_names(ctx, fileserver):
    """Load 100 rows from TSV sample file."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.tsv", columns=["id", "price"], format="TSVWithNames", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


@pytest.mark.url
async def test_url_format_json_each_row(ctx, fileserver):
    """Load 100 rows from JSONL sample file."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.jsonl", columns=["id", "price"], format="JSONEachRow", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


@pytest.mark.url
async def test_url_format_orc(ctx, fileserver):
    """Load 100 rows from ORC sample file."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.orc", columns=["id", "price"], format="ORC", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


# =============================================================================
# Functional integration tests (require file server)
# =============================================================================


@pytest.mark.url
async def test_url_single_column(ctx, fileserver):
    """Single column load creates an array Object (column renamed to 'value')."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv", columns=["price"], format="CSVWithNames",
    )
    data = await obj.data()
    assert isinstance(data, list)
    assert len(data) == _NUM_ROWS
    assert not obj.stale


@pytest.mark.url
async def test_url_multi_column(ctx, fileserver):
    """Multi-column load creates a dict Object with original column names."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["name", "price"], format="Parquet",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert "name" in data
    assert "price" in data
    assert len(data["name"]) == _NUM_ROWS


@pytest.mark.url
async def test_url_with_limit(ctx, fileserver):
    """LIMIT restricts the number of loaded rows."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv", columns=["price"], format="CSVWithNames", limit=3,
    )
    data = await obj.data()
    assert len(data) == 3


@pytest.mark.url
async def test_url_with_where(ctx, fileserver):
    """WHERE clause filters rows during load."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv", columns=["id", "price"], format="CSVWithNames",
        where="price > 200",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert all(p > 200 for p in data["price"])
    assert len(data["price"]) < _NUM_ROWS


@pytest.mark.url
async def test_url_snowflake_ids_ordered(ctx, fileserver):
    """Snowflake IDs are monotonically increasing and unique."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["price"], format="Parquet", limit=100,
    )
    result = await ctx.ch_client.query(f"SELECT aai_id FROM {obj.table} ORDER BY aai_id")
    ids = [row[0] for row in result.result_rows]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)
    assert len(ids) == 100


@pytest.mark.url
async def test_url_aggregation_on_result(ctx, fileserver):
    """Aggregation operators work on Objects loaded from URL."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv", columns=["price"], format="CSVWithNames", limit=10,
    )
    # First 10 prices: 1.5, 3.0, 4.5, ..., 15.0 => sum = 82.5
    total = await obj.sum()
    total_data = await total.data()
    assert total_data == pytest.approx(82.5, abs=0.1)


# =============================================================================
# insert_from_url() validation tests (require file server to create initial object)
# =============================================================================


@pytest.mark.url
async def test_insert_from_url_invalid_scheme(ctx, fileserver):
    """insert_from_url rejects non-HTTP URLs."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet", columns=["id", "price"], format="Parquet", limit=1
    )
    with pytest.raises(ValueError, match="http or https"):
        await obj.insert_from_url("ftp://example.com/data.parquet")


@pytest.mark.url
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


@pytest.mark.url
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


@pytest.mark.url
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


@pytest.mark.url
async def test_insert_from_url_appends_data(ctx, fileserver):
    """insert_from_url appends data to existing Object."""
    # Create initial object with 10 rows
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["id", "price"],
        format="Parquet",
        limit=10,
    )
    initial_count = len((await obj.data())["id"])
    assert initial_count == 10

    # Insert 5 more rows
    await obj.insert_from_url(
        f"{fileserver}/sample.parquet",
        columns=["id", "price"],
        format="Parquet",
        limit=5,
    )
    final_count = len((await obj.data())["id"])
    assert final_count == 15


@pytest.mark.url
async def test_insert_from_url_auto_columns(ctx, fileserver):
    """insert_from_url uses object's columns when not specified."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        limit=5,
    )

    # Insert without specifying columns - should use object's columns
    await obj.insert_from_url(
        f"{fileserver}/sample.csv",
        format="CSVWithNames",
        limit=5,
    )
    data = await obj.data()
    assert len(data["id"]) == 10
    assert len(data["price"]) == 10


@pytest.mark.url
async def test_insert_from_url_with_where(ctx, fileserver):
    """insert_from_url applies WHERE filter."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        limit=5,
    )
    initial_count = len((await obj.data())["id"])

    # Insert only rows where price > 200
    await obj.insert_from_url(
        f"{fileserver}/sample.csv",
        columns=["id", "price"],
        format="CSVWithNames",
        where="price > 200",
    )
    data = await obj.data()

    # Should have more rows than initial, but not all 200
    assert len(data["id"]) > initial_count
    assert len(data["id"]) < initial_count + _NUM_ROWS


@pytest.mark.url
async def test_insert_from_url_snowflake_ids(ctx, fileserver):
    """insert_from_url generates unique Snowflake IDs."""
    obj = await create_object_from_url(
        f"{fileserver}/sample.parquet",
        columns=["price"],
        format="Parquet",
        limit=5,
    )

    # Insert more data
    await obj.insert_from_url(
        f"{fileserver}/sample.parquet",
        columns=["price"],
        format="Parquet",
        limit=5,
    )

    # All IDs should be unique
    result = await ctx.ch_client.query(f"SELECT aai_id FROM {obj.table}")
    ids = [row[0] for row in result.result_rows]
    assert len(set(ids)) == len(ids)  # All unique
    assert len(ids) == 10
