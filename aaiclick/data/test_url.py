"""
Tests for create_object_from_url().

Validation tests verify input sanitization (always run).
Integration tests load data from sample files served by a local HTTP server
(require AAICLICK_URL_TEST_ENABLE=1).

The fileserver fixture starts Python's http.server on a random port, serving
aaiclick/url_samples/. ClickHouse reaches it via host.docker.internal.
"""

import threading
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import pytest

from aaiclick import create_object_from_url

_NUM_ROWS = 200
_SAMPLES_DIR = str(Path(__file__).resolve().parent.parent / "url_samples")


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
    yield f"http://host.docker.internal:{port}"
    server.shutdown()


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


async def test_url_invalid_column_name(ctx):
    with pytest.raises(ValueError, match="not a valid identifier"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["valid_col", "invalid col"],
        )


async def test_url_sql_injection_column_name(ctx):
    with pytest.raises(ValueError, match="not a valid identifier"):
        await create_object_from_url(
            "https://example.com/data.parquet",
            columns=["col1; DROP TABLE users--"],
        )


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
