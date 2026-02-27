"""
Tests for create_object_from_url().

Validation tests verify input sanitization.
Integration tests use ClickHouse's own HTTP interface to serve test data in
various formats (Parquet, CSV, TSV, JSON, ORC). ClickHouse queries itself
via url(), avoiding Docker networking issues in CI.
"""

import urllib.parse

import pytest

from aaiclick import create_object_from_url, create_object_from_value
from aaiclick.data.env import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
)


# =============================================================================
# Helper: build ClickHouse HTTP URL for self-querying
# =============================================================================


def _ch_url(query: str) -> str:
    """Build a ClickHouse HTTP URL to execute a query against itself.

    ClickHouse can query its own HTTP interface via the url() table function.
    This avoids Docker networking issues in CI (ClickHouse container reaches itself).
    """
    auth = ""
    if CLICKHOUSE_PASSWORD:
        auth = f"{CLICKHOUSE_USER}:{CLICKHOUSE_PASSWORD}@"
    elif CLICKHOUSE_USER and CLICKHOUSE_USER != "default":
        auth = f"{CLICKHOUSE_USER}@"

    encoded_query = urllib.parse.quote(query, safe="")
    return f"http://{auth}{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/?query={encoded_query}"


# =============================================================================
# Fixture: test table with 200 rows of known data
# =============================================================================

_NUM_ROWS = 200


@pytest.fixture
async def source_table(ctx):
    """Create a source table with 200 rows for URL loading tests."""
    obj = await create_object_from_value({
        "id": list(range(1, _NUM_ROWS + 1)),
        "price": [round(i * 1.5, 2) for i in range(1, _NUM_ROWS + 1)],
        "name": [f"item_{i}" for i in range(1, _NUM_ROWS + 1)],
    })
    yield obj.table


# =============================================================================
# Input validation tests
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
# Per-format integration tests (ClickHouse self-query, limit=100)
# =============================================================================


async def test_url_format_parquet(ctx, source_table):
    """Load 100 rows from Parquet format."""
    url = _ch_url(f"SELECT id, price FROM {source_table} ORDER BY aai_id FORMAT Parquet")
    obj = await create_object_from_url(url, columns=["id", "price"], format="Parquet", limit=100)
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


async def test_url_format_csv_with_names(ctx, source_table):
    """Load 100 rows from CSVWithNames format."""
    url = _ch_url(f"SELECT id, price, name FROM {source_table} ORDER BY aai_id FORMAT CSVWithNames")
    obj = await create_object_from_url(
        url, columns=["id", "price", "name"], format="CSVWithNames", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100
    assert len(data["name"]) == 100


async def test_url_format_tsv_with_names(ctx, source_table):
    """Load 100 rows from TSVWithNames format."""
    url = _ch_url(f"SELECT id, price FROM {source_table} ORDER BY aai_id FORMAT TSVWithNames")
    obj = await create_object_from_url(
        url, columns=["id", "price"], format="TSVWithNames", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


async def test_url_format_json_each_row(ctx, source_table):
    """Load 100 rows from JSONEachRow format."""
    url = _ch_url(f"SELECT id, price FROM {source_table} ORDER BY aai_id FORMAT JSONEachRow")
    obj = await create_object_from_url(
        url, columns=["id", "price"], format="JSONEachRow", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


async def test_url_format_orc(ctx, source_table):
    """Load 100 rows from ORC format."""
    url = _ch_url(f"SELECT id, price FROM {source_table} ORDER BY aai_id FORMAT ORC")
    obj = await create_object_from_url(url, columns=["id", "price"], format="ORC", limit=100)
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["price"]) == 100


# =============================================================================
# Functional integration tests
# =============================================================================


async def test_url_single_column(ctx, source_table):
    """Single column load creates an array Object (column renamed to 'value')."""
    url = _ch_url(f"SELECT price FROM {source_table} ORDER BY aai_id FORMAT CSVWithNames")
    obj = await create_object_from_url(url, columns=["price"], format="CSVWithNames")
    data = await obj.data()
    assert isinstance(data, list)
    assert len(data) == _NUM_ROWS
    assert not obj.stale


async def test_url_multi_column(ctx, source_table):
    """Multi-column load creates a dict Object with original column names."""
    url = _ch_url(f"SELECT name, price FROM {source_table} ORDER BY aai_id FORMAT Parquet")
    obj = await create_object_from_url(url, columns=["name", "price"], format="Parquet")
    data = await obj.data()
    assert isinstance(data, dict)
    assert "name" in data
    assert "price" in data
    assert len(data["name"]) == _NUM_ROWS


async def test_url_with_limit(ctx, source_table):
    """LIMIT restricts the number of loaded rows."""
    url = _ch_url(f"SELECT price FROM {source_table} ORDER BY aai_id FORMAT CSVWithNames")
    obj = await create_object_from_url(
        url, columns=["price"], format="CSVWithNames", limit=3,
    )
    data = await obj.data()
    assert len(data) == 3


async def test_url_with_where(ctx, source_table):
    """WHERE clause filters rows during load."""
    url = _ch_url(f"SELECT id, price FROM {source_table} ORDER BY aai_id FORMAT CSVWithNames")
    obj = await create_object_from_url(
        url, columns=["id", "price"], format="CSVWithNames",
        where="price > 200",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert all(p > 200 for p in data["price"])
    assert len(data["price"]) < _NUM_ROWS


async def test_url_snowflake_ids_ordered(ctx, source_table):
    """Snowflake IDs are monotonically increasing and unique."""
    url = _ch_url(f"SELECT price FROM {source_table} ORDER BY aai_id FORMAT Parquet")
    obj = await create_object_from_url(
        url, columns=["price"], format="Parquet", limit=100,
    )
    result = await ctx.ch_client.query(f"SELECT aai_id FROM {obj.table} ORDER BY aai_id")
    ids = [row[0] for row in result.result_rows]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)
    assert len(ids) == 100


async def test_url_aggregation_on_result(ctx, source_table):
    """Aggregation operators work on Objects loaded from URL."""
    url = _ch_url(f"SELECT price FROM {source_table} ORDER BY aai_id FORMAT CSVWithNames")
    obj = await create_object_from_url(
        url, columns=["price"], format="CSVWithNames", limit=10,
    )
    # First 10 prices: 1.5, 3.0, 4.5, ..., 15.0 => sum = 82.5
    total = await obj.sum()
    total_data = await total.data()
    assert total_data == pytest.approx(82.5, abs=0.1)


async def test_url_known_dataset_parquet(ctx):
    """Load from a known public Parquet dataset (Teradata Kylo userdata1)."""
    url = "https://raw.githubusercontent.com/Teradata/kylo/master/samples/sample-data/parquet/userdata1.parquet"
    obj = await create_object_from_url(
        url, columns=["id", "salary"], format="Parquet", limit=100,
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert len(data["id"]) == 100
    assert len(data["salary"]) == 100
    assert all(isinstance(s, (int, float)) for s in data["salary"])
