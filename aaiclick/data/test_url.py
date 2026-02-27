"""
Tests for create_object_from_url().

Validation tests verify input sanitization (no ClickHouse needed beyond ctx fixture).
Integration tests use a local HTTP server serving CSV files to test end-to-end loading.
"""

import http.server
import io
import threading

import pytest

from aaiclick import create_object_from_url


# =============================================================================
# Local HTTP server fixture for integration tests
# =============================================================================

# CSV test data: 10 rows with id, name, price columns
_TEST_CSV = (
    "id,name,price\n"
    "1,apple,1.50\n"
    "2,banana,0.75\n"
    "3,cherry,3.00\n"
    "4,date,5.25\n"
    "5,elderberry,8.10\n"
    "6,fig,2.40\n"
    "7,grape,1.80\n"
    "8,honeydew,4.50\n"
    "9,kiwi,2.00\n"
    "10,lemon,0.90\n"
)


class _CSVHandler(http.server.BaseHTTPRequestHandler):
    """Serves _TEST_CSV on any GET request."""

    def do_GET(self):
        data = _TEST_CSV.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/csv")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, format, *args):
        pass  # suppress request logs


@pytest.fixture(scope="module")
def csv_server():
    """Start a local HTTP server that serves test CSV data. Returns the base URL."""
    server = http.server.HTTPServer(("127.0.0.1", 0), _CSVHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{port}/test.csv"
    server.shutdown()


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
# Integration tests (require ClickHouse + local HTTP server)
# =============================================================================


async def test_url_single_column(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["price"], format="CSVWithNames",
    )
    data = await obj.data()
    assert isinstance(data, list)
    assert len(data) == 10
    assert not obj.stale


async def test_url_multi_column(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["name", "price"], format="CSVWithNames",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert "name" in data
    assert "price" in data
    assert len(data["name"]) == 10
    assert len(data["price"]) == 10


async def test_url_with_limit(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["price"], format="CSVWithNames", limit=3,
    )
    data = await obj.data()
    assert len(data) == 3


async def test_url_with_where(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["name", "price"], format="CSVWithNames",
        where="price > 3",
    )
    data = await obj.data()
    assert isinstance(data, dict)
    assert all(p > 3 for p in data["price"])
    assert len(data["price"]) < 10


async def test_url_snowflake_ids_ordered(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["price"], format="CSVWithNames",
    )
    result = await ctx.ch_client.query(f"SELECT aai_id FROM {obj.table} ORDER BY aai_id")
    ids = [row[0] for row in result.result_rows]
    assert ids == sorted(ids)
    assert len(set(ids)) == len(ids)  # all unique
    assert len(ids) == 10


async def test_url_operations_on_result(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["price"], format="CSVWithNames",
    )
    total = await obj.sum()
    total_data = await total.data()
    assert total_data == pytest.approx(30.2, abs=0.01)


async def test_url_object_not_stale(ctx, csv_server):
    obj = await create_object_from_url(
        csv_server, columns=["price"], format="CSVWithNames",
    )
    assert not obj.stale
    data = await obj.data()
    assert len(data) == 10
