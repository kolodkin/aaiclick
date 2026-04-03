"""Native chdb benchmark — hand-written SQL, in-memory, materialized results.

Data is loaded from Python dict via PyArrow zero-copy table function.
All queries materialize results into tables (matching aaiclick's behavior)
so the comparison is apples-to-apples. chdb-specific optimizations:
LowCardinality, ORDER BY keys, optimize_aggregation_in_order.
"""

from contextlib import contextmanager

import chdb
import pyarrow as pa
from chdb.session import Session

NAME = "chdb"
VERSION = chdb.__version__

_session = None
_counter = 0


def _next_table():
    global _counter
    _counter += 1
    return f"bench.result_{_counter}"


@contextmanager
def context():
    global _session, _counter
    _session = Session()
    _counter = 0
    try:
        yield
    finally:
        _session.cleanup()
        _session.close()
        _session = None


def convert(data, filter_threshold):
    _session.query("CREATE DATABASE IF NOT EXISTS bench ENGINE = Atomic")
    _session.query("DROP TABLE IF EXISTS bench.data")
    _session.query("""
        CREATE TABLE bench.data (
            id Int64,
            category LowCardinality(String),
            subcategory LowCardinality(String),
            amount Float64,
            quantity Int64
        ) ENGINE = MergeTree() ORDER BY (category, subcategory)
    """)
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    _session.query("INSERT INTO bench.data SELECT * FROM Python(arrow_table)")
    return _session


def ingest_only(data, filter_threshold):
    """Insert-only benchmark: DDL is pre-created, only measure INSERT."""
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    _session.query("TRUNCATE TABLE bench.data")
    _session.query("INSERT INTO bench.data SELECT * FROM Python(arrow_table)")


def _insert_into(query):
    """Wrap a SELECT query in CREATE TABLE + INSERT INTO (materialize results)."""
    tbl = _next_table()
    _session.query(f"DROP TABLE IF EXISTS {tbl}")
    _session.query(f"CREATE TABLE {tbl} ENGINE = Memory AS {query}")


def make_benchmarks(filter_threshold):
    return {
        "Column sum": lambda s: _insert_into(
            "SELECT sum(amount) FROM bench.data"
        ),
        "Column multiply": lambda s: _insert_into(
            "SELECT amount * quantity AS value FROM bench.data"
        ),
        "Filter rows": lambda s: _insert_into(
            f"SELECT * FROM bench.data WHERE amount > {filter_threshold}"
        ),
        "Sort": lambda s: _insert_into(
            "SELECT * FROM bench.data ORDER BY amount DESC"
        ),
        "Count distinct": lambda s: _insert_into(
            "SELECT count() FROM (SELECT category FROM bench.data GROUP BY category)"
        ),
        "Group-by sum": lambda s: _insert_into(
            "SELECT category, sum(amount) FROM bench.data"
            " GROUP BY category SETTINGS optimize_aggregation_in_order=1"
        ),
        "Group-by count": lambda s: _insert_into(
            "SELECT category, count() FROM bench.data"
            " GROUP BY category SETTINGS optimize_aggregation_in_order=1"
        ),
        "Group-by multi-agg": lambda s: _insert_into(
            "SELECT category, sum(amount), avg(amount), min(amount), max(amount)"
            " FROM bench.data GROUP BY category"
            " SETTINGS optimize_aggregation_in_order=1"
        ),
        "Multi-key group-by": lambda s: _insert_into(
            "SELECT category, subcategory, sum(amount) FROM bench.data"
            " GROUP BY category, subcategory"
            " SETTINGS optimize_aggregation_in_order=1"
        ),
        "High-card group-by": lambda s: _insert_into(
            "SELECT subcategory, sum(amount) FROM bench.data"
            " GROUP BY subcategory SETTINGS optimize_aggregation_in_order=1"
        ),
    }
