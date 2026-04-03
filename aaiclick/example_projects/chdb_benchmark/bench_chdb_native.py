"""Native chdb benchmark — hand-written SQL, in-memory, zero abstraction.

Data is loaded from Python dict via PyArrow zero-copy table function.
All queries use chdb-optimized patterns (LowCardinality, ORDER BY keys,
optimize_aggregation_in_order, FORMAT Null for compute-only).
"""

from contextlib import contextmanager

import chdb
import pyarrow as pa
from chdb.session import Session

NAME = "chdb"
VERSION = chdb.__version__

_session = None


@contextmanager
def context():
    global _session
    _session = Session()
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


def make_benchmarks(filter_threshold):
    return {
        "Column sum": lambda s: s.query(
            "SELECT sum(amount) FROM bench.data"
        ),
        "Column multiply": lambda s: s.query(
            "SELECT amount * quantity FROM bench.data FORMAT Null"
        ),
        "Filter rows": lambda s: s.query(
            f"SELECT * FROM bench.data WHERE amount > {filter_threshold} FORMAT Null"
        ),
        "Sort": lambda s: s.query(
            "SELECT * FROM bench.data ORDER BY amount DESC FORMAT Null"
        ),
        "Count distinct": lambda s: s.query(
            "SELECT count() FROM (SELECT category FROM bench.data GROUP BY category)"
        ),
        "Group-by sum": lambda s: s.query(
            "SELECT category, sum(amount) FROM bench.data"
            " GROUP BY category SETTINGS optimize_aggregation_in_order=1"
        ),
        "Group-by count": lambda s: s.query(
            "SELECT category, count() FROM bench.data"
            " GROUP BY category SETTINGS optimize_aggregation_in_order=1"
        ),
        "Group-by multi-agg": lambda s: s.query(
            "SELECT category, sum(amount), avg(amount), min(amount), max(amount)"
            " FROM bench.data GROUP BY category"
            " SETTINGS optimize_aggregation_in_order=1"
        ),
        "Multi-key group-by": lambda s: s.query(
            "SELECT category, subcategory, sum(amount) FROM bench.data"
            " GROUP BY category, subcategory"
            " SETTINGS optimize_aggregation_in_order=1"
        ),
        "High-card group-by": lambda s: s.query(
            "SELECT subcategory, sum(amount) FROM bench.data"
            " GROUP BY subcategory SETTINGS optimize_aggregation_in_order=1"
        ),
    }
