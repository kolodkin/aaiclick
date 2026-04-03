"""Native chdb benchmark — hand-written SQL, Memory engine, materialized results.

Data is loaded from Python dict via PyArrow zero-copy table function.
Uses Memory engine (matching aaiclick) and unique table names (no DROP
overhead). This is the baseline — aaiclick wraps chdb, so it should
always be equal or slower than this.
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
    return f"bench.r_{_counter}"


@contextmanager
def context():
    global _session, _counter
    _session = Session()
    _counter = 0
    _session.query("CREATE DATABASE IF NOT EXISTS bench ENGINE = Atomic")
    try:
        yield
    finally:
        _session.cleanup()
        _session.close()
        _session = None


def convert(data, filter_threshold):
    _session.query("DROP TABLE IF EXISTS bench.data")
    _session.query("""
        CREATE TABLE bench.data (
            id Int64,
            category String,
            subcategory String,
            amount Float64,
            quantity Int64
        ) ENGINE = Memory
    """)
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    _session.query("INSERT INTO bench.data SELECT * FROM Python(arrow_table)")
    return _session


def ingest_only(data, filter_threshold):
    """Insert-only benchmark: create new Memory table each run (no TRUNCATE)."""
    tbl = _next_table()
    _session.query(f"""
        CREATE TABLE {tbl} (
            id Int64,
            category String,
            subcategory String,
            amount Float64,
            quantity Int64
        ) ENGINE = Memory
    """)
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    _session.query(f"INSERT INTO {tbl} SELECT * FROM Python(arrow_table)")


_result_tables = []


def _insert_into(query):
    """Materialize a SELECT query into a new Memory table (no DROP overhead)."""
    tbl = _next_table()
    _session.query(f"CREATE TABLE {tbl} ENGINE = Memory AS {query}")
    _result_tables.append(tbl)


def cleanup_results():
    """Drop all accumulated result tables to release memory between benchmarks."""
    for tbl in _result_tables:
        _session.query(f"DROP TABLE IF EXISTS {tbl}")
    _result_tables.clear()


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
            "SELECT category, sum(amount) FROM bench.data GROUP BY category"
        ),
        "Group-by count": lambda s: _insert_into(
            "SELECT category, count() FROM bench.data GROUP BY category"
        ),
        "Group-by multi-agg": lambda s: _insert_into(
            "SELECT category, sum(amount), avg(amount), min(amount), max(amount)"
            " FROM bench.data GROUP BY category"
        ),
        "Multi-key group-by": lambda s: _insert_into(
            "SELECT category, subcategory, sum(amount) FROM bench.data"
            " GROUP BY category, subcategory"
        ),
        "High-card group-by": lambda s: _insert_into(
            "SELECT subcategory, sum(amount) FROM bench.data GROUP BY subcategory"
        ),
    }
