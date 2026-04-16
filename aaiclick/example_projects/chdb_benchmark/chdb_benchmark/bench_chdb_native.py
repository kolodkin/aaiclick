"""Native chdb benchmark — hand-written SQL, Memory engine, materialized results.

Data is loaded from Python dict via PyArrow zero-copy table function.
Uses Memory engine (matching aaiclick) and CREATE TABLE + INSERT INTO
(matching aaiclick's two-step pattern). This is the baseline — aaiclick
wraps chdb, so it should always be equal or slower than this.
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
    """Insert-only benchmark: create new Memory table each run."""
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
    _result_tables.append(tbl)


_result_tables = []


def _create_and_insert(create_ddl, insert_query, read_limit=0):
    """Two-step materialize: CREATE TABLE then INSERT INTO (matching aaiclick).

    When read_limit > 0, also SELECT that many rows from the result to
    measure end-to-end cost (e.g. sort + read).
    """
    tbl = _next_table()
    _session.query(create_ddl.format(tbl=tbl))
    _session.query(f"INSERT INTO {tbl} {insert_query}")
    if read_limit:
        _session.query(f"SELECT * FROM {tbl} LIMIT {read_limit}")
    _result_tables.append(tbl)


def cleanup_results():
    """Drop all accumulated result tables to release memory between benchmarks."""
    for tbl in _result_tables:
        _session.query(f"DROP TABLE IF EXISTS {tbl}")
    _result_tables.clear()


def make_benchmarks(filter_threshold):
    return {
        "Column sum": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (value Float64) ENGINE = Memory",
            "SELECT sum(amount) FROM bench.data",
        ),
        "Column multiply": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (value Float64) ENGINE = Memory",
            "SELECT amount * quantity AS value FROM bench.data",
        ),
        "Filter rows": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (id Int64, category String, subcategory String,"
            " amount Float64, quantity Int64) ENGINE = Memory",
            f"SELECT * FROM bench.data WHERE amount > {filter_threshold}",
        ),
        "Sort": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (id Int64, category String, subcategory String,"
            " amount Float64, quantity Int64) ENGINE = Memory",
            "SELECT * FROM bench.data ORDER BY amount DESC",
            read_limit=1000,
        ),
        "Count distinct": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (value UInt64) ENGINE = Memory",
            "SELECT count() FROM (SELECT category FROM bench.data GROUP BY category)",
        ),
        "Group-by sum": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (category String, amount Float64) ENGINE = Memory",
            "SELECT category, sum(amount) FROM bench.data GROUP BY category",
        ),
        "Group-by count": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (category String, _count UInt64) ENGINE = Memory",
            "SELECT category, count() FROM bench.data GROUP BY category",
        ),
        "Group-by multi-agg": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (category String, amount_sum Float64,"
            " amount_mean Float64, amount_min Float64, amount_max Float64)"
            " ENGINE = Memory",
            "SELECT category, sum(amount), avg(amount), min(amount), max(amount)"
            " FROM bench.data GROUP BY category",
        ),
        "Multi-key group-by": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (category String, subcategory String,"
            " amount Float64) ENGINE = Memory",
            "SELECT category, subcategory, sum(amount) FROM bench.data"
            " GROUP BY category, subcategory",
        ),
        "High-card group-by": lambda s: _create_and_insert(
            "CREATE TABLE {tbl} (subcategory String, amount Float64)"
            " ENGINE = Memory",
            "SELECT subcategory, sum(amount) FROM bench.data"
            " GROUP BY subcategory",
        ),
    }
