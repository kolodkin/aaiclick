"""Native chdb benchmark — hand-written SQL, Memory engine, materialized results.

Mirrors aaiclick's internal ``copy_db`` pattern: the LowCardinality(String)
schema matches the aaiclick Schema declared in ``bench_aaiclick``, and
materialization uses two statements (CREATE TABLE + INSERT INTO SELECT) —
the same shape that aaiclick's ``view.copy()`` emits. This is the baseline:
aaiclick wraps chdb, so it should be equal or slower than this.
"""

from contextlib import contextmanager

import chdb
import pyarrow as pa
from chdb.session import Session

from .config import FILTER_THRESHOLD

NAME = "chdb"
VERSION = chdb.__version__

_session = None
_sink_seq = 0

_COLUMNS_DDL = (
    "id Int64, category LowCardinality(String), subcategory LowCardinality(String), amount Float64, quantity Int64"
)


@contextmanager
def context():
    """Open a fresh chdb session. Called once per benchmark operation."""
    global _session, _sink_seq
    _session = Session()
    _sink_seq = 0
    _session.query("CREATE DATABASE IF NOT EXISTS bench ENGINE = Atomic")
    try:
        yield
    finally:
        _session.cleanup()
        _session.close()
        _session = None


def convert(data):
    """Load the Python dict into ``bench.data`` via PyArrow zero-copy."""
    _session.query("DROP TABLE IF EXISTS bench.data")
    _session.query(f"CREATE TABLE bench.data ({_COLUMNS_DDL}) ENGINE = Memory")
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    _session.query("INSERT INTO bench.data SELECT * FROM Python(arrow_table)")
    return _session


def _materialize(s, create_ddl, select_sql):
    """Two-step CREATE + INSERT materialize — mirrors aaiclick's ``copy_db``."""
    global _sink_seq
    name = f"bench.sink_{_sink_seq}"
    _sink_seq += 1
    s.query(f"CREATE TABLE {name} ({create_ddl}) ENGINE = Memory")
    s.query(f"INSERT INTO {name} {select_sql}")


BENCHMARKS = {
    "Column sum": lambda s: s.query("SELECT sum(amount) FROM bench.data"),
    "Column multiply": lambda s: _materialize(
        s,
        "value Float64",
        "SELECT amount * quantity AS value FROM bench.data",
    ),
    "Filter rows": lambda s: _materialize(
        s,
        _COLUMNS_DDL,
        f"SELECT id, category, subcategory, amount, quantity FROM bench.data WHERE amount > {FILTER_THRESHOLD}",
    ),
    "Sort": lambda s: _materialize(
        s,
        _COLUMNS_DDL,
        "SELECT id, category, subcategory, amount, quantity FROM bench.data ORDER BY amount DESC",
    ),
    "Count distinct": lambda s: s.query("SELECT count() FROM (SELECT category FROM bench.data GROUP BY category)"),
    "Group-by sum": lambda s: s.query("SELECT category, sum(amount) FROM bench.data GROUP BY category"),
    "Group-by count": lambda s: s.query("SELECT category, count() FROM bench.data GROUP BY category"),
    "Group-by multi-agg": lambda s: s.query(
        "SELECT category, sum(amount), avg(amount), min(amount), max(amount) FROM bench.data GROUP BY category"
    ),
    "Multi-key group-by": lambda s: s.query(
        "SELECT category, subcategory, sum(amount) FROM bench.data GROUP BY category, subcategory"
    ),
    "High-card group-by": lambda s: s.query("SELECT subcategory, sum(amount) FROM bench.data GROUP BY subcategory"),
}
