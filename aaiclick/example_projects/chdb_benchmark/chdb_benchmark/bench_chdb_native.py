"""Native chdb benchmark — hand-written SQL, Memory engine, materialized results.

Mirrors aaiclick's internal ``copy_db`` pattern: the LowCardinality(String)
schema matches the aaiclick Schema declared in ``bench_aaiclick``, and
materialization uses two statements (CREATE TABLE + INSERT INTO SELECT) —
the same shape that aaiclick's ``view.copy()`` emits. This is the baseline:
aaiclick wraps chdb, so it should be equal or slower than this.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from itertools import count

import chdb
import pyarrow as pa
from chdb.session import Session

from .config import FILTER_THRESHOLD

NAME = "chdb"
VERSION = chdb.__version__

_session_var: ContextVar[Session] = ContextVar("bench_chdb_session")
_sink_seq_var: ContextVar[Iterator[int]] = ContextVar("bench_chdb_sink_seq")

_COLUMNS_DDL = (
    "id Int64, category LowCardinality(String), subcategory LowCardinality(String), amount Float64, quantity Int64"
)


def _get_session() -> Session:
    """The chdb session opened by the enclosing :func:`context`."""
    return _session_var.get()


@contextmanager
def context() -> Iterator[None]:
    """Open a fresh chdb session. Called once per benchmark operation."""
    session = Session()
    session_token = _session_var.set(session)
    seq_token = _sink_seq_var.set(count())
    session.query("CREATE DATABASE IF NOT EXISTS bench ENGINE = Atomic")
    try:
        yield
    finally:
        _sink_seq_var.reset(seq_token)
        _session_var.reset(session_token)
        session.cleanup()
        session.close()


def convert(data):
    """Load the Python dict into ``bench.data`` via PyArrow zero-copy."""
    session = _get_session()
    session.query("DROP TABLE IF EXISTS bench.data")
    session.query(f"CREATE TABLE bench.data ({_COLUMNS_DDL}) ENGINE = Memory")
    arrow_table = pa.table(data)  # noqa: F841 — referenced by SQL below
    session.query("INSERT INTO bench.data SELECT * FROM Python(arrow_table)")
    return session


def _materialize(s, create_ddl, select_sql):
    """Two-step CREATE + INSERT materialize — mirrors aaiclick's ``copy_db``."""
    name = f"bench.sink_{next(_sink_seq_var.get())}"
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
