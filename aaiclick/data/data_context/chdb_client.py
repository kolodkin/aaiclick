"""
aaiclick.data.chdb_client - chdb adapter matching clickhouse-connect AsyncClient interface.

Provides ChdbClient that wraps chdb.session.Session to duck-type the subset of
clickhouse-connect's AsyncClient used by aaiclick (command, query, insert).

The session is stateful and disk-backed — tables persist across calls.
Thread-safe for concurrent access from background workers.
"""

from __future__ import annotations

import asyncio
import atexit
import re
import shutil
import tempfile
import urllib.error
import urllib.request
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pyarrow as pa
from chdb.session import Session

from aaiclick.data.sql_utils import escape_sql_string, quote_identifier

from .arrow_types import ch_type_to_pa

# Matches url('https://...', 'Format') in SQL — used to detect and rewrite
# URL calls that chdb's embedded HTTP client hangs on.
_URL_FUNC_RE = re.compile(r"url\('(https?://[^']+)',\s*'([^']+)'\)", re.IGNORECASE)


def _download_to_path(url: str, dest: str) -> None:
    """Download ``url`` to ``dest`` with deterministic socket cleanup.

    Uses a bare opener (``HTTPHandler`` / ``HTTPSHandler`` without
    ``HTTPErrorProcessor``) so non-2xx responses surface as a regular
    ``HTTPResponse`` whose socket closes when the ``with`` block exits.
    The default ``urllib.request.urlopen`` raises ``HTTPError`` from
    inside the error processor and leaks the underlying socket, which
    later trips ``filterwarnings=error`` via ``PytestUnraisableExceptionWarning``.
    """
    opener = urllib.request.OpenerDirector()
    opener.add_handler(urllib.request.HTTPHandler())
    opener.add_handler(urllib.request.HTTPSHandler())
    opener.add_handler(urllib.request.HTTPDefaultErrorHandler())
    with opener.open(url) as response:
        if response.status >= 400:
            raise urllib.error.HTTPError(
                url,
                response.status,
                response.reason,
                response.headers,
                response,  # type: ignore[arg-type]
            )
        with open(dest, "wb") as out:
            shutil.copyfileobj(response, out)


@asynccontextmanager
async def _rewrite_external_urls(query: str) -> AsyncIterator[str]:
    """Context manager that rewrites ``url('https://...', 'fmt')`` → ``file(...)``.

    Downloads each URL to a :class:`tempfile.NamedTemporaryFile` via
    :func:`asyncio.to_thread`.  Files are deleted automatically when the
    ``async with`` block exits, whether normally or on exception.

    chdb's embedded ClickHouse hangs indefinitely on HTTP/HTTPS requests via
    ``url()``.  Downloading through Python first and loading with ``file()``
    is the reliable workaround.
    """
    matches = list(_URL_FUNC_RE.finditer(query))
    if not matches:
        yield query
        return

    replacements: dict[tuple[int, int], str] = {}
    tmp_files: list[Any] = []
    try:
        for m in matches:
            url, fmt = m.group(1), m.group(2)
            suffix = "".join(Path(urlparse(url).path).suffixes)  # e.g. ".tsv.gz"
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=True)
            tmp_files.append(tmp)
            await asyncio.to_thread(_download_to_path, url, tmp.name)
            safe_tmp = escape_sql_string(tmp.name)
            replacements[m.span()] = f"file('{safe_tmp}', '{fmt}')"

        if not replacements:
            yield query
            return

        # Apply replacements in reverse order to preserve string offsets.
        result = query
        for (start, end), replacement in sorted(replacements.items(), reverse=True):
            result = result[:start] + replacement + result[end:]

        yield result
    finally:
        for tmp in tmp_files:
            tmp.close()


def _with_settings(query: str, settings: dict | None) -> str:
    """Append a SETTINGS clause to a query for chdb.

    chdb does not accept settings as keyword arguments, so they must be
    embedded directly in the SQL. Integer/float values are unquoted;
    strings are single-quoted.
    """
    if not settings:
        return query
    parts = []
    for key, val in settings.items():
        if isinstance(val, bool):
            parts.append(f"{key}={1 if val else 0}")
        elif isinstance(val, (int, float)):
            parts.append(f"{key}={val}")
        else:
            escaped = escape_sql_string(str(val))
            parts.append(f"{key}='{escaped}'")
    return f"{query} SETTINGS {', '.join(parts)}"


def _serialize_param(value: object) -> object:
    """Convert a Python parameter value into a form chdb's ``{name:Type}``
    parser accepts.

    Numeric values and numeric arrays pass through — chdb serializes them
    fine. String arrays must be pre-formatted as a ClickHouse literal
    (``"['a','b']"``) because chdb's built-in stringifier emits bare
    tokens which the ``Array(String)`` parser rejects.
    """
    if isinstance(value, (list, tuple)):
        if not value:
            return "[]"
        first = value[0]
        if isinstance(first, str):
            parts = [f"'{escape_sql_string(v)}'" for v in value]
            return "[" + ",".join(parts) + "]"
    return value


def _serialize_parameters(parameters: dict | None) -> dict | None:
    if not parameters:
        return None
    return {k: _serialize_param(v) for k, v in parameters.items()}


@dataclass
class ChdbQueryResult:
    """Mimics clickhouse-connect QueryResult with .result_rows and .column_names."""

    result_rows: list[tuple] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)


class ChdbCommandSummary:
    """clickhouse-connect ``QuerySummary`` stand-in for a body-less chdb statement.

    Exposes a ``.summary`` dict keyed like ClickHouse's ``X-ClickHouse-Summary``
    header so :meth:`QueryStats.from_clickhouse_summary` maps both backends
    through one path. chdb surfaces only scan-side counters and elapsed time —
    ``storage_rows_read`` / ``storage_bytes_read`` are the rows/bytes read from
    storage (the analogue of the summary's ``read_rows`` / ``read_bytes``; the
    plain ``rows_read`` / ``bytes_read`` report the result stream, which is empty
    here). Written/result counts are absent, so ``QueryStats`` leaves them ``None``.
    """

    def __init__(self, result):
        self.summary = {
            "read_rows": str(result.storage_rows_read()),
            "read_bytes": str(result.storage_bytes_read()),
            "elapsed_ns": str(int(result.elapsed() * 1e9)),
        }


class ChdbClient:
    """Duck-type adapter for clickhouse-connect AsyncClient backed by chdb.

    Wraps a single chdb Session instance. All methods are sync internally
    but exposed as async to match the AsyncClient interface used by
    data_context.py and operators.py.

    Args:
        session: A chdb Session instance (disk-backed or ephemeral).
    """

    def __init__(self, session: Session):
        self._session = session

    @property
    def session(self) -> Session:
        """Access the underlying chdb session (for TableWorker)."""
        return self._session

    async def command(
        self,
        query: str,
        settings: dict | None = None,
        parameters: dict | None = None,
    ) -> object:
        """Execute DDL/INSERT/command query, mirroring AsyncClient.command().

        Like clickhouse-connect's ``command()``: returns the decoded scalar when
        the statement produces a result body (e.g. ``EXISTS``, ``SELECT count()``),
        and a :class:`ChdbCommandSummary` carrying the run's stats when it does
        not (``CREATE`` / ``DROP`` / ``INSERT … SELECT`` / ``SELECT … FORMAT Null``).
        That uniform return lets :func:`execute_for_stats` map both backends
        through :meth:`QueryStats.from_clickhouse_summary` without a backend check.

        Settings are embedded as a SQL SETTINGS clause since chdb does not accept
        them as keyword arguments. ``parameters`` are forwarded to chdb's
        native ``{name:Type}`` placeholder binding.

        Any ``url('https://...', 'fmt')`` calls in *query* are transparently
        rewritten to ``file('/tmp/x', 'fmt')`` because chdb's embedded HTTP client
        hangs on external URLs.
        """
        async with _rewrite_external_urls(query) as rewritten:
            result = self._session.query(
                _with_settings(rewritten, settings),
                "TabSeparated",
                params=_serialize_parameters(parameters),
            )
            raw = result.bytes()
        if raw:
            text = raw.decode("utf-8").strip()
            if text:
                try:
                    return int(text)
                except ValueError:
                    return text
        return ChdbCommandSummary(result)

    async def query(
        self,
        query: str,
        settings: dict | None = None,
        parameters: dict | None = None,
    ) -> ChdbQueryResult:
        """Execute SELECT query, return result with .result_rows.

        Matches AsyncClient.query() — returns object with result_rows attribute.
        Uses ArrowTable format for efficient, typed data from chdb.
        Settings are embedded as a SQL SETTINGS clause since chdb does not accept
        them as keyword arguments. ``parameters`` are forwarded to chdb's
        native ``{name:Type}`` placeholder binding.

        Any ``url('https://...', 'fmt')`` calls in *query* are transparently
        rewritten to ``file('/tmp/x', 'fmt')`` because chdb's embedded HTTP client
        hangs on external URLs.
        """
        async with _rewrite_external_urls(query) as rewritten:
            table = self._session.query(
                _with_settings(rewritten, settings),
                "Arrowtable",
                params=_serialize_parameters(parameters),
            )
            if table is None or table.num_rows == 0:
                return ChdbQueryResult()

            columns = table.to_pydict()
            col_names = list(table.column_names)
            n_rows = table.num_rows
            rows = [tuple(columns[name][i] for name in col_names) for i in range(n_rows)]
            return ChdbQueryResult(result_rows=rows, column_names=col_names)

    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        column_type_names: Sequence[str] | None = None,
    ) -> None:
        """Bulk insert via pyarrow Python() table function.

        Matches clickhouse-connect AsyncClient.insert() signature.
        When ``column_oriented=True``, *data* is a list of columns (zero-copy).
        When ``False`` (default), *data* is a list of rows (transposed internally).

        When ``column_type_names`` is provided, uses those ClickHouse type
        strings directly — no ``system.columns`` lookup needed.
        """
        if not data:
            return

        names = list(column_names) if column_names else [f"c{i}" for i in range(len(data[0]))]

        if column_oriented:
            cols_data = list(data)
        else:
            cols_data = [list(col) for col in zip(*data, strict=False)]

        if column_type_names:
            pa_types = [ch_type_to_pa(ct) for ct in column_type_names]
        else:
            pa_types = [None] * len(names)
        await self.insert_arrow(
            table,
            pa.table(
                {
                    name: pa.array(col, type=pa_type)
                    for name, col, pa_type in zip(names, cols_data, pa_types, strict=False)
                }
            ),
        )

    async def insert_arrow(self, table: str, arrow_table: pa.Table) -> None:
        """Insert a pyarrow Table via chdb's ``Python()`` table function.

        Column names are taken from the arrow schema; table columns absent
        from it (e.g. ``aai_id``) get their ClickHouse defaults.
        """
        if arrow_table.num_rows == 0:
            return
        cols = ", ".join(quote_identifier(c) for c in arrow_table.column_names)
        self._session.query(f"INSERT INTO {table} ({cols}) SELECT * FROM Python(arrow_table)")

    async def close(self) -> None:
        # chdb's Session is a per-process singleton (see docs/designs/testing.md)
        # owned outside ChdbClient — closing here would break sibling contexts.
        pass


def get_chdb_data_path() -> str:
    """Return the chdb data directory path from AAICLICK_CH_URL.

    Parses the path component of the chdb://path URL.
    """
    from aaiclick.backend import get_ch_url

    url = get_ch_url()
    if url.startswith("chdb://"):
        return url.removeprefix("chdb://")
    return str(Path.home() / ".aaiclick" / "chdb_data")


# Process-wide singleton chdb session, keyed by data path.
# All ChdbClient instances in a process share this session
# so that tables created in one data_context are visible to all others.
# chdb's Session cannot be safely closed and reopened in-process — we hold and
# reuse one Session per process for its entire lifetime and only close via the
# atexit hook below.
_sessions: dict[str, Session] = {}


def _close_sessions() -> None:
    """Shut down the shared sessions' engines before interpreter teardown.

    Registered with ``atexit`` so it runs while Python is still fully alive.
    Without it, whether the engine shuts down depends on ``Session.__del__``
    firing during interpreter finalization; when it doesn't, chdb's background
    threads (e.g. BgSchPool) outlive the interpreter and race the library's
    C++ static destructors inside ``exit()`` — an intermittent SIGSEGV/SIGABRT
    after an otherwise green process.
    """
    while _sessions:
        _, session = _sessions.popitem()
        session.cleanup()


atexit.register(_close_sessions)


def get_shared_session(path: str | None = None) -> Session:
    """Return (or create) the shared chdb Session for a given data path.

    Using a singleton ensures all data_context instances in the same process
    share one chdb session and can see each other's tables.

    Pass ``:memory:`` for an in-memory session (no disk persistence).
    """
    data_path = path or get_chdb_data_path()
    if data_path not in _sessions:
        if data_path == ":memory:":
            _sessions[data_path] = Session()
        else:
            Path(data_path).mkdir(parents=True, exist_ok=True)
            _sessions[data_path] = Session(data_path)
    return _sessions[data_path]


def create_chdb_client(path: str | None = None) -> ChdbClient:
    """Create a ChdbClient backed by the shared chdb session."""
    return ChdbClient(get_shared_session(path))
