"""
aaiclick.data.chdb_client - chdb adapter matching clickhouse-connect AsyncClient interface.

Provides ChdbClient that wraps chdb.session.Session to duck-type the subset of
clickhouse-connect's AsyncClient used by aaiclick (command, query, insert).

The session is stateful and disk-backed — tables persist across calls.
Thread-safe for concurrent access from background workers.
"""

from __future__ import annotations

import asyncio
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

from aaiclick.data.sql_utils import escape_sql_string

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
                url, response.status, response.reason, response.headers, response  # type: ignore[arg-type]
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
    """Mimics clickhouse-connect QueryResult with .result_rows and .first_row."""

    result_rows: list[tuple] = field(default_factory=list)
    column_names: list[str] = field(default_factory=list)

    @property
    def first_row(self) -> tuple:
        """Return the first row, matching clickhouse-connect QueryResult."""
        return self.result_rows[0]


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
        """Execute DDL or INSERT query, return scalar result if any.

        Matches AsyncClient.command() — used for CREATE TABLE, INSERT, DROP, EXISTS.
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
            return None

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
            pa_types = [_ch_type_to_pa(ct) for ct in column_type_names]
        else:
            pa_types = [None] * len(names)
        arrow_table = pa.table(  # noqa: F841 — referenced by SQL below
            {name: pa.array(col, type=pa_type) for name, col, pa_type in zip(names, cols_data, pa_types, strict=False)}
        )
        cols = f" ({', '.join(f'`{c}`' for c in names)})"
        self._session.query(f"INSERT INTO {table}{cols} SELECT * FROM Python(arrow_table)")

    def cleanup(self) -> None:
        """Clean up the chdb session."""
        self._session.cleanup()


class ChdbSyncClient:
    """Sync chdb client for TableWorker background thread.

    Matches the sync clickhouse-connect client interface used by TableWorker
    (command and close methods).
    """

    def __init__(self, session: Session):
        self._session = session

    def command(self, query: str) -> object:
        """Execute a command synchronously."""
        result = self._session.query(query, "TabSeparated")
        raw = result.bytes()
        if raw:
            text = raw.decode("utf-8").strip()
            if text:
                try:
                    return int(text)
                except ValueError:
                    return text
        return None

    def close(self) -> None:
        """No-op — session lifecycle managed by ChdbClient."""


_PA_BASE_TYPES: dict[str, pa.DataType] = {
    "UInt8": pa.uint8(),
    "UInt16": pa.uint16(),
    "UInt32": pa.uint32(),
    "UInt64": pa.uint64(),
    "Int8": pa.int8(),
    "Int16": pa.int16(),
    "Int32": pa.int32(),
    "Int64": pa.int64(),
    "Float32": pa.float32(),
    "Float64": pa.float64(),
    "String": pa.string(),
    "Bool": pa.bool_(),
}


def _ch_type_to_pa(ch_type: str) -> pa.DataType:
    """Convert a ClickHouse type string to a pyarrow DataType."""
    if ch_type.startswith("Nullable("):
        return _ch_type_to_pa(ch_type[9:-1])
    if ch_type.startswith("LowCardinality("):
        return _ch_type_to_pa(ch_type[15:-1])
    if ch_type in _PA_BASE_TYPES:
        return _PA_BASE_TYPES[ch_type]
    if ch_type.startswith("DateTime64"):
        return pa.timestamp("ms", tz="UTC")
    if ch_type.startswith("Array("):
        return pa.list_(_ch_type_to_pa(ch_type[6:-1]))
    if ch_type.startswith("Map("):
        key_type, val_type = _split_map_args(ch_type[4:-1])
        return pa.map_(_ch_type_to_pa(key_type), _ch_type_to_pa(val_type))
    if ch_type.startswith("Tuple("):
        elem_types = _split_top_level(ch_type[6:-1])
        return pa.struct([(f"f{i}", _ch_type_to_pa(t)) for i, t in enumerate(elem_types)])
    return pa.string()


def _split_map_args(inner: str) -> tuple[str, str]:
    """Split Map(K, V) arguments respecting nested parentheses."""
    depth = 0
    for i, ch in enumerate(inner):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            return inner[:i].strip(), inner[i + 1 :].strip()
    return inner, ""


def _split_top_level(inner: str) -> list[str]:
    """Split comma-separated type arguments respecting nested parentheses."""
    parts: list[str] = []
    depth = 0
    start = 0
    for i, ch in enumerate(inner):
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        elif ch == "," and depth == 0:
            parts.append(inner[start:i].strip())
            start = i + 1
    parts.append(inner[start:].strip())
    return parts


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
# All ChdbClient and ChdbSyncClient instances in a process share this session
# so that tables created in one data_context are visible to all others.
_sessions: dict[str, Session] = {}


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


def close_session(path: str) -> None:
    """Remove a cached chdb Session for the given data path.

    Calls cleanup() on the session before discarding to release native
    resources (file locks, C++ state).
    """
    session = _sessions.pop(path, None)
    if session is not None:
        session.cleanup()


def get_open_session(path: str) -> Session | None:
    """Return the cached Session for *path* if already open, else None.

    Unlike get_shared_session(), this never creates a new session.
    """
    return _sessions.get(path)


def create_chdb_session(path: str | None = None) -> Session:
    """Return the shared chdb Session (singleton per data path)."""
    return get_shared_session(path)


def create_chdb_client(path: str | None = None) -> ChdbClient:
    """Create a ChdbClient backed by the shared chdb session."""
    return ChdbClient(get_shared_session(path))


def create_chdb_sync_client(connection_string: str) -> ChdbSyncClient:
    """Create a ChdbSyncClient backed by the shared chdb session.

    Args:
        connection_string: chdb://path/to/data URL.
    """
    path = connection_string[len("chdb://") :]
    return ChdbSyncClient(get_shared_session(path))
