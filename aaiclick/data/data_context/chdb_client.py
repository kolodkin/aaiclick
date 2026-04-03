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
import tempfile
import urllib.request
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncIterator, List, Optional, Sequence
from urllib.parse import urlparse

import pyarrow as pa
from chdb.session import Session


# Matches url('https://...', 'Format') in SQL — used to detect and rewrite
# external URL calls that chdb's embedded HTTP client hangs on.
_URL_FUNC_RE = re.compile(r"url\('(https?://[^']+)',\s*'([^']+)'\)", re.IGNORECASE)

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})


@asynccontextmanager
async def _rewrite_external_urls(query: str) -> AsyncIterator[str]:
    """Context manager that rewrites ``url('https://...', 'fmt')`` → ``file(...)``.

    Downloads each external URL to a :class:`tempfile.NamedTemporaryFile` via
    :func:`asyncio.to_thread`.  Files are deleted automatically when the
    ``async with`` block exits, whether normally or on exception.

    chdb's embedded ClickHouse hangs indefinitely on external HTTP/HTTPS
    requests via ``url()``.  Downloading through Python first and loading
    with ``file()`` is the reliable workaround.
    """
    matches = list(_URL_FUNC_RE.finditer(query))
    if not matches:
        yield query
        return

    replacements: dict[tuple[int, int], str] = {}
    tmp_files: list[tempfile.NamedTemporaryFile] = []
    try:
        for m in matches:
            url, fmt = m.group(1), m.group(2)
            if (urlparse(url).hostname or "") in _LOCAL_HOSTS:
                continue
            suffix = "".join(Path(urlparse(url).path).suffixes)  # e.g. ".tsv.gz"
            tmp = tempfile.NamedTemporaryFile(suffix=suffix, delete=True)
            tmp_files.append(tmp)
            await asyncio.to_thread(urllib.request.urlretrieve, url, tmp.name)
            safe_tmp = tmp.name.replace("'", "\\'")
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


def _with_settings(query: str, settings: Optional[dict]) -> str:
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
            escaped = str(val).replace("'", "\\'")
            parts.append(f"{key}='{escaped}'")
    return f"{query} SETTINGS {', '.join(parts)}"


@dataclass
class ChdbQueryResult:
    """Mimics clickhouse-connect QueryResult with .result_rows and .first_row."""

    result_rows: List[tuple] = field(default_factory=list)

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
        self._schema_cache: dict[str, dict[str, pa.DataType]] = {}

    @property
    def session(self) -> Session:
        """Access the underlying chdb session (for TableWorker)."""
        return self._session

    async def command(self, query: str, settings: Optional[dict] = None) -> object:
        """Execute DDL or INSERT query, return scalar result if any.

        Matches AsyncClient.command() — used for CREATE TABLE, INSERT, DROP, EXISTS.
        Settings are embedded as a SQL SETTINGS clause since chdb does not accept
        them as keyword arguments.

        Any ``url('https://...', 'fmt')`` calls in *query* are transparently
        rewritten to ``file('/tmp/x', 'fmt')`` because chdb's embedded HTTP client
        hangs on external URLs.
        """
        async with _rewrite_external_urls(query) as rewritten:
            result = self._session.query(_with_settings(rewritten, settings), "TabSeparated")
            raw = result.bytes()
            if raw:
                text = raw.decode("utf-8").strip()
                if text:
                    try:
                        return int(text)
                    except ValueError:
                        return text
            return None

    async def query(self, query: str, settings: Optional[dict] = None) -> ChdbQueryResult:
        """Execute SELECT query, return result with .result_rows.

        Matches AsyncClient.query() — returns object with result_rows attribute.
        Uses ArrowTable format for efficient, typed data from chdb.
        Settings are embedded as a SQL SETTINGS clause since chdb does not accept
        them as keyword arguments.

        Any ``url('https://...', 'fmt')`` calls in *query* are transparently
        rewritten to ``file('/tmp/x', 'fmt')`` because chdb's embedded HTTP client
        hangs on external URLs.
        """
        async with _rewrite_external_urls(query) as rewritten:
            table = self._session.query(_with_settings(rewritten, settings), "Arrowtable")
            if table is None or table.num_rows == 0:
                return ChdbQueryResult()

            columns = table.to_pydict()
            col_names = table.column_names
            n_rows = table.num_rows
            rows = [
                tuple(columns[name][i] for name in col_names)
                for i in range(n_rows)
            ]
            return ChdbQueryResult(result_rows=rows)

    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Optional[Sequence[str]] = None,
        column_oriented: bool = False,
    ) -> None:
        """Bulk insert via pyarrow Python() table function.

        Matches clickhouse-connect AsyncClient.insert() signature.
        When ``column_oriented=True``, *data* is a list of columns (zero-copy).
        When ``False`` (default), *data* is a list of rows — transposed to
        columns and typed from the table schema so pyarrow handles all
        ClickHouse types (Array, Map, Nullable, etc.) correctly.
        """
        if not data:
            return

        names = list(column_names) if column_names else [f"c{i}" for i in range(len(data[0]))]

        if column_oriented:
            cols_data = list(data)
        else:
            cols_data = [list(col) for col in zip(*data)]

        pa_types = self._get_pa_types(table, names)
        arrow_table = pa.table(  # noqa: F841 — referenced by SQL below
            {name: _make_pa_array(col, pa_types.get(name)) for name, col in zip(names, cols_data)}
        )
        cols = f" ({', '.join(f'`{c}`' for c in names)})"
        self._session.query(f"INSERT INTO {table}{cols} SELECT * FROM Python(arrow_table)")

    def _get_pa_types(self, table: str, columns: list[str]) -> dict[str, pa.DataType]:
        """Look up pyarrow types for columns from the table schema.

        Caches per table to avoid repeated system.columns queries.
        """
        if table not in self._schema_cache:
            result = self._session.query(
                f"SELECT name, type FROM system.columns WHERE table = '{table}'",
                "Arrowtable",
            )
            if result and result.num_rows > 0:
                d = result.to_pydict()
                self._schema_cache[table] = {
                    name: _ch_type_to_pa(ch_type) for name, ch_type in zip(d["name"], d["type"])
                }
            else:
                self._schema_cache[table] = {}
        schema = self._schema_cache[table]
        return {col: schema[col] for col in columns if col in schema}

    def cleanup(self) -> None:
        """Clean up the chdb session."""
        self._session.cleanup()


def _make_pa_array(col: list, pa_type: pa.DataType | None) -> pa.Array:
    """Build a pyarrow array, using the schema type with inference fallback.

    Uses the explicit schema type when available. Falls back to pyarrow
    inference when the schema type is incompatible with the Python data
    (e.g. Python bools into a UInt8 column — ClickHouse casts on insert).
    """
    if pa_type is not None:
        try:
            return pa.array(col, type=pa_type)
        except (pa.ArrowTypeError, pa.ArrowInvalid):
            pass
    return pa.array(col)


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
        inner = ch_type[4:-1]
        key_type, val_type = inner.split(", ", 1)
        return pa.map_(_ch_type_to_pa(key_type), _ch_type_to_pa(val_type))
    return pa.string()


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


def get_shared_session(path: Optional[str] = None) -> Session:
    """Return (or create) the shared chdb Session for a given data path.

    Using a singleton ensures all data_context instances in the same process
    share one chdb session and can see each other's tables.
    """
    data_path = path or get_chdb_data_path()
    if data_path not in _sessions:
        Path(data_path).mkdir(parents=True, exist_ok=True)
        _sessions[data_path] = Session(data_path)
    return _sessions[data_path]


def create_chdb_session(path: Optional[str] = None) -> Session:
    """Return the shared chdb Session (singleton per data path)."""
    return get_shared_session(path)


def create_chdb_client(path: Optional[str] = None) -> ChdbClient:
    """Create a ChdbClient backed by the shared chdb session."""
    return ChdbClient(get_shared_session(path))


def create_chdb_sync_client(connection_string: str) -> ChdbSyncClient:
    """Create a ChdbSyncClient backed by the shared chdb session.

    Args:
        connection_string: chdb://path/to/data URL.
    """
    path = connection_string[len("chdb://"):]
    return ChdbSyncClient(get_shared_session(path))
