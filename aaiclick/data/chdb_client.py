"""
aaiclick.data.chdb_client - chdb adapter matching clickhouse-connect AsyncClient interface.

Provides ChdbClient that wraps chdb.session.Session to duck-type the subset of
clickhouse-connect's AsyncClient used by aaiclick (command, query, insert).

The session is stateful and disk-backed — tables persist across calls.
Thread-safe for concurrent access from background workers.
"""

from __future__ import annotations

import asyncio
import os
import re
import tempfile
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence
from urllib.parse import urlparse

from chdb.session import Session


# Matches url('https://...', 'Format') in SQL — used to detect and rewrite
# external URL calls that chdb's embedded HTTP client hangs on.
_URL_FUNC_RE = re.compile(r"url\('(https?://[^']+)',\s*'([^']+)'\)", re.IGNORECASE)

_LOCAL_HOSTS = frozenset({"localhost", "127.0.0.1", "::1", "0.0.0.0"})


async def _rewrite_external_urls(query: str) -> tuple[str, list[str]]:
    """Replace ``url('https://...', 'fmt')`` with ``file('/tmp/x', 'fmt')`` in *query*.

    Downloads each external URL to a temp file via :func:`asyncio.to_thread` so
    the event loop is not blocked.  Returns the rewritten query and a list of
    temp file paths that the caller must delete.

    chdb's embedded ClickHouse hangs indefinitely on external HTTP/HTTPS requests
    via ``url()``.  Downloading through Python first and using ``file()`` is the
    reliable workaround.
    """
    matches = list(_URL_FUNC_RE.finditer(query))
    if not matches:
        return query, []

    tmp_paths: list[str] = []
    # Build span→replacement map; apply in reverse to preserve offsets.
    replacements: dict[tuple[int, int], str] = {}

    for m in matches:
        url, fmt = m.group(1), m.group(2)
        if (urlparse(url).hostname or "") in _LOCAL_HOSTS:
            continue
        filename = Path(urlparse(url).path).name or "download"
        suffix = "".join(Path(filename).suffixes)  # e.g. ".tsv.gz"
        fd, tmp_path = tempfile.mkstemp(suffix=suffix)
        os.close(fd)
        await asyncio.to_thread(urllib.request.urlretrieve, url, tmp_path)
        tmp_paths.append(tmp_path)
        safe_tmp = tmp_path.replace("'", "\\'")
        replacements[m.span()] = f"file('{safe_tmp}', '{fmt}')"

    if not replacements:
        return query, []

    result = query
    for (start, end), replacement in sorted(replacements.items(), reverse=True):
        result = result[:start] + replacement + result[end:]

    return result, tmp_paths


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
        query, tmp_paths = await _rewrite_external_urls(query)
        try:
            result = self._session.query(_with_settings(query, settings), "TabSeparated")
            raw = result.bytes()
            if raw:
                text = raw.decode("utf-8").strip()
                if text:
                    try:
                        return int(text)
                    except ValueError:
                        return text
            return None
        finally:
            for p in tmp_paths:
                os.unlink(p)

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
        query, tmp_paths = await _rewrite_external_urls(query)
        try:
            table = self._session.query(_with_settings(query, settings), "Arrowtable")
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
        finally:
            for p in tmp_paths:
                os.unlink(p)

    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Optional[Sequence[str]] = None,
    ) -> None:
        """Bulk insert rows into a table.

        Matches AsyncClient.insert() — converts Python data to VALUES clause.
        """
        if not data:
            return

        cols = f" ({', '.join(f'`{c}`' for c in column_names)})" if column_names else ""
        value_rows = []
        for row in data:
            formatted = []
            for val in row:
                formatted.append(_format_value(val))
            value_rows.append(f"({', '.join(formatted)})")

        values_sql = ", ".join(value_rows)
        self._session.query(f"INSERT INTO {table}{cols} VALUES {values_sql}")

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


def _format_value(val: object) -> str:
    """Format a Python value for a ClickHouse VALUES clause."""
    if val is None:
        return "NULL"
    if isinstance(val, str):
        escaped = val.replace("\\", "\\\\").replace("'", "\\'")
        return f"'{escaped}'"
    if isinstance(val, bool):
        return "1" if val else "0"
    if isinstance(val, datetime):
        return f"'{val.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}'"
    if isinstance(val, (list, tuple)):
        inner = ", ".join(_format_value(v) for v in val)
        return f"[{inner}]"
    return str(val)


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
