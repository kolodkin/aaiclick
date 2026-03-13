"""
aaiclick.data.chdb_client - chdb adapter matching clickhouse-connect AsyncClient interface.

Provides ChdbClient that wraps chdb.session.Session to duck-type the subset of
clickhouse-connect's AsyncClient used by aaiclick (command, query, insert).

The session is stateful and disk-backed — tables persist across calls.
Thread-safe for concurrent access from background workers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Sequence

from chdb.session import Session


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

    async def command(self, query: str) -> object:
        """Execute DDL or INSERT query, return scalar result if any.

        Matches AsyncClient.command() — used for CREATE TABLE, INSERT, DROP, EXISTS.
        """
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

    async def query(self, query: str) -> ChdbQueryResult:
        """Execute SELECT query, return result with .result_rows.

        Matches AsyncClient.query() — returns object with result_rows attribute.
        Uses ArrowTable format for efficient, typed data from chdb.
        """
        table = self._session.query(query, "Arrowtable")
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


def create_chdb_session(path: Optional[str] = None) -> Session:
    """Create a disk-backed chdb Session.

    Args:
        path: Directory for chdb data. If None, uses get_chdb_data_path().
    """
    data_path = path or get_chdb_data_path()
    Path(data_path).mkdir(parents=True, exist_ok=True)
    return Session(data_path)


def create_chdb_client(path: Optional[str] = None) -> ChdbClient:
    """Create a ChdbClient with a disk-backed session.

    Args:
        path: Directory for chdb data. If None, uses default.
    """
    session = create_chdb_session(path)
    return ChdbClient(session)


def create_chdb_sync_client(connection_string: str) -> ChdbSyncClient:
    """Create a ChdbSyncClient from a chdb:// connection string.

    Args:
        connection_string: chdb://path/to/data URL.
    """
    path = connection_string[len("chdb://"):]
    session = create_chdb_session(path)
    return ChdbSyncClient(session)
