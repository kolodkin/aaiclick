"""
aaiclick.data.ch_client - ClickHouse client protocol and factory dispatch.

Defines ChClient Protocol for type-safe usage across the codebase,
and lazy-imports the appropriate concrete client based on AAICLICK_CH_URL.
"""

from __future__ import annotations

import asyncio
import os
from contextvars import ContextVar
from pathlib import Path
from typing import Optional, Protocol, Sequence
from urllib.parse import urlparse

from aaiclick.backend import is_chdb
from ..formats import open_export_writer


class QueryResult(Protocol):
    """Protocol for ClickHouse query results."""

    result_rows: list[tuple]
    column_names: list[str]


class ChClient(Protocol):
    """Protocol for async ClickHouse client operations.

    Both ChdbClient and clickhouse-connect AsyncClient satisfy this protocol.

    ``parameters`` are ClickHouse ``{name:Type}`` placeholder bindings.
    Both backends support them natively: clickhouse-connect routes them
    through its own typed substitution, chdb forwards to ``session.query(
    sql, fmt, params=...)``. Prefer parameter binding over string
    interpolation for any value that might be large or user-derived.
    """

    async def command(
        self,
        query: str,
        settings: dict | None = None,
        parameters: dict | None = None,
    ) -> object: ...
    async def query(
        self,
        query: str,
        settings: dict | None = None,
        parameters: dict | None = None,
    ) -> QueryResult: ...
    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        column_type_names: Sequence[str] | None = None,
    ) -> None: ...


_ch_client_var: ContextVar[ChClient | None] = ContextVar('ch_client', default=None)

# VS Code's debug console evaluates each `await` in a fresh Python Context, so
# ContextVar-bound clients are invisible. Setting AAICLICK_DEBUGGER=1 enables a
# module-level chdb fallback (chdb only — the only backend that can be created
# synchronously without a running event loop).
_DEBUGGER_ENABLED = bool(os.environ.get("AAICLICK_DEBUGGER"))
_debug_ch_client: ChClient | None = None


def get_ch_client() -> ChClient:
    """Return the ClickHouse client for the active data context."""
    client = _ch_client_var.get()
    if client is None and _DEBUGGER_ENABLED and is_chdb():
        global _debug_ch_client
        if _debug_ch_client is None:
            from .chdb_client import create_chdb_client

            _debug_ch_client = create_chdb_client()
        client = _debug_ch_client
    if client is None:
        raise RuntimeError(
            "No active data or orch context — "
            "use 'async with data_context()' or 'async with orch_context()'"
        )
    return client


async def export_query_to_file(query: str, path: str, fmt: str) -> str:
    """Stream a ``SELECT`` query result to a local file in *fmt*.

    - **chdb (embedded):** ``INSERT INTO FUNCTION file('path', fmt) <query>``
      — the embedded engine writes directly to disk. Compression is inferred
      from the path suffix by ClickHouse's ``file()``.
    - **clickhouse-connect (remote HTTP):** ``raw_stream`` returns uncompressed
      formatted bytes; they are copied to the local file in 64 KB chunks via
      :func:`aaiclick.data.formats.open_export_writer`, which re-applies
      ``.gz`` / ``.xz`` compression client-side so output is byte-equivalent
      across backends. The blocking copy runs in a worker thread to keep the
      event loop responsive during multi-GB exports.

    ``INSERT INTO FUNCTION file()`` is unusable against a remote server
    because it would write to the server's ``user_files_path`` rather than
    the client's filesystem.
    """
    abs_path = str(Path(path).resolve())
    client = get_ch_client()
    if is_chdb():
        safe_path = abs_path.replace("'", "\\'")
        await client.command(
            f"INSERT INTO FUNCTION file('{safe_path}', '{fmt}') {query}"
        )
        return abs_path

    stream = await client.raw_stream(query=query, fmt=fmt)  # type: ignore[attr-defined]
    await asyncio.to_thread(_drain_stream_to_file, stream, abs_path)
    return abs_path


def _drain_stream_to_file(stream, abs_path: str) -> None:
    """Copy a blocking ``io.IOBase`` stream into *abs_path*, compressing if the suffix matches."""
    try:
        with open_export_writer(abs_path) as f:
            while chunk := stream.read(1 << 16):
                f.write(chunk)
    finally:
        stream.close()


async def create_ch_client() -> ChClient:
    """Create a ClickHouse client from AAICLICK_CH_URL."""
    if is_chdb():
        from .chdb_client import create_chdb_client

        return create_chdb_client()

    from .clickhouse_client import create_clickhouse_client

    return cast(ChClient, await create_clickhouse_client())


def create_sync_client(connection_string: str) -> object:
    """Create a sync ClickHouse client from a connection string.

    Supports:
    - chdb:///path/to/data → ChdbSyncClient
    - clickhouse://user:pass@host:port/db → clickhouse-connect sync client
    """
    if connection_string.startswith("chdb://"):
        from .chdb_client import create_chdb_sync_client

        return create_chdb_sync_client(connection_string)

    from clickhouse_connect import get_client

    parsed = urlparse(connection_string)
    return get_client(
        host=parsed.hostname or "localhost",
        port=parsed.port or 8123,
        username=parsed.username or "default",
        password=parsed.password or "",
        database=parsed.path.lstrip("/") or "default",
    )
