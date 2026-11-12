"""
aaiclick.data.ch_client - ClickHouse client protocol and factory dispatch.

Defines ChClient Protocol for type-safe usage across the codebase,
and lazy-imports the appropriate concrete client based on AAICLICK_CH_URL.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path
from typing import Protocol, cast

import pyarrow as pa

from aaiclick.backend import is_chdb
from aaiclick.snowflake import get_snowflake_id

from ..models import QueryStats
from ..sql_utils import quote_sql_literal
from ..url_utils import require_local_backend


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
    async def raw_query(
        self,
        query: str,
        parameters: dict | None = None,
        settings: dict | None = None,
        fmt: str | None = None,
    ) -> bytes:
        """ClickHouse's own output for ``query`` in format ``fmt``, as bytes.

        ``fmt=None`` sends ``query`` unchanged — no ``FORMAT`` clause is
        appended, so the server's default applies. ``EXPLAIN`` callers rely
        on this: a ``FORMAT`` after an ``EXPLAIN`` binds to the explained
        query.
        """
        ...

    async def insert_arrow(self, table: str, arrow_table: pa.Table) -> None:
        """Insert an arrow table; column names come from the arrow schema.

        Contract: an empty table is a no-op, table columns absent from the
        arrow schema get their ClickHouse defaults, and identifier quoting
        is the implementation's responsibility.
        """
        ...

    async def close(self) -> None: ...


_ch_client_var: ContextVar[ChClient | None] = ContextVar("ch_client", default=None)

# VS Code's debug console evaluates each `await` in a fresh Python Context, so
# ContextVar-bound clients are invisible. Setting AAICLICK_DEBUGGER=1 enables a
# module-level chdb fallback (chdb only — the only backend that can be created
# synchronously without a running event loop).
_DEBUGGER_ENABLED = bool(os.environ.get("AAICLICK_DEBUGGER"))
# Module global on purpose: a ContextVar would be invisible to the debug console too.
_debug_ch_client: ChClient | None = None


def get_ch_client() -> ChClient:
    """Return the ClickHouse client for the active object context."""
    client = _ch_client_var.get()
    if client is None and _DEBUGGER_ENABLED and is_chdb():
        # Module global: the debugger fallback keeps one chdb session per process.
        global _debug_ch_client  # noqa: PLW0603
        if _debug_ch_client is None:
            # Load only the backend in use.
            from .chdb_client import create_chdb_client  # noqa: PLC0415

            _debug_ch_client = create_chdb_client()
        client = _debug_ch_client
    if client is None:
        raise RuntimeError(
            "No active data or orch context — use 'async with object_context()' or 'async with orch_context()'"
        )
    return client


async def execute_for_stats(
    query: str,
    settings: dict | None = None,
    parameters: dict | None = None,
    client: ChClient | None = None,
) -> QueryStats:
    """Run *query* purely to capture its ClickHouse run statistics.

    Backs the discard terminal (:meth:`Object.execute` runs ``… FORMAT Null``)
    and materializing writes (``copy`` / operator ``INSERT … SELECT``) that want
    the row/byte counts ClickHouse reports. Backend-agnostic: both clients'
    ``command()`` return a summary for a body-less statement — a
    ``clickhouse-connect`` ``QuerySummary`` (HTTP) or a ``ChdbCommandSummary``
    (chdb) — which :meth:`QueryStats.from_clickhouse_summary` maps uniformly.

    ``client`` defaults to the active context's client; ingest / operator
    callers pass their explicit client.
    """
    client = client or get_ch_client()
    summary = await client.command(query, settings=settings, parameters=parameters)
    return QueryStats.from_clickhouse_summary(summary)


async def export_query_to_file(query: str, path: str, fmt: str, settings: dict[str, str | int] | None = None) -> str:
    """Write a ``SELECT`` query result to a local file in *fmt*, replacing it.

    chdb only: ``INSERT INTO FUNCTION file('path', fmt) <query>`` — the embedded
    engine writes straight to disk, with compression inferred from the path
    suffix. A remote server cannot reach the client's disk, so it raises.
    """
    require_local_backend()
    target = Path(path).resolve()
    # Written beside the target and swapped in, so a failed export — or one
    # reading the target itself — leaves the existing file intact. The name
    # keeps its suffixes: ClickHouse picks the compression from them.
    partial = target.with_name(f".{get_snowflake_id()}-{target.name}")
    try:
        await get_ch_client().command(
            f"INSERT INTO FUNCTION file({quote_sql_literal(str(partial))}, '{fmt}') {query}", settings=settings
        )
        os.replace(partial, target)
    finally:
        partial.unlink(missing_ok=True)
    return str(target)


# Settings a bucket export keeps for itself: ``overwrite`` owns truncation,
# creating a new file would write to a key other than the URL returned, and
# lending the server's S3 identity is the operator's call (blob_storage.md).
_S3_EXPORT_RESERVED_SETTINGS = frozenset(
    {"s3_truncate_on_insert", "s3_create_new_file_on_insert", "s3_allow_server_credentials_in_user_queries"}
)


async def export_query_to_url(
    query: str, url: str, fmt: str, overwrite: bool = False, settings: dict[str, str | int] | None = None
) -> str:
    """Write a ``SELECT`` query result to a bucket URL in *fmt*, server-side.

    ``INSERT INTO FUNCTION s3(url, fmt) <query>`` on both backends: the bytes
    never pass through Python and the SQL carries no credentials — ClickHouse
    resolves them itself. An existing key is an error unless *overwrite*.
    """
    if reserved := _S3_EXPORT_RESERVED_SETTINGS.intersection(settings or ()):
        raise ValueError(f"ch_settings cannot set {sorted(reserved)} on export")
    await get_ch_client().command(
        f"INSERT INTO FUNCTION s3({quote_sql_literal(url)}, '{fmt}') {query}",
        settings={**(settings or {}), "s3_truncate_on_insert": int(overwrite)},
    )
    return url


DEFAULT_MAX_EXECUTION_TIME = 30
JSON_COMPACT = "JSONCompact"
CSV_WITH_NAMES = "CSVWithNames"
# Lossless JSONCompact: 64-bit integers, decimals, and denormals arrive quoted
# and named tuples as objects — the settings QueryView's driver sends, so the
# kernel renders {meta, data} unchanged.
JSON_COMPACT_SETTINGS = {
    "output_format_json_quote_64bit_integers": 1,
    "output_format_json_quote_decimals": 1,
    "output_format_json_quote_denormals": 1,
    "output_format_json_named_tuples_as_objects": 1,
}


async def query_bytes(sql: str, fmt: str | None = None, settings: dict | None = None) -> bytes:
    """ClickHouse's own output for ``sql`` in format ``fmt``, exactly as it sent
    it — so ``JSONCompact`` / ``CSVWithNames`` can reach a response body without
    a decode/encode round trip."""
    return await get_ch_client().raw_query(sql, settings=settings, fmt=fmt)


async def query_text(sql: str, fmt: str | None = None, settings: dict | None = None) -> str:
    """``query_bytes`` decoded, for callers that work with the text itself."""
    return (await query_bytes(sql, fmt, settings)).decode("utf-8")


async def create_ch_client() -> ChClient:
    """Create a ClickHouse client from AAICLICK_CH_URL."""
    if is_chdb():
        # Load only the backend in use.
        from .chdb_client import create_chdb_client  # noqa: PLC0415

        return create_chdb_client()

    # Load only the backend in use.
    from .clickhouse_client import create_clickhouse_client  # noqa: PLC0415

    return cast(ChClient, await create_clickhouse_client())


class LazyChClient:
    """ChClient that creates the real client on its first call.

    ``orch_context`` sets one at entry, so SQL-only callers never open
    ClickHouse or take the chdb file lock. Child tasks share the instance.
    With ``refused`` set it never connects: every call raises that message.
    Arguments past the first go by keyword: chdb and clickhouse-connect order
    ``settings`` / ``parameters`` differently.
    """

    def __init__(self, refused: str | None = None) -> None:
        self._client: ChClient | None = None
        self._lock = asyncio.Lock()
        self._refusal = refused

    async def _get(self) -> ChClient:
        if self._refusal is not None:
            raise RuntimeError(self._refusal)
        if self._client is not None:
            return self._client
        async with self._lock:
            if self._client is None:
                self._client = await create_ch_client()
            return self._client

    async def command(self, query: str, settings: dict | None = None, parameters: dict | None = None) -> object:
        return await (await self._get()).command(query, settings=settings, parameters=parameters)

    async def query(self, query: str, settings: dict | None = None, parameters: dict | None = None) -> QueryResult:
        return await (await self._get()).query(query, settings=settings, parameters=parameters)

    async def insert(
        self,
        table: str,
        data: Sequence[Sequence],
        column_names: Sequence[str] | None = None,
        column_oriented: bool = False,
        column_type_names: Sequence[str] | None = None,
    ) -> None:
        await (await self._get()).insert(
            table,
            data,
            column_names=column_names,
            column_oriented=column_oriented,
            column_type_names=column_type_names,
        )

    async def raw_query(
        self,
        query: str,
        parameters: dict | None = None,
        settings: dict | None = None,
        fmt: str | None = None,
    ) -> bytes:
        return await (await self._get()).raw_query(query, parameters=parameters, settings=settings, fmt=fmt)

    async def insert_arrow(self, table: str, arrow_table: pa.Table) -> None:
        await (await self._get()).insert_arrow(table, arrow_table)

    async def close(self) -> None:
        # A call after close must fail, not reconnect — even if nothing connected yet.
        self._refusal = "LazyChClient is closed"
        if self._client is not None:
            await self._client.close()


@contextmanager
def ch_refused(reason: str) -> Iterator[None]:
    """Make every ClickHouse call in this context raise ``RuntimeError(reason)``,
    including from nested ``orch_context`` calls, which reuse the outer client."""
    token = _ch_client_var.set(LazyChClient(refused=reason))
    try:
        yield
    finally:
        _ch_client_var.reset(token)
