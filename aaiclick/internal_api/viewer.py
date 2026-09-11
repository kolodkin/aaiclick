"""Internal API for the viewer: object-scoped queries, saved queries, dashboards.

Every function runs inside ``orch_context(with_ch=True)`` and the active
tenant. Queries never see a table name: ``open_scoped`` resolves ``(scope,
object)`` and the Object API builds the SELECT (see docs/designs/viewer.md).
"""

from __future__ import annotations

import asyncio
import json
from typing import TypeVar

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from aaiclick.data.data_context.ch_client import (
    CSV_WITH_NAMES,
    DEFAULT_MAX_EXECUTION_TIME,
    JSON_COMPACT,
    JSON_COMPACT_SETTINGS,
    query_bytes,
)
from aaiclick.data.models import AAI_ID_COLUMN
from aaiclick.data.object import Object
from aaiclick.data.sql_utils import quote_identifier, validate_where_expression
from aaiclick.data.view_models import ColumnSchema
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.tenancy import get_active_tenant_id
from aaiclick.view_models import Deleted, Page
from aaiclick.viewer.cell_view import cell_view_error
from aaiclick.viewer.models import DashboardRow, SavedQueryRow
from aaiclick.viewer.scope import parse_scope
from aaiclick.viewer.view_models import (
    FMT_CSV,
    MAX_LIMIT,
    Dashboard,
    DashboardIn,
    DashboardResults,
    DashboardSummary,
    ObjectQuery,
    ObjectQueryRequest,
    ObjectQueryResult,
    OrderBy,
    SavedQuery,
    SavedQueryFilter,
    SavedQueryIn,
)

from . import objects as objects_api
from .errors import Invalid, NotFound

RowT = TypeVar("RowT", SavedQueryRow, DashboardRow)


def _scope_job(key: str) -> str | None:
    """The job reference inside a scope key (``None`` for persistent); ``Invalid`` for a bad key."""
    try:
        return parse_scope(key).job
    except ValueError as exc:
        raise Invalid(str(exc)) from exc


def _check_where(where: str | None, prefix: str = "") -> None:
    if where is not None and (err := validate_where_expression(where)):
        raise Invalid(prefix + err)


def _projection(obj: Object, fields: list[str] | None) -> str:
    columns = [c for c in obj.schema.columns if c != AAI_ID_COLUMN]
    if fields is None:
        chosen = columns
    else:
        unknown = [f for f in fields if f not in columns]
        if unknown:
            raise Invalid(f"unknown fields: {', '.join(unknown)}")
        chosen = list(fields)
    if not chosen:
        raise Invalid("fields must name at least one column")
    return ", ".join(quote_identifier(c) for c in chosen)


def _order_clause(obj: Object, order_by: list[OrderBy]) -> str | None:
    if not order_by:
        return None
    parts = []
    for item in order_by:
        entry = OrderBy._make(item)
        if entry.name not in obj.schema.columns or entry.name == AAI_ID_COLUMN:
            raise Invalid(f"unknown order_by column: {entry.name}")
        parts.append(f"{quote_identifier(entry.name)} {entry.dir}")
    return ", ".join(parts)


async def query_object_bytes(request: ObjectQueryRequest) -> bytes:
    """One page of an object as ClickHouse sent it — ``JSONCompact`` or
    ``CSVWithNames``. The REST route returns these bytes verbatim, so the SPA's
    kernel reads ``{meta, data}`` with no parse or re-serialise in between."""
    _check_where(request.where)
    obj = await objects_api.open_scoped(request.object, _scope_job(request.scope))
    view = obj.view(
        where=request.where,
        order_by=_order_clause(obj, request.order_by),
        limit=request.limit,
        offset=request.offset,
    )
    sql = view.select_sql(columns=_projection(obj, request.fields))
    if request.fmt == FMT_CSV:
        return await query_bytes(sql, CSV_WITH_NAMES)
    settings = {**JSON_COMPACT_SETTINGS, "max_execution_time": DEFAULT_MAX_EXECUTION_TIME}
    return await query_bytes(sql, JSON_COMPACT, settings)


async def query_object(request: ObjectQueryRequest) -> ObjectQueryResult:
    """The same page typed for MCP and the CLI: ``meta`` + ``data`` for
    ``fmt="json"``, ``text`` for ``fmt="csv"``."""
    raw = await query_object_bytes(request)
    if request.fmt == FMT_CSV:
        return ObjectQueryResult(text=raw.decode("utf-8"))
    doc = json.loads(raw)
    return ObjectQueryResult(
        meta=[ColumnSchema(name=str(m["name"]), type=str(m["type"])) for m in doc["meta"]], data=doc["data"]
    )


async def _find_row(session: AsyncSession, model: type[RowT], name: str) -> RowT | None:
    """The active tenant's row named ``name`` — the one place the tenant filter lives."""
    stmt = select(model).where(model.tenant_id == get_active_tenant_id(), model.name == name)
    return (await session.execute(stmt)).scalar_one_or_none()


def _row_to_saved_query(row: SavedQueryRow) -> SavedQuery:
    return SavedQuery(
        name=row.name,
        scope=row.scope,
        object=row.object,
        fields=json.loads(row.fields) if row.fields else None,
        where=row.where,
        order_by=[OrderBy._make(o) for o in json.loads(row.order_by)] if row.order_by else [],
        cell_view=row.cell_view,
        updated_at=row.updated_at,
    )


def _validate_saved_query(query: SavedQueryIn) -> None:
    if not query.name.strip():
        raise Invalid("name required")
    if query.scope is not None:
        _scope_job(query.scope)
    _check_where(query.where)
    if err := cell_view_error(query.cell_view):
        raise Invalid(err)


async def list_saved_queries(filter: SavedQueryFilter | None = None) -> Page[SavedQuery]:
    """Saved queries of the active tenant. ``scope`` matches that scope plus
    queries saved without one; ``object`` matches exactly."""
    filter = filter or SavedQueryFilter()
    predicates = [SavedQueryRow.tenant_id == get_active_tenant_id()]
    if filter.scope is not None:
        predicates.append((col(SavedQueryRow.scope) == filter.scope) | (col(SavedQueryRow.scope).is_(None)))
    if filter.object is not None:
        predicates.append(SavedQueryRow.object == filter.object)
    stmt = select(SavedQueryRow).where(*predicates).order_by(col(SavedQueryRow.name)).limit(filter.limit)
    async with get_sql_session() as session:
        rows = (await session.execute(stmt)).scalars().all()
    return Page[SavedQuery](items=[_row_to_saved_query(r) for r in rows], total=len(rows))


async def save_query(query: SavedQueryIn) -> SavedQuery:
    """Upsert a saved query by ``(tenant, name)``."""
    _validate_saved_query(query)
    async with get_sql_session() as session:
        row = await _find_row(session, SavedQueryRow, query.name)
        if row is None:
            row = SavedQueryRow(id=get_snowflake_id(), tenant_id=get_active_tenant_id(), name=query.name, object="")
            session.add(row)
        row.scope = query.scope
        row.object = query.object
        row.where = query.where
        row.fields = json.dumps(query.fields) if query.fields is not None else None
        row.order_by = json.dumps([list(OrderBy._make(o)) for o in query.order_by]) if query.order_by else None
        row.cell_view = query.cell_view or None
        row.updated_at = utc_now()
        await session.commit()
        await session.refresh(row)
        return _row_to_saved_query(row)


async def delete_saved_query(name: str) -> Deleted:
    async with get_sql_session() as session:
        row = await _find_row(session, SavedQueryRow, name)
        if row is None:
            raise NotFound(f"Saved query not found: {name}")
        await session.delete(row)
        await session.commit()
    return Deleted(name=name)


def _row_to_dashboard(row: DashboardRow) -> Dashboard:
    queries = {panel: ObjectQuery.model_validate(q) for panel, q in json.loads(row.queries).items()}
    return Dashboard(name=row.name, scope=row.scope, html=row.html, queries=queries, updated_at=row.updated_at)


def _validate_dashboard(dashboard: DashboardIn) -> None:
    if not dashboard.name.strip():
        raise Invalid("name required")
    if not dashboard.html.strip():
        raise Invalid("html required")
    if not dashboard.queries:
        raise Invalid("at least one panel query required")
    _scope_job(dashboard.scope)
    for panel, query in dashboard.queries.items():
        _check_where(query.where, f"{panel}: ")


async def list_dashboards() -> Page[DashboardSummary]:
    stmt = select(DashboardRow).where(DashboardRow.tenant_id == get_active_tenant_id()).order_by(col(DashboardRow.name))
    async with get_sql_session() as session:
        rows = (await session.execute(stmt)).scalars().all()
    return Page[DashboardSummary](
        items=[DashboardSummary(name=r.name, scope=r.scope, updated_at=r.updated_at) for r in rows], total=len(rows)
    )


async def _dashboard_row(session: AsyncSession, name: str) -> DashboardRow:
    row = await _find_row(session, DashboardRow, name)
    if row is None:
        raise NotFound(f"Dashboard not found: {name}")
    return row


async def get_dashboard(name: str) -> Dashboard:
    async with get_sql_session() as session:
        return _row_to_dashboard(await _dashboard_row(session, name))


async def save_dashboard(dashboard: DashboardIn) -> Dashboard:
    """Upsert a dashboard by ``(tenant, name)``."""
    _validate_dashboard(dashboard)
    async with get_sql_session() as session:
        row = await _find_row(session, DashboardRow, dashboard.name)
        if row is None:
            row = DashboardRow(
                id=get_snowflake_id(),
                tenant_id=get_active_tenant_id(),
                name=dashboard.name,
                scope="",
                html="",
                queries="{}",
            )
            session.add(row)
        row.scope = dashboard.scope
        row.html = dashboard.html
        row.queries = json.dumps({panel: q.model_dump(mode="json") for panel, q in dashboard.queries.items()})
        row.updated_at = utc_now()
        await session.commit()
        await session.refresh(row)
        return _row_to_dashboard(row)


async def delete_dashboard(name: str) -> Deleted:
    async with get_sql_session() as session:
        row = await _dashboard_row(session, name)
        await session.delete(row)
        await session.commit()
    return Deleted(name=name)


async def run_dashboard(name: str) -> DashboardResults:
    """Run every panel query under the dashboard's scope; column-oriented results."""
    dashboard = await get_dashboard(name)
    panels = list(dashboard.queries)
    requests = [
        ObjectQueryRequest(**{**dashboard.queries[p].model_dump(), "scope": dashboard.scope, "limit": MAX_LIMIT})
        for p in panels
    ]
    pages = dict(zip(panels, await asyncio.gather(*(query_object(r) for r in requests)), strict=True))
    results = {
        panel: {c.name: [row[i] for row in page.data] for i, c in enumerate(page.meta)} for panel, page in pages.items()
    }
    return DashboardResults(results=results, meta={panel: page.meta for panel, page in pages.items()})
