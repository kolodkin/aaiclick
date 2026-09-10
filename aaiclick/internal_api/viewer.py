"""Internal API for the viewer: object-scoped queries, saved queries, dashboards.

Every function runs inside ``orch_context(with_ch=True)`` and the active
tenant. Queries never see a table name: ``open_object`` resolves ``(scope,
object)`` and the Object API builds the SELECT (see docs/designs/viewer.md).
"""

from __future__ import annotations

import json
from typing import NamedTuple

from sqlmodel import col, select

from aaiclick.ai.agents.lineage_tools import (
    DEFAULT_MAX_EXECUTION_TIME,
    ColumnSchema,
    validate_where_expression,
)
from aaiclick.data.data_context import ObjectNotFoundError, open_object
from aaiclick.data.data_context.ch_client import query_text
from aaiclick.data.models import AAI_ID_COLUMN
from aaiclick.data.object import Object
from aaiclick.data.scope import SCOPE_GLOBAL, SCOPE_JOB
from aaiclick.data.sql_utils import quote_identifier
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.sql_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id
from aaiclick.tenancy import get_active_tenant_id
from aaiclick.view_models import Page
from aaiclick.viewer.cell_view import cell_view_error
from aaiclick.viewer.models import SavedQueryRow
from aaiclick.viewer.scope import SCOPE_JOB_KIND, SCOPE_PERSISTENT, ScopeKind, parse_scope
from aaiclick.viewer.view_models import (
    FMT_CSV,
    Deleted,
    ObjectQueryRequest,
    ObjectQueryResult,
    OrderBy,
    SavedQuery,
    SavedQueryFilter,
    SavedQueryIn,
)

from . import jobs as jobs_api
from .errors import Invalid, NotFound

JSON_COMPACT = "JSONCompact"
CSV_WITH_NAMES = "CSVWithNames"
JSON_SETTINGS = {
    "output_format_json_quote_64bit_integers": 1,
    "output_format_json_quote_decimals": 1,
    "output_format_json_quote_denormals": 1,
    "output_format_json_named_tuples_as_objects": 1,
}


class ResolvedScope(NamedTuple):
    kind: ScopeKind
    job_id: int | None
    job_name: str | None


async def resolve_scope(key: str) -> ResolvedScope:
    """Parse a scope key and resolve a job reference to one run of the tenant."""
    try:
        ref = parse_scope(key)
    except ValueError as exc:
        raise Invalid(str(exc)) from exc
    if ref.kind == SCOPE_PERSISTENT or ref.job is None:
        return ResolvedScope(SCOPE_PERSISTENT, None, None)
    job = await jobs_api.resolve_job(ref.job)  # NotFound for a foreign or missing job
    return ResolvedScope(SCOPE_JOB_KIND, job.id, job.name)


async def open_scoped(name: str, scope: ResolvedScope) -> Object:
    """``open_object`` for a resolved scope; ``NotFound`` when the object is missing."""
    try:
        if scope.kind == SCOPE_PERSISTENT:
            return await open_object(name, scope=SCOPE_GLOBAL)
        return await open_object(name, scope=SCOPE_JOB, job_id=scope.job_id)
    except (ObjectNotFoundError, ValueError) as exc:
        raise NotFound(f"Object not found in scope: {name}") from exc


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


async def query_object(request: ObjectQueryRequest) -> ObjectQueryResult:
    """Read one page of an object as ClickHouse JSONCompact (or CSV text)."""
    if request.where is not None and (err := validate_where_expression(request.where)):
        raise Invalid(err.message)
    scope = await resolve_scope(request.scope)
    obj = await open_scoped(request.object, scope)
    projection = _projection(obj, request.fields)
    view = obj.view(
        where=request.where, order_by=_order_clause(obj, request.order_by), limit=request.limit, offset=request.offset
    )
    sql = view.select_sql(columns=projection)
    if request.fmt == FMT_CSV:
        return ObjectQueryResult(text=await query_text(sql, CSV_WITH_NAMES))
    text = await query_text(sql, JSON_COMPACT, {**JSON_SETTINGS, "max_execution_time": DEFAULT_MAX_EXECUTION_TIME})
    doc = json.loads(text)
    return ObjectQueryResult(
        meta=[ColumnSchema(name=str(m["name"]), type=str(m["type"])) for m in doc["meta"]],
        data=[list(row) for row in doc["data"]],
    )


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


def _apply_saved_query(row: SavedQueryRow, query: SavedQueryIn) -> None:
    row.scope = query.scope
    row.object = query.object
    row.where = query.where
    row.fields = json.dumps(query.fields) if query.fields is not None else None
    row.order_by = json.dumps([list(OrderBy._make(o)) for o in query.order_by]) if query.order_by else None
    row.cell_view = query.cell_view or None
    row.updated_at = utc_now()


def _validate_saved_query(query: SavedQueryIn) -> None:
    if not query.name.strip():
        raise Invalid("name required")
    if query.scope is not None:
        try:
            parse_scope(query.scope)
        except ValueError as exc:
            raise Invalid(str(exc)) from exc
    if query.where is not None and (err := validate_where_expression(query.where)):
        raise Invalid(err.message)
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
    async with get_sql_session() as session:
        rows = (
            (
                await session.execute(
                    select(SavedQueryRow).where(*predicates).order_by(col(SavedQueryRow.name)).limit(filter.limit)
                )
            )
            .scalars()
            .all()
        )
    return Page[SavedQuery](items=[_row_to_saved_query(r) for r in rows], total=len(rows))


async def save_query(query: SavedQueryIn) -> SavedQuery:
    """Upsert a saved query by ``(tenant, name)``."""
    _validate_saved_query(query)
    tenant_id = get_active_tenant_id()
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(SavedQueryRow).where(SavedQueryRow.tenant_id == tenant_id, SavedQueryRow.name == query.name)
            )
        ).scalar_one_or_none()
        if row is None:
            row = SavedQueryRow(id=get_snowflake_id(), tenant_id=tenant_id, name=query.name, object=query.object)
            session.add(row)
        _apply_saved_query(row, query)
        await session.commit()
        await session.refresh(row)
        return _row_to_saved_query(row)


async def delete_saved_query(name: str) -> Deleted:
    async with get_sql_session() as session:
        row = (
            await session.execute(
                select(SavedQueryRow).where(
                    SavedQueryRow.tenant_id == get_active_tenant_id(), SavedQueryRow.name == name
                )
            )
        ).scalar_one_or_none()
        if row is None:
            raise NotFound(f"Saved query not found: {name}")
        await session.delete(row)
        await session.commit()
    return Deleted(name=name)
