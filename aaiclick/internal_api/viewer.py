"""Internal API for the viewer: object-scoped queries, saved queries, dashboards.

Every function runs inside ``orch_context(with_ch=True)`` and the active
tenant. Queries never see a table name: ``open_object`` resolves ``(scope,
object)`` and the Object API builds the SELECT (see docs/designs/viewer.md).
"""

from __future__ import annotations

import json
from typing import NamedTuple

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
from aaiclick.viewer.scope import SCOPE_JOB_KIND, SCOPE_PERSISTENT, ScopeKind, parse_scope
from aaiclick.viewer.view_models import (
    FMT_CSV,
    ObjectQueryRequest,
    ObjectQueryResult,
    OrderBy,
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
