"""Internal API for persistent data-object commands.

See docs/designs/viewer.md — "Scope Model".

Each function runs inside an active ``orch_context()`` and reads
the ClickHouse client via the contextvar getter. Registry-backed paths
(``get_object`` -> ``open_object``) additionally need the SQL session that
only orch provides. Returns pydantic view models.

``list_objects`` covers the ``global`` tier (``p_*`` tables), one job's
``j_<id>_*`` tables (``ObjectFilter.job``), or both tiers (``scope="all"``). The other operations
target the ``global`` tier only.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import ColumnElement, and_, or_
from sqlmodel import col, select

from aaiclick.data.object import Object
from aaiclick.data.object.adapters import object_to_detail
from aaiclick.data.object_context import (
    InvalidObjectNameError,
    ObjectNotFoundError,
    delete_persistent_object,
    delete_persistent_objects,
    get_ch_client,
    open_object,
)
from aaiclick.data.scope import GLOBAL_PREFIX, JOB_PREFIX, SCOPE_GLOBAL, SCOPE_JOB, name_from_table, scope_of
from aaiclick.data.view_models import (
    ObjectDetail,
    ObjectView,
)
from aaiclick.orchestration.lifecycle.db_lifecycle import TableRegistry
from aaiclick.orchestration.models import Job
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.view_models import (
    SCOPE_ALL,
    Deleted,
    ObjectFilter,
    Page,
    PurgeObjectsRequest,
    PurgeObjectsResult,
    RefId,
)

from . import jobs as jobs_api
from .errors import Invalid, NotFound


async def _fetch_table_metadata(tables: list[str]) -> dict[str, dict[str, Any]]:
    """Look up per-table stats (row count, size, creation time) in one query.

    Returned dict is keyed by table name; each value is a kwargs dict ready to
    spread into ``ObjectView`` / ``object_to_detail`` (``row_count``,
    ``size_bytes``, ``created_at``).
    """
    if not tables:
        return {}
    names_lit = ", ".join(f"'{t}'" for t in tables)
    ch = get_ch_client()
    result = await ch.query(
        "SELECT name, total_rows, total_bytes, metadata_modification_time "
        "FROM system.tables "
        f"WHERE database = currentDatabase() AND name IN ({names_lit})"
    )
    return {row[0]: {"row_count": row[1], "size_bytes": row[2], "created_at": row[3]} for row in result.result_rows}


async def _registered(clause: ColumnElement[bool]) -> list[tuple[str, int | None, str | None]]:
    """``(table_name, job_id, job_name)`` of the registry rows matching ``clause``."""
    async with get_sql_session() as session:
        result = await session.execute(
            select(TableRegistry.table_name, TableRegistry.job_id, Job.name)
            .outerjoin(Job, col(Job.id) == TableRegistry.job_id)
            .where(clause)
        )
    return [(row[0], row[1], row[2]) for row in result.all()]


async def list_objects(filter: ObjectFilter | None = None) -> Page[ObjectView]:
    """Return a page of persistent objects ordered by name.

    ``filter.job`` (id, or name → latest run) lists that job's ``j_<id>_*``
    tables; ``scope="all"`` lists the ``p_*`` tables plus every job's;
    otherwise ``scope=None`` / ``"global"`` lists the ``p_*`` tables. Any
    other scope raises ``Invalid``. Job-scoped items carry ``job_id`` /
    ``job_name``.
    """
    filter = filter or ObjectFilter()
    is_global = col(TableRegistry.table_name).startswith(GLOBAL_PREFIX, autoescape=True)
    is_job = col(TableRegistry.table_name).startswith(JOB_PREFIX, autoescape=True)
    if filter.job is not None:
        job = await jobs_api.resolve_job(filter.job)
        rows = await _registered(and_(is_job, col(TableRegistry.job_id) == job.id))
    elif filter.scope == SCOPE_JOB:
        raise Invalid("scope='job' requires job (id or name)")
    elif filter.scope == SCOPE_ALL:
        rows = await _registered(or_(is_global, is_job))
    elif filter.scope in (None, SCOPE_GLOBAL):
        rows = await _registered(is_global)
    else:
        raise Invalid(f"scope={filter.scope!r} not supported (global, job or all)")

    # shortcut: loads every matching registry row, then sorts and pages in Python;
    # move prefix/ORDER BY/LIMIT into SQL once the registry holds thousands of rows.
    entries = sorted((name_from_table(row[0]), *row) for row in rows)
    if filter.prefix:
        entries = [e for e in entries if e[0].startswith(filter.prefix)]

    total = len(entries)
    paged = entries[: filter.limit]
    metadata = await _fetch_table_metadata([table for _, table, _, _ in paged])

    items = []
    for name, table, job_id, job_name in paged:
        scope = scope_of(table)
        # A p_* table created inside a job still has a job_id; only j_* belong to one.
        owned = scope == SCOPE_JOB
        items.append(
            ObjectView(
                name=name,
                table=table,
                scope=scope,
                persistent=True,
                job_id=job_id if owned else None,
                job_name=job_name if owned else None,
                **metadata.get(table, {}),
            )
        )
    return Page[ObjectView](items=items, total=total)


async def open_scoped(name: str, job: RefId | None = None) -> Object:
    """Open the persistent object ``name`` — of ``job`` (id or name) when given,
    else of the global tier. Raises ``NotFound`` for a missing object or job."""
    try:
        if job is None:
            return await open_object(name, scope=SCOPE_GLOBAL)
        resolved = await jobs_api.resolve_job(job)
        return await open_object(name, scope=SCOPE_JOB, job_id=resolved.id)
    except (ObjectNotFoundError, ValueError) as exc:
        raise NotFound(f"Object not found: {name}") from exc


async def get_object(name: str, job: RefId | None = None) -> ObjectDetail:
    """Return full object detail including its schema (see ``open_scoped``)."""
    obj = await open_scoped(name, job)

    metadata = await _fetch_table_metadata([obj.table])
    return object_to_detail(obj, **metadata.get(obj.table, {}))


async def delete_object(name: str) -> Deleted:
    """Drop a global-scope persistent object by name.

    Idempotent (``DROP TABLE IF EXISTS``). Raises ``Invalid`` for a malformed name.
    """
    try:
        await delete_persistent_object(name, scope=SCOPE_GLOBAL)
    except InvalidObjectNameError as exc:
        raise Invalid(str(exc)) from exc
    return Deleted(name=name)


async def purge_objects(request: PurgeObjectsRequest) -> PurgeObjectsResult:
    """Drop global-scope persistent objects filtered by creation time.

    Raises ``Invalid`` if neither ``after`` nor ``before`` is set — the
    producer refuses to purge everything unfiltered.
    """
    try:
        deleted = await delete_persistent_objects(
            after=request.after,
            before=request.before,
        )
    except ValueError as exc:
        raise Invalid(str(exc)) from exc
    return PurgeObjectsResult(deleted=deleted)
