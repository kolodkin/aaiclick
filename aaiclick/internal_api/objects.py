"""Internal API for persistent data-object commands.

Each function runs inside an active ``orch_context(with_ch=True)`` and reads
the ClickHouse client via the contextvar getter. Registry-backed paths
(``get_object`` -> ``open_object``) additionally need the SQL session that
only orch provides. Returns pydantic view models.

``list_objects`` covers the ``global`` tier (``p_*`` tables) and, given
``ObjectFilter.job``, one job's ``j_<id>_*`` tables. The other operations
target the ``global`` tier only.
"""

from __future__ import annotations

from typing import Any

from aaiclick.data.data_context import (
    ObjectNotFoundError,
    delete_persistent_object,
    delete_persistent_objects,
    get_ch_client,
    list_job_tables,
    list_persistent_tables,
    open_object,
)
from aaiclick.data.object import Object
from aaiclick.data.object.adapters import object_to_detail
from aaiclick.data.scope import SCOPE_GLOBAL, SCOPE_JOB, ObjectScope, name_from_table
from aaiclick.data.view_models import (
    ObjectDetail,
    ObjectView,
)
from aaiclick.view_models import (
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


async def list_objects(filter: ObjectFilter | None = None) -> Page[ObjectView]:
    """Return a page of persistent objects ordered by name.

    ``filter.job`` (id, or name → latest run) lists that job's ``j_<id>_*``
    tables; otherwise ``scope=None`` / ``"global"`` lists the tenant's ``p_*``
    tables. Any other scope raises ``Invalid``.
    """
    filter = filter or ObjectFilter()
    scope: ObjectScope
    if filter.job is not None:
        job = await jobs_api.resolve_job(filter.job)
        tables = await list_job_tables(job.id)
        scope = SCOPE_JOB
    elif filter.scope == SCOPE_JOB:
        raise Invalid("scope='job' requires job (id or name)")
    elif filter.scope in (None, SCOPE_GLOBAL):
        tables = await list_persistent_tables()
        scope = SCOPE_GLOBAL
    else:
        raise Invalid(f"scope={filter.scope!r} not supported (global or job)")

    pairs = sorted((name_from_table(t), t) for t in tables)
    if filter.prefix:
        pairs = [(n, t) for n, t in pairs if n.startswith(filter.prefix)]

    total = len(pairs)
    paged = pairs[: filter.limit]
    metadata = await _fetch_table_metadata([t for _, t in paged])

    items = [
        ObjectView(
            name=name,
            table=table,
            scope=scope,
            persistent=True,
            **metadata.get(table, {}),
        )
        for name, table in paged
    ]
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

    Idempotent — dropping a non-existent object is not an error, matching
    ClickHouse's ``DROP TABLE IF EXISTS`` semantics used underneath.
    """
    await delete_persistent_object(name, scope=SCOPE_GLOBAL)
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
