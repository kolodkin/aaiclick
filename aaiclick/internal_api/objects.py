"""Internal API for persistent data-object commands.

Each function runs inside an active ``orch_context(with_ch=True)`` and reads
the ClickHouse client via the contextvar getter. Registry-backed paths
(``get_object`` -> ``open_object``) additionally need the SQL session that
only orch provides. Returns pydantic view models.

Scope support is intentionally narrow in this migration: all operations
target the ``global`` persistence tier, matching what the CLI exposed
before the migration. Job-scoped listing / filtering is left
to a follow-up once an active orch job is plumbed through.
"""

from __future__ import annotations

from typing import Any

from aaiclick.data.data_context import (
    ObjectNotFoundError,
    delete_persistent_object,
    delete_persistent_objects,
    get_ch_client,
    list_persistent_entries,
    open_object,
)
from aaiclick.data.object.adapters import object_to_detail
from aaiclick.data.scope import SCOPE_GLOBAL
from aaiclick.data.view_models import (
    ObjectDetail,
    ObjectView,
)
from aaiclick.view_models import (
    ObjectDeleted,
    ObjectFilter,
    Page,
    PurgeObjectsRequest,
    PurgeObjectsResult,
)

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

    Currently lists global-scope persistent objects only. ``filter.scope``
    is accepted for forward-compatibility; only ``None`` and ``"global"``
    succeed today — anything else raises ``Invalid``.
    """
    filter = filter or ObjectFilter()
    if filter.scope not in (None, SCOPE_GLOBAL):
        raise Invalid(f"scope={filter.scope!r} not yet supported (global only)")

    entries = await list_persistent_entries()
    if filter.prefix:
        entries = [e for e in entries if e.name.startswith(filter.prefix)]

    total = len(entries)
    paged = entries[: filter.limit]
    metadata = await _fetch_table_metadata([e.table for e in paged])

    items = [
        ObjectView(
            name=entry.name,
            table=entry.table,
            scope=SCOPE_GLOBAL,
            persistent=True,
            **metadata.get(entry.table, {}),
        )
        for entry in paged
    ]
    return Page[ObjectView](items=items, total=total)


async def get_object(name: str) -> ObjectDetail:
    """Return full object detail including its schema.

    Raises ``NotFound`` if no global-scope persistent object matches ``name``.
    """
    try:
        obj = await open_object(name, scope=SCOPE_GLOBAL)
    except ObjectNotFoundError as exc:
        raise NotFound(f"Object not found: {name}") from exc

    metadata = await _fetch_table_metadata([obj.table])
    return object_to_detail(obj, **metadata.get(obj.table, {}))


async def delete_object(name: str) -> ObjectDeleted:
    """Drop a global-scope persistent object by name.

    Idempotent — dropping a non-existent object is not an error, matching
    ClickHouse's ``DROP TABLE IF EXISTS`` semantics used underneath.
    """
    await delete_persistent_object(name, scope=SCOPE_GLOBAL)
    return ObjectDeleted(name=name)


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
