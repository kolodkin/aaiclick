"""Internal API for persistent data-object commands.

Each function runs inside an active ``data_context()`` and reads the
ClickHouse client via the contextvar getter. Returns pydantic view models.

Scope support is intentionally narrow in this migration: all operations
target the ``global`` persistence tier (``p_*`` tables), matching what the
CLI exposed before the migration. Job-scoped listing / filtering is left
to a follow-up once an active orch job is plumbed through.
"""

from __future__ import annotations

from datetime import datetime

from aaiclick.data.data_context import (
    delete_persistent_object,
    delete_persistent_objects,
    get_ch_client,
    list_persistent_objects,
    open_object,
)
from aaiclick.data.view_models import (
    ObjectDetail,
    ObjectView,
    object_to_detail,
)
from aaiclick.view_models import (
    ObjectFilter,
    Page,
    PurgeObjectsRequest,
    PurgeObjectsResult,
)

from .errors import Invalid, NotFound

_GLOBAL_PREFIX = "p_"


async def _fetch_table_metadata(
    tables: list[str],
) -> dict[str, tuple[int | None, int | None, datetime | None]]:
    """Look up ``(row_count, size_bytes, created_at)`` per table in one query."""
    if not tables:
        return {}
    names_lit = ", ".join(f"'{t}'" for t in tables)
    ch = get_ch_client()
    result = await ch.query(
        "SELECT name, total_rows, total_bytes, metadata_modification_time "
        "FROM system.tables "
        f"WHERE database = currentDatabase() AND name IN ({names_lit})"
    )
    return {row[0]: (row[1], row[2], row[3]) for row in result.result_rows}


async def list_objects(filter: ObjectFilter | None = None) -> Page[ObjectView]:
    """Return a page of persistent objects ordered by name.

    Currently lists global-scope persistent objects only (``p_*`` tables).
    ``filter.scope`` is accepted for forward-compatibility; only ``None`` and
    ``"global"`` succeed today — anything else raises ``Invalid``.
    """
    filter = filter or ObjectFilter()
    if filter.scope not in (None, "global"):
        raise Invalid(f"scope={filter.scope!r} not yet supported (global only)")

    names = sorted(await list_persistent_objects())
    if filter.prefix:
        names = [n for n in names if n.startswith(filter.prefix)]

    total = len(names)
    paged = names[: filter.limit]
    tables = [f"{_GLOBAL_PREFIX}{n}" for n in paged]
    metadata = await _fetch_table_metadata(tables)

    items: list[ObjectView] = []
    for name, table in zip(paged, tables, strict=True):
        rows, size, created = metadata.get(table, (None, None, None))
        items.append(
            ObjectView(
                name=name,
                table=table,
                scope="global",
                persistent=True,
                row_count=rows,
                size_bytes=size,
                created_at=created,
            )
        )
    return Page[ObjectView](items=items, total=total)


async def get_object(name: str) -> ObjectDetail:
    """Return full object detail including its schema.

    Raises ``NotFound`` if no global-scope persistent object matches ``name``.
    """
    try:
        obj = await open_object(name)
    except RuntimeError as exc:
        raise NotFound(f"Object not found: {name}") from exc

    metadata = await _fetch_table_metadata([obj.table])
    rows, size, created = metadata.get(obj.table, (None, None, None))
    return object_to_detail(
        obj,
        row_count=rows,
        size_bytes=size,
        created_at=created,
    )


async def delete_object(name: str) -> None:
    """Drop a global-scope persistent object by name.

    Idempotent — dropping a non-existent object is not an error, matching
    ClickHouse's ``DROP TABLE IF EXISTS`` semantics used underneath.
    """
    await delete_persistent_object(name)


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
