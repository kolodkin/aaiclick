"""Object → view_models adapters.

Lives next to ``Object`` (rather than in ``aaiclick/data/view_models.py``)
so the view_models module stays free of any dependency on the runtime
``Object`` class — that import would form a cycle through
``aaiclick/data/object/ingest.py`` which already imports SchemaView /
view_to_schema from view_models.
"""

from __future__ import annotations

from datetime import datetime

from ..scope import scope_of
from ..view_models import ObjectDetail, _object_name_from_table, schema_to_view
from .object import Object


def object_to_detail(
    obj: Object,
    *,
    row_count: int | None = None,
    size_bytes: int | None = None,
    created_at: datetime | None = None,
    lineage_summary: str | None = None,
) -> ObjectDetail:
    """Adapt an :class:`Object` to a detail-form view.

    ``row_count``, ``size_bytes``, and ``created_at`` come from
    ``system.tables`` and are supplied by the caller (``internal_api``)
    because ``Object`` itself does not carry them.
    """
    return ObjectDetail(
        name=_object_name_from_table(obj.table),
        table=obj.table,
        scope=scope_of(obj.table),
        persistent=obj.persistent,
        row_count=row_count,
        size_bytes=size_bytes,
        created_at=created_at,
        table_schema=schema_to_view(obj.schema),
        lineage_summary=lineage_summary,
    )
