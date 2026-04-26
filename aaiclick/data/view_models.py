"""Data domain view models.

``ColumnView``, ``SchemaView``, ``ObjectView``, and ``ObjectDetail`` are
plain pydantic and know nothing about the ClickHouse client. The
``Schema`` ↔ ``SchemaView`` adapters live here too. ``Object`` →
``ObjectDetail`` lives in ``aaiclick.data.object.adapters`` to avoid a
circular dependency through ``object/ingest.py``.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field

from .models import (
    FIELDTYPE_ARRAY,
    FIELDTYPE_SCALAR,
    ColumnFieldtype,
    ColumnInfo,
    EngineType,
    Fieldtype,
    Schema,
)
from .scope import GLOBAL_PREFIX, JOB_SCOPED_RE, ObjectScope


class ColumnView(BaseModel):
    """Single column description used inside ``SchemaView``."""

    name: str
    type: str
    nullable: bool = False
    array_depth: int = 0
    low_cardinality: bool = False
    fieldtype: ColumnFieldtype = FIELDTYPE_SCALAR
    default: str | None = None  # ClickHouse DEFAULT expression


class SchemaView(BaseModel):
    """Table schema representation used inside ``ObjectDetail``."""

    columns: list[ColumnView] = Field(default_factory=list)
    order_by: str | None = None
    engine: EngineType | None = None
    fieldtype: Fieldtype = FIELDTYPE_SCALAR


class ObjectView(BaseModel):
    """Compact object representation used by list endpoints."""

    name: str
    table: str
    scope: ObjectScope
    persistent: bool
    row_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime | None = None


class ObjectDetail(ObjectView):
    """Full object representation used by ``GET /objects/{name}``."""

    table_schema: SchemaView
    lineage_summary: str | None = None


def _object_name_from_table(table: str) -> str:
    """Return the user-visible name for a table.

    - ``p_<name>`` → ``<name>`` (global-scope persistent)
    - ``j_<job_id>_<name>`` → ``<name>`` (job-scoped persistent)
    - ``t_<snowflake>`` → the table name itself (unnamed temp)
    """
    if table.startswith(GLOBAL_PREFIX):
        return table[len(GLOBAL_PREFIX) :]
    if JOB_SCOPED_RE.match(table):
        return table.split("_", 2)[2]
    return table


def column_info_to_view(name: str, info: ColumnInfo, *, schema_fieldtype: Fieldtype | None = None) -> ColumnView:
    """Adapt a ``ColumnInfo`` to its ``ColumnView``.

    When ``info.fieldtype`` is the SCALAR default (older call sites that don't
    set it explicitly), fall back to a fieldtype derived from the enclosing
    schema: DICT object → SCALAR col, ARRAY object → ARRAY col, SCALAR → SCALAR.
    """
    fieldtype: ColumnFieldtype
    if info.fieldtype != FIELDTYPE_SCALAR:
        fieldtype = info.fieldtype
    elif schema_fieldtype == FIELDTYPE_ARRAY:
        fieldtype = FIELDTYPE_ARRAY
    else:
        fieldtype = FIELDTYPE_SCALAR
    return ColumnView(
        name=name,
        type=info.type,
        nullable=info.nullable,
        array_depth=int(info.array),
        low_cardinality=info.low_cardinality,
        fieldtype=fieldtype,
        default=info.default,
    )


def schema_to_view(schema: Schema) -> SchemaView:
    return SchemaView(
        columns=[
            column_info_to_view(name, info, schema_fieldtype=schema.fieldtype) for name, info in schema.columns.items()
        ],
        order_by=schema.order_by,
        engine=schema.engine,
        fieldtype=schema.fieldtype,
    )


def view_to_schema(view: SchemaView, *, table: str) -> Schema:
    """Hydrate a Schema dataclass from a persisted SchemaView.

    The inverse of :func:`schema_to_view`. Used by ``_get_table_schema``
    to reconstruct the runtime Schema from ``table_registry.schema_doc``.
    """
    columns = {
        cv.name: ColumnInfo(
            type=cv.type,
            nullable=cv.nullable,
            array=cv.array_depth,
            low_cardinality=cv.low_cardinality,
            fieldtype=cv.fieldtype,
            default=cv.default,
        )
        for cv in view.columns
    }
    return Schema(
        fieldtype=view.fieldtype,
        columns=columns,
        table=table,
        order_by=view.order_by,
        engine=view.engine,
    )
