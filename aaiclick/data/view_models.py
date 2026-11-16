"""Data domain view models.

``ObjectView`` and ``ObjectDetail`` are plain Pydantic and know nothing
about the ClickHouse client. The ``Object`` → ``ObjectDetail`` adapter
lives in ``aaiclick.data.object.adapters`` to avoid a circular dependency
through ``object/ingest.py``. ``Schema`` itself is the API-surface type
for table schemas — see ``aaiclick.data.models``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from ..api_model import ApiModel
from ..log_models import SnowflakeId
from .models import Schema
from .scope import ObjectScope

OBJECT_KIND_OBJECT = "object"
OBJECT_KIND_VALUE = "value"
OBJECT_KIND_REF = "ref"
OBJECT_KIND_ALL = "all"
ObjectKind = Literal["object", "value", "ref"]
ObjectKindFilter = Literal["object", "value", "ref", "all"]

DIRECTION_IN = "in"
DIRECTION_OUT = "out"
ValueDirection = Literal["in", "out"]


class ColumnSchema(ApiModel):
    """One result column: its name and ClickHouse type string."""

    name: str
    type: str


class ObjectView(ApiModel):
    """Compact object representation used by list endpoints."""

    name: str
    table: str
    scope: ObjectScope
    persistent: bool
    job_id: SnowflakeId | None = None
    job_name: str | None = None
    row_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime | None = None
    # Task rows (kind "value" / "ref") — see aaiclick/internal_api/task_values.py.
    # There ``name`` is the row's address and ``label`` its display name.
    kind: ObjectKind = OBJECT_KIND_OBJECT
    label: str | None = None
    task_id: SnowflakeId | None = None
    direction: ValueDirection | None = None
    ref_type: str | None = None
    preview: str | None = None
    target: str | None = None


class ObjectDetail(ObjectView):
    """Full object representation used by ``GET /objects/{name}``."""

    table_schema: Schema
    lineage_summary: str | None = None
