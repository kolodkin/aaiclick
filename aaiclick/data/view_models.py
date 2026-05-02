"""Data domain view models.

``ObjectView`` and ``ObjectDetail`` are plain Pydantic and know nothing
about the ClickHouse client. The ``Object`` → ``ObjectDetail`` adapter
lives in ``aaiclick.data.object.adapters`` to avoid a circular dependency
through ``object/ingest.py``. ``Schema`` itself is the API-surface type
for table schemas — see ``aaiclick.data.models``.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel

from .models import Schema
from .scope import ObjectScope


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

    table_schema: Schema
    lineage_summary: str | None = None
