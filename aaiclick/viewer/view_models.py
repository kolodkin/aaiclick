"""Request / response models for the viewer verbs (``internal_api.viewer``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, Field

from aaiclick.ai.agents.lineage_tools import ColumnSchema

ORDER_ASC = "ASC"
ORDER_DESC = "DESC"
OrderDir = Literal["ASC", "DESC"]

FMT_JSON = "json"
FMT_CSV = "csv"
QueryFormat = Literal["json", "csv"]

MAX_LIMIT = 1000


class OrderBy(NamedTuple):
    name: str
    dir: OrderDir


class ObjectQuery(BaseModel):
    """What to read from one object: the part of a request that is saved."""

    scope: str = "persistent"  # "persistent" | "job:<id|name>"
    object: str
    fields: list[str] | None = None  # None = every column
    where: str | None = None  # SQL boolean expression over the object's columns
    order_by: list[OrderBy] = Field(default_factory=list)


class ObjectQueryRequest(ObjectQuery):
    limit: int = Field(default=100, ge=1, le=MAX_LIMIT)
    offset: int = Field(default=0, ge=0)
    fmt: QueryFormat = FMT_JSON


class ObjectQueryResult(BaseModel):
    """``meta`` + ``data`` for ``fmt="json"`` (ClickHouse JSONCompact), ``text`` for CSV."""

    meta: list[ColumnSchema] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)
    text: str | None = None


class SavedQueryIn(ObjectQuery):
    name: str
    scope: str | None = "persistent"  # None = the query is offered under every scope
    cell_view: str | None = None  # raw YAML, see aaiclick/viewer/cell_view.py


class SavedQuery(SavedQueryIn):
    updated_at: datetime


class SavedQueryFilter(BaseModel):
    scope: str | None = None  # also matches queries saved with no scope
    object: str | None = None
    limit: int = 50


class DashboardIn(BaseModel):
    name: str
    scope: str = "persistent"
    html: str
    queries: dict[str, ObjectQuery]  # panel name → query


class Dashboard(DashboardIn):
    updated_at: datetime


class DashboardSummary(BaseModel):
    name: str
    scope: str
    updated_at: datetime


class DashboardResults(BaseModel):
    """Column-oriented results per panel — the ``window.queries`` contract."""

    results: dict[str, dict[str, list[Any]]]
    meta: dict[str, list[ColumnSchema]]


class Deleted(BaseModel):
    name: str
