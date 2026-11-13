"""Request / response models for the viewer verbs (``internal_api.viewer``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, NamedTuple

from pydantic import Field

from aaiclick.api_model import ApiModel
from aaiclick.data.view_models import ColumnSchema

ORDER_ASC = "ASC"
ORDER_DESC = "DESC"
OrderDir = Literal["ASC", "DESC"]

FMT_JSON = "json"
FMT_CSV = "csv"
QueryFormat = Literal["json", "csv"]

MAX_LIMIT = 1000
MAX_DASHBOARD_PANELS = 50


class OrderBy(NamedTuple):
    name: str
    dir: OrderDir


class ObjectQuery(ApiModel):
    """What to read from one object — a dashboard panel, or the stored part of a request."""

    object: str
    fields: list[str] | None = None  # None = every column
    where: str | None = None  # SQL boolean expression over the object's columns
    order_by: list[OrderBy] = Field(default_factory=list)


class ObjectQueryRequest(ObjectQuery):
    scope: str = "persistent"  # "persistent" | "job:<id|name>"
    limit: int = Field(default=100, ge=1, le=MAX_LIMIT)
    offset: int = Field(default=0, ge=0)
    fmt: QueryFormat = FMT_JSON


class ObjectQueryResult(ApiModel):
    """``meta`` + ``data`` for ``fmt="json"`` (ClickHouse JSONCompact), ``text`` for CSV."""

    meta: list[ColumnSchema] = Field(default_factory=list)
    data: list[list[Any]] = Field(default_factory=list)
    text: str | None = None


PARAM_VALUE = "value"
PARAM_IDENTIFIER = "identifier"
PARAM_DIMENSION = "dimension"
ParamKind = Literal["value", "identifier", "dimension"]


class DashboardParam(ApiModel):
    """A dashboard selector, in QueryView's spec; the SPA kernel interprets it."""

    name: str
    kind: ParamKind = PARAM_VALUE
    options: list[str | int | float] | None = None
    options_sql: str | None = None
    default: Literal["none"] | None = None  # "none": start unselected


class DashboardIn(ApiModel):
    name: str
    scope: str = "persistent"
    html: str
    # panel name → query, run under the dashboard's scope; capped since every panel runs at once
    queries: dict[str, ObjectQuery] = Field(max_length=MAX_DASHBOARD_PANELS)
    # {name} placeholders in a panel's where take the selected value
    params: list[DashboardParam] = Field(default_factory=list, max_length=MAX_DASHBOARD_PANELS)


class Dashboard(DashboardIn):
    # uncapped: the caps guard writes, not dashboards already stored
    queries: dict[str, ObjectQuery]
    params: list[DashboardParam] = Field(default_factory=list)
    updated_at: datetime


class RunQueriesRequest(ApiModel):
    """Panels to run under one scope — a dashboard's queries with its params substituted."""

    scope: str = "persistent"
    queries: dict[str, ObjectQuery] = Field(max_length=MAX_DASHBOARD_PANELS)


class DashboardSummary(ApiModel):
    name: str
    scope: str
    updated_at: datetime


class DashboardResults(ApiModel):
    """Column-oriented results per panel — the ``window.queries`` contract."""

    results: dict[str, dict[str, list[Any]]]
    meta: dict[str, list[ColumnSchema]]
