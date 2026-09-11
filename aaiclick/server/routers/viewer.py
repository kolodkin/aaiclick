from __future__ import annotations

from fastapi import APIRouter, Depends, Response

from aaiclick.internal_api import viewer as viewer_api
from aaiclick.view_models import Deleted, Page
from aaiclick.viewer.view_models import (
    FMT_CSV,
    Dashboard,
    DashboardIn,
    DashboardResults,
    DashboardSummary,
    ObjectQueryRequest,
    SavedQuery,
    SavedQueryFilter,
    SavedQueryIn,
)

from ..deps import orch_scope_with_ch
from ..errors import problem_responses

router = APIRouter(prefix="/viewer", tags=["viewer"], dependencies=[Depends(orch_scope_with_ch)])


class SavedQueryBody(SavedQueryIn):
    name: str = ""  # taken from the path


class DashboardBody(DashboardIn):
    name: str = ""  # taken from the path


@router.post(
    "/query",
    responses={
        200: {
            "description": "ClickHouse's own output, verbatim: `JSONCompact` (`{meta, data, rows, statistics}`) "
            "for `fmt=json`, `CSVWithNames` for `fmt=csv`.",
            "content": {"application/json": {}, "text/csv": {}},
        },
        **problem_responses(404, 422),
    },
)
async def query_object(request: ObjectQueryRequest) -> Response:
    media_type = "text/csv" if request.fmt == FMT_CSV else "application/json"
    return Response(content=await viewer_api.query_object_bytes(request), media_type=media_type)


@router.get("/queries", response_model=Page[SavedQuery])
async def list_saved_queries(filter: SavedQueryFilter = Depends()) -> Page[SavedQuery]:
    return await viewer_api.list_saved_queries(filter)


@router.put("/queries/{name}", response_model=SavedQuery, responses=problem_responses(422))
async def save_query(name: str, body: SavedQueryBody) -> SavedQuery:
    return await viewer_api.save_query(SavedQueryIn(**{**body.model_dump(), "name": name}))


@router.delete("/queries/{name}", response_model=Deleted, responses=problem_responses(404))
async def delete_saved_query(name: str) -> Deleted:
    return await viewer_api.delete_saved_query(name)


@router.get("/dashboards", response_model=Page[DashboardSummary])
async def list_dashboards() -> Page[DashboardSummary]:
    return await viewer_api.list_dashboards()


@router.get("/dashboards/{name}", response_model=Dashboard, responses=problem_responses(404))
async def get_dashboard(name: str) -> Dashboard:
    return await viewer_api.get_dashboard(name)


@router.put("/dashboards/{name}", response_model=Dashboard, responses=problem_responses(422))
async def save_dashboard(name: str, body: DashboardBody) -> Dashboard:
    return await viewer_api.save_dashboard(DashboardIn(**{**body.model_dump(), "name": name}))


@router.delete("/dashboards/{name}", response_model=Deleted, responses=problem_responses(404))
async def delete_dashboard(name: str) -> Deleted:
    return await viewer_api.delete_dashboard(name)


@router.post("/dashboards/{name}:run", response_model=DashboardResults, responses=problem_responses(404, 422))
async def run_dashboard(name: str) -> DashboardResults:
    return await viewer_api.run_dashboard(name)
