"""REST router for persistent-object commands — paths relative to ``/api/v0``.

Object routes need a ClickHouse client, so they enter
``orch_context(with_ch=True)``. The internal_api functions read the ch client
through the shared contextvar, so no per-request setup beyond the scope
dependency is required.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.internal_api import objects as objects_api
from aaiclick.view_models import (
    ObjectDeleted,
    ObjectFilter,
    Page,
    PurgeObjectsRequest,
    PurgeObjectsResult,
)

from ..deps import orch_scope_with_ch

router = APIRouter(prefix="/objects", tags=["objects"])


@router.get("", response_model=Page[ObjectView])
async def list_objects(
    filter: ObjectFilter = Depends(),
    _scope: None = Depends(orch_scope_with_ch),
) -> Page[ObjectView]:
    return await objects_api.list_objects(filter)


@router.post(":purge", response_model=PurgeObjectsResult)
async def purge_objects(
    request: PurgeObjectsRequest,
    _scope: None = Depends(orch_scope_with_ch),
) -> PurgeObjectsResult:
    return await objects_api.purge_objects(request)


@router.get("/{name}", response_model=ObjectDetail)
async def get_object(
    name: str,
    _scope: None = Depends(orch_scope_with_ch),
) -> ObjectDetail:
    return await objects_api.get_object(name)


@router.delete("/{name}", response_model=ObjectDeleted)
async def delete_object(
    name: str,
    _scope: None = Depends(orch_scope_with_ch),
) -> ObjectDeleted:
    return await objects_api.delete_object(name)
