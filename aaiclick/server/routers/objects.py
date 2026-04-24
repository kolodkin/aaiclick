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
from ..errors import problem_responses

router = APIRouter(prefix="/objects", tags=["objects"], dependencies=[Depends(orch_scope_with_ch)])


@router.get("", response_model=Page[ObjectView], responses=problem_responses(422))
async def list_objects(filter: ObjectFilter = Depends()) -> Page[ObjectView]:
    return await objects_api.list_objects(filter)


@router.post(":purge", response_model=PurgeObjectsResult, responses=problem_responses(422))
async def purge_objects(request: PurgeObjectsRequest) -> PurgeObjectsResult:
    return await objects_api.purge_objects(request)


@router.get("/{name}", response_model=ObjectDetail, responses=problem_responses(404))
async def get_object(name: str) -> ObjectDetail:
    return await objects_api.get_object(name)


@router.delete("/{name}", response_model=ObjectDeleted)
async def delete_object(name: str) -> ObjectDeleted:
    return await objects_api.delete_object(name)
