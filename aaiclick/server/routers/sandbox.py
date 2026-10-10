from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.auth import store
from aaiclick.auth.models import SCOPE_ADMIN, User
from aaiclick.internal_api import sandbox as sandbox_api
from aaiclick.orchestration.view_models import SandboxFileDetailView, SandboxFileView
from aaiclick.view_models import Page, SandboxConfigView, SandboxFileFilter, SubmitSandboxRequest

from ..auth import Principal, principal_to_scope, require_principal, require_session
from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/sandbox", tags=["sandbox"], dependencies=[Depends(orch_scope)])


async def _current_user(principal: Principal = Depends(require_session)) -> User | None:
    """The caller's users row, or ``None`` for local mode's synthetic admin.

    ``require_session``: every role may submit, but only from a signed-in
    session — the sandbox is a browser feature, not an API-token surface."""
    if principal.user_id is None:
        return None
    return await store.get_user_by_id(principal.user_id)


@router.get("/config", response_model=SandboxConfigView)
async def sandbox_config(principal: Principal = Depends(require_principal)) -> SandboxConfigView:
    return sandbox_api.sandbox_config(admin=principal_to_scope(principal) == SCOPE_ADMIN)


@router.post(
    "",
    response_model=SandboxFileView,
    status_code=201,
    responses=problem_responses(404, 409, 422, 502),
)
async def submit_sandbox_file(
    request: SubmitSandboxRequest, user: User | None = Depends(_current_user)
) -> SandboxFileView:
    """Any role may submit — the sandbox is open to every signed-in user."""
    return await sandbox_api.submit_sandbox_file(request, user=user)


@router.get("", response_model=Page[SandboxFileView])
async def list_sandbox_files(filter: SandboxFileFilter = Depends()) -> Page[SandboxFileView]:
    return await sandbox_api.list_sandbox_files(filter)


@router.get("/{file_id}", response_model=SandboxFileDetailView, responses=problem_responses(404, 502))
async def get_sandbox_file(file_id: int) -> SandboxFileDetailView:
    return await sandbox_api.get_sandbox_file(file_id)
