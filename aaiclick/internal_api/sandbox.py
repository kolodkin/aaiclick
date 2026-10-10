"""Internal API for the sandbox: submit a file, list and read submissions.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from aaiclick.auth.models import User
from aaiclick.datetime_utils import utc_now
from aaiclick.orchestration.models import SANDBOX_PENDING, SandboxFile
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.view_models import SandboxFileDetailView, SandboxFileView, sandbox_file_to_view
from aaiclick.sandbox.parse import SandboxSourceError, find_job_functions
from aaiclick.sandbox.paths import submission_path
from aaiclick.sandbox.repo import (
    Author,
    SandboxGitError,
    SandboxPushRejected,
    SandboxRepo,
    get_sandbox_repo,
    sandbox_repo_for,
)
from aaiclick.view_models import Page, SandboxConfigView, SandboxFileFilter, SubmitSandboxRequest

from .errors import Conflict, Invalid, NotFound, SandboxUnavailable
from .pagination import paginate


def _require_repo() -> SandboxRepo:
    repo = get_sandbox_repo()
    if repo is None:
        raise NotFound("sandbox is not enabled: set AAICLICK_SANDBOX to a git remote")
    return repo


def sandbox_config(*, admin: bool) -> SandboxConfigView:
    repo = get_sandbox_repo()
    return SandboxConfigView(enabled=repo is not None, remote=repo.remote if repo is not None and admin else None)


async def submit_sandbox_file(request: SubmitSandboxRequest, *, user: User | None) -> SandboxFileView:
    """Scan ``request.source`` for ``@job`` functions, commit it to the sandbox
    repo and record the submission as ``pending``.

    ``user`` is ``None`` in local mode (synthetic admin, no users row).

    Raises ``Invalid`` for a syntax error or a file without a ``@job``,
    ``Conflict`` when the push lost the race twice, ``SandboxUnavailable``
    for any other git failure, ``NotFound`` when the sandbox is disabled.
    """
    repo = _require_repo()
    try:
        job_names = find_job_functions(request.source)
    except SandboxSourceError as exc:
        raise Invalid(str(exc)) from exc

    username = user.username if user is not None else None
    email = user.email if user is not None else None
    author_name = username or "local"
    author = Author(author_name, email or f"{author_name}@sandbox")
    try:
        committed = await repo.commit_file(
            submission_path(request.name, utc_now()),
            request.source,
            author=author,
            message=f"sandbox: {request.name}",
        )
    except SandboxPushRejected as exc:
        raise Conflict(f"sandbox push rejected twice: {exc}") from exc
    except SandboxGitError as exc:
        raise SandboxUnavailable(str(exc)) from exc

    row = SandboxFile(
        name=request.name,
        path=committed.path,
        git_remote=repo.remote,
        git_sha=committed.sha,
        job_names=job_names,
        status=SANDBOX_PENDING,
        submitted_by=user.id if user is not None else None,
    )
    async with get_sql_session() as session:
        session.add(row)
        await session.commit()
        await session.refresh(row)
    return sandbox_file_to_view(row, username)


async def _views(session: AsyncSession, rows: list[SandboxFile]) -> list[SandboxFileView]:
    """``rows`` as views, each with its submitter's username (``None`` for
    local mode's synthetic admin)."""
    ids = {r.submitted_by for r in rows if r.submitted_by is not None}
    names: dict[int, str] = {}
    if ids:
        result = await session.execute(select(User.id, User.username).where(col(User.id).in_(ids)))
        names = {int(user_id): str(name) for user_id, name in result.all()}  # noqa: C416 - typed narrowing
    return [sandbox_file_to_view(r, names.get(r.submitted_by) if r.submitted_by is not None else None) for r in rows]


async def list_sandbox_files(filter: SandboxFileFilter | None = None) -> Page[SandboxFileView]:
    """Submissions newest first."""
    filter = filter or SandboxFileFilter()
    page = await paginate(
        SandboxFile,
        order_by=col(SandboxFile.created_at).desc(),
        limit=filter.limit,
        offset=filter.offset,
    )
    async with get_sql_session() as session:
        items = await _views(session, page.rows)
    return Page[SandboxFileView](items=items, total=page.total)


async def get_sandbox_file(file_id: int) -> SandboxFileDetailView:
    """One submission plus its committed source, read from the remote the
    row was committed to — not necessarily today's ``AAICLICK_SANDBOX``."""
    async with get_sql_session() as session:
        row = await session.get(SandboxFile, file_id)
        if row is None:
            raise NotFound(f"sandbox file {file_id} not found")
        (view,) = await _views(session, [row])
    try:
        source = await sandbox_repo_for(row.git_remote).read_file(row.path, row.git_sha)
    except SandboxGitError as exc:
        raise SandboxUnavailable(str(exc)) from exc
    return SandboxFileDetailView(**view.model_dump(), source=source)
