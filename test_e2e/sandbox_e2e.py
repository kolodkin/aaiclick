"""The sandbox flow both runner e2es drive: submit a file into an empty
remote, run one background-worker poll, run every job it created, then submit
again to show commits stack. Only the runner under test differs."""

from __future__ import annotations

from pathlib import Path

from job_wait import run_worker_until_all_done

from aaiclick.internal_api.sandbox import submit_sandbox_file
from aaiclick.orchestration.background.background_worker import BackgroundWorker
from aaiclick.orchestration.docker_config import resolve_remote_head
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import JOB_COMPLETED, SANDBOX_SUBMITTED, SandboxFile
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.sandbox.paths import module_parts
from aaiclick.sandbox.repo import SandboxRepo, sandbox_repo_override
from aaiclick.view_models import SubmitSandboxRequest

_SOURCE = Path(__file__).parent / "fixtures" / "sandbox_job" / "sandbox_jobs.py"


async def _head(remote: str) -> str:
    return (await resolve_remote_head(remote, "main")).sha


async def run_sandbox_submission(remote: str, clone_dir: Path, *, max_tasks: int) -> None:
    source = _SOURCE.read_text()
    request = SubmitSandboxRequest(name="demo", source=source)
    repo = SandboxRepo(remote, clone_dir)
    with sandbox_repo_override(repo):
        view = await submit_sandbox_file(request, user=None)
    assert await _head(remote) == view.git_sha

    # One poll step, not the worker loop: the loop would race this call for
    # the same pending row, and every job it created would compete for the
    # mp worker's task budget below.
    bg_worker = BackgroundWorker()
    try:
        await bg_worker._run_sandbox_files()
    finally:
        await bg_worker.stop()

    async with get_sql_session() as session:
        row = await session.get(SandboxFile, view.id)
    assert row is not None
    assert row.status == SANDBOX_SUBMITTED, row.error
    assert row.job_ids is not None and len(row.job_ids) == 2

    module_name = module_parts(row.path).module_name
    jobs = await run_worker_until_all_done(f"{module_name}.first", f"{module_name}.second", max_tasks=max_tasks)
    for completed in jobs:
        assert completed.status == JOB_COMPLETED, completed.error
        probe = next(t for t in await get_tasks_for_job(completed.id) if t.entrypoint.endswith(".probe"))
        assert probe.result == {"native_value": {"n": 1}}, (probe.result, probe.error)

    # A second submission stacks on the first in the remote. It stays pending:
    # no further poll step runs, so it creates no jobs.
    with sandbox_repo_override(repo):
        second = await submit_sandbox_file(request, user=None)
    assert second.git_sha != view.git_sha
    assert await _head(remote) == second.git_sha
