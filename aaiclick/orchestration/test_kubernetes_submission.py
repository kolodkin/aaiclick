"""Tests for the container submission path (factory) with Pod resources."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.orchestration.execution.image_build_task import IMAGE_BUILD_ENTRYPOINT
from aaiclick.orchestration.factories import create_container_job
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.runner_config import ImageBuild


@pytest.mark.usefixtures("fast_poll")
async def test_create_container_job_writes_job_and_entry_task(orch_ctx_no_ch, monkeypatch):
    """The job row carries only the Pod resources snapshot; the image is
    stamped on the entry task and the build task is injected regardless of
    the submitting machine's env."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    source = ImageBuild(git_remote="git://x/repo.git", git_sha="a" * 40, git_branch="main")
    job = await create_container_job(
        name="k8s_submit",
        entrypoint="sample_jobs.entry",
        image_source=source,
        resources={"limits": {"cpu": "1"}},
        entry_type="module",
    )
    assert job.resources == {"limits": {"cpu": "1"}}

    async with get_sql_session() as session:
        tasks = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    by_entry = {t.entrypoint: t for t in tasks}
    assert set(by_entry) == {"sample_jobs.entry", IMAGE_BUILD_ENTRYPOINT}
    assert by_entry["sample_jobs.entry"].image_source is not None
    assert by_entry["sample_jobs.entry"].image_source["type"] == "build"
