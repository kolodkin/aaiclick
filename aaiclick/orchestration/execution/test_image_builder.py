"""Tests for the on-demand image build seam (ensure_image)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from ...snowflake import get_snowflake_id
from .. import docker_config
from ..factories import create_built_job, create_task
from ..models import BUILD_BUILDING, BUILD_FAILED, BUILD_READY, TASK_COMPLETED, BuildTask, Job, Task
from ..orch_context import get_sql_session
from ..runner_config import BUILD_TASK_ENTRYPOINT, DockerRunner, ImageBuild, ImagePrebuilt
from . import image_builder
from .claiming import claim_next_task, update_task_status
from .execution_worker_context import set_current_task_info
from .image_builder import BuildFailed, build_job_image, ensure_built_image, ensure_image


def _source(sha="a" * 40) -> ImageBuild:
    return ImageBuild(git_remote="git@x:r.git", git_sha=sha)


async def test_ensure_image_builds_once_and_marks_ready(orch_ctx_no_ch, monkeypatch):
    build = AsyncMock()
    monkeypatch.setattr(image_builder, "build_image_to_tag", build)
    source = _source()

    ensured = await ensure_image(source, holder_id=1)

    build.assert_awaited_once()
    assert ensured.image_tag == docker_config.compute_image_tag("a" * 40)
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == ensured.build_task_id))).scalar_one()
    assert row.status == BUILD_READY


async def test_ensure_image_reuses_ready_row_without_building(orch_ctx_no_ch, monkeypatch):
    build = AsyncMock()
    monkeypatch.setattr(image_builder, "build_image_to_tag", build)
    source = _source()

    first = await ensure_image(source, holder_id=1)
    second = await ensure_image(source, holder_id=2)

    build.assert_awaited_once()  # only the first call built
    assert second.build_task_id == first.build_task_id


async def test_ensure_image_raises_after_exhausting_retries(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setattr(image_builder, "BUILD_POLL_INTERVAL", 0.0)
    monkeypatch.setattr(image_builder, "build_image_to_tag", AsyncMock(side_effect=RuntimeError("boom")))
    source = _source()

    with pytest.raises(BuildFailed):
        await ensure_image(source, holder_id=1)

    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.git_sha == "a" * 40))).scalar_one()
    assert row.status == BUILD_FAILED
    assert row.attempts == row.max_retries + 1


async def test_ensure_built_image_stamps_build_task_id_on_task(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setattr(image_builder, "build_image_to_tag", AsyncMock())
    source = _source(sha="d" * 40)
    # The task needs a real job row so its job_id FK is satisfied on Postgres
    # (SQLite doesn't enforce foreign keys).
    job = Job(id=get_snowflake_id(), name="j", run_type="MANUAL")
    task = create_task("mod.fn")
    task.job_id = job.id
    async with get_sql_session() as session:
        session.add(job)
        session.add(task)
        await session.commit()

    tag = await ensure_built_image(task.id, source, holder_id=7)

    assert tag == docker_config.compute_image_tag("d" * 40)
    async with get_sql_session() as session:
        row = (await session.execute(select(Task).where(Task.id == task.id))).scalar_one()
    assert row.build_task_id is not None


async def test_finish_only_updates_row_for_current_lease_holder(orch_ctx_no_ch):
    source = _source(sha="e" * 40)
    build_task = BuildTask(
        id=1,
        image_key=docker_config.image_key(source),
        image_tag=docker_config.compute_image_tag(source.git_sha),
        git_remote=source.git_remote,
        git_sha=source.git_sha,
        status=BUILD_BUILDING,
        holder_execution_worker_id=1,
    )
    async with get_sql_session() as session:
        session.add(build_task)
        await session.commit()

    # A stale/zombie worker (holder_id=2) no longer holds the lease (worker 1
    # does), so its write must be rejected and the row left unchanged.
    await image_builder._finish(build_task.id, 2, status=BUILD_READY, error=None)
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == build_task.id))).scalar_one()
    assert row.status == BUILD_BUILDING

    # The actual lease holder (holder_id=1) can record the outcome.
    await image_builder._finish(build_task.id, 1, status=BUILD_READY, error=None)
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == build_task.id))).scalar_one()
    assert row.status == BUILD_READY


def _build_runner(sha: str) -> DockerRunner:
    return DockerRunner(image=ImageBuild(git_remote="git@x:r.git", git_sha=sha))


async def _job_tasks(job_id: int) -> dict[str, Task]:
    async with get_sql_session() as session:
        tasks = (await session.execute(select(Task).where(Task.job_id == job_id))).scalars().all()
    return {t.entrypoint: t for t in tasks}


async def test_build_job_image_marks_ready_and_stamps_build_task_id(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setattr(image_builder, "build_image_to_tag", AsyncMock())
    job = await create_built_job(name="bj", entrypoint="mod.fn", runner=_build_runner("1" * 40))
    build_task = (await _job_tasks(job.id))[BUILD_TASK_ENTRYPOINT]
    set_current_task_info(task_id=build_task.id, job_id=job.id)

    await build_job_image(job.id)

    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.git_sha == "1" * 40))).scalar_one()
        task_row = (await session.execute(select(Task).where(Task.id == build_task.id))).scalar_one()
    assert row.status == BUILD_READY
    assert task_row.build_task_id == row.id


async def test_build_job_image_rejects_job_without_build_source(orch_ctx_no_ch):
    runner = DockerRunner(image=ImagePrebuilt(image_tag="python:3.12"))
    job = await create_built_job(name="pj", entrypoint="mod.fn", runner=runner)
    entry_task = (await _job_tasks(job.id))["mod.fn"]
    set_current_task_info(task_id=entry_task.id, job_id=job.id)

    with pytest.raises(ValueError, match="no build image source"):
        await build_job_image(job.id)


async def test_build_task_gates_entry_task_claiming(orch_ctx_no_ch):
    """The injected build task is claimed first; the entry task stays
    unclaimable until the build task completes, so no worker slot is spent
    waiting on an in-flight image build."""
    await create_built_job(name="gate", entrypoint="mod.entry", runner=_build_runner("2" * 40))

    first = await claim_next_task(execution_worker_id=1)
    assert first is not None
    assert first.entrypoint == BUILD_TASK_ENTRYPOINT

    # Entry task depends on the (still-running) build task.
    assert await claim_next_task(execution_worker_id=2) is None

    await update_task_status(first.id, TASK_COMPLETED)
    second = await claim_next_task(execution_worker_id=2)
    assert second is not None
    assert second.entrypoint == "mod.entry"
