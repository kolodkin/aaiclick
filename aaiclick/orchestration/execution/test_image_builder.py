"""Tests for the on-demand image build seam (ensure_image)."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from .. import docker_config
from ..models import BUILD_FAILED, BUILD_READY, BuildTask
from ..orch_context import get_sql_session
from ..runner_config import ImageBuild
from . import image_builder
from .image_builder import BuildFailed, ensure_image


def _source(sha="a" * 40) -> ImageBuild:
    return ImageBuild(git_remote="git@x:r.git", git_sha=sha)


async def test_ensure_image_builds_once_and_marks_ready(orch_ctx_no_ch, monkeypatch):
    build = AsyncMock()
    monkeypatch.setattr(image_builder, "build_image_to_tag", build)
    source = _source()

    ensured = await ensure_image(source, worker_id=1)

    build.assert_awaited_once()
    assert ensured.image_tag == docker_config.compute_image_tag("a" * 40)
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == ensured.build_task_id))).scalar_one()
    assert row.status == BUILD_READY


async def test_ensure_image_reuses_ready_row_without_building(orch_ctx_no_ch, monkeypatch):
    build = AsyncMock()
    monkeypatch.setattr(image_builder, "build_image_to_tag", build)
    source = _source()

    first = await ensure_image(source, worker_id=1)
    second = await ensure_image(source, worker_id=2)

    build.assert_awaited_once()  # only the first call built
    assert second.build_task_id == first.build_task_id


async def test_ensure_image_raises_after_exhausting_retries(orch_ctx_no_ch, monkeypatch):
    monkeypatch.setattr(image_builder, "BUILD_POLL_INTERVAL", 0.0)
    monkeypatch.setattr(image_builder, "build_image_to_tag", AsyncMock(side_effect=RuntimeError("boom")))
    source = _source()

    with pytest.raises(BuildFailed):
        await ensure_image(source, worker_id=1)

    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.git_sha == "a" * 40))).scalar_one()
    assert row.status == BUILD_FAILED
    assert row.attempts == row.max_retries + 1
