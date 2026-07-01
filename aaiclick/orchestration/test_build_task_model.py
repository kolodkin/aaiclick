"""Tests for the BuildTask model and its uniqueness constraint."""

from __future__ import annotations

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from aaiclick.orchestration.models import (
    BUILD_BUILDING,
    BUILD_READY,
    BuildTask,
)
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.snowflake import get_snowflake_id


def _build_task(**overrides) -> BuildTask:
    base = {
        "id": get_snowflake_id(),
        "image_key": "k" * 64,
        "image_tag": "aaiclick-job:" + "a" * 40,
        "git_remote": "git@x:r.git",
        "git_sha": "a" * 40,
        "dockerfile": None,
        "status": BUILD_BUILDING,
        "max_retries": 2,
        "attempts": 1,
    }
    base.update(overrides)
    return BuildTask(**base)


async def test_build_task_round_trips(orch_ctx_no_ch):
    bt = _build_task()
    async with get_sql_session() as session:
        session.add(bt)
        await session.commit()
    async with get_sql_session() as session:
        row = (await session.execute(select(BuildTask).where(BuildTask.id == bt.id))).scalar_one()
    assert row.image_key == "k" * 64
    assert row.status == BUILD_BUILDING


async def test_image_key_is_unique(orch_ctx_no_ch):
    first = _build_task(status=BUILD_READY)
    async with get_sql_session() as session:
        session.add(first)
        await session.commit()
    duplicate = _build_task(id=get_snowflake_id())  # same image_key, different id
    with pytest.raises(IntegrityError):
        async with get_sql_session() as session:
            session.add(duplicate)
            await session.commit()
