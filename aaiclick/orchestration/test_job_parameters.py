"""
Tests for job submission parameters: ``preservation_mode`` and
``sampling_strategy``. Verifies:

- explicit args persist on the ``Job`` row
- ``AAICLICK_DEFAULT_PRESERVATION_MODE`` env var provides the default
- invalid env values and invalid mode/strategy combinations raise
"""

from __future__ import annotations

import pytest

from aaiclick.orchestration.env import get_default_preservation_mode
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.models import Job, PreservationMode
from aaiclick.orchestration.orch_context import get_sql_session
from sqlmodel import select


async def test_default_mode_is_none_when_env_unset(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    assert get_default_preservation_mode() is PreservationMode.NONE


async def test_env_var_sets_default_mode(monkeypatch):
    monkeypatch.setenv("AAICLICK_DEFAULT_PRESERVATION_MODE", "FULL")
    assert get_default_preservation_mode() is PreservationMode.FULL


async def test_env_var_is_case_insensitive(monkeypatch):
    monkeypatch.setenv("AAICLICK_DEFAULT_PRESERVATION_MODE", "full")
    assert get_default_preservation_mode() is PreservationMode.FULL


async def test_env_var_invalid_raises(monkeypatch):
    monkeypatch.setenv("AAICLICK_DEFAULT_PRESERVATION_MODE", "BANANA")
    with pytest.raises(ValueError, match="AAICLICK_DEFAULT_PRESERVATION_MODE"):
        get_default_preservation_mode()


async def test_create_job_persists_preservation_mode(orch_ctx):
    job = await create_job(
        "test-pm",
        "aaiclick.orchestration.fixtures.sample_tasks.sleep_task",
        preservation_mode=PreservationMode.FULL,
    )
    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one()
    assert db_job.preservation_mode is PreservationMode.FULL
    assert db_job.sampling_strategy is None


async def test_create_job_persists_sampling_strategy(orch_ctx):
    strategy = {"p_some_table": "x = 1"}
    job = await create_job(
        "test-strategy",
        "aaiclick.orchestration.fixtures.sample_tasks.sleep_task",
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy=strategy,
    )
    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one()
    assert db_job.preservation_mode is PreservationMode.STRATEGY
    assert db_job.sampling_strategy == strategy


async def test_create_job_strategy_without_mode_raises(orch_ctx):
    with pytest.raises(ValueError, match="preservation_mode=STRATEGY"):
        await create_job(
            "test-bad-strategy",
            "aaiclick.orchestration.fixtures.sample_tasks.sleep_task",
            sampling_strategy={"p_foo": "x = 1"},
        )


async def test_create_job_strategy_mode_without_strategy_raises(orch_ctx):
    with pytest.raises(ValueError, match="requires a non-empty sampling_strategy"):
        await create_job(
            "test-empty-strategy",
            "aaiclick.orchestration.fixtures.sample_tasks.sleep_task",
            preservation_mode=PreservationMode.STRATEGY,
        )


async def test_create_job_env_default_applied(orch_ctx, monkeypatch):
    monkeypatch.setenv("AAICLICK_DEFAULT_PRESERVATION_MODE", "FULL")
    job = await create_job(
        "test-env-default",
        "aaiclick.orchestration.fixtures.sample_tasks.sleep_task",
    )
    async with get_sql_session() as session:
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one()
    assert db_job.preservation_mode is PreservationMode.FULL
