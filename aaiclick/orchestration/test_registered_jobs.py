"""Tests for registered jobs CRUD operations."""

from datetime import datetime

import pytest
from sqlmodel import select

from .factories import resolve_job_config
from .models import PreservationMode, RegisteredJob, RunType, Task
from .orch_context import get_sql_session
from .registered_jobs import (
    compute_next_run,
    disable_job,
    enable_job,
    get_registered_job,
    list_registered_jobs,
    register_job,
    run_job,
    upsert_registered_job,
)


def test_compute_next_run_daily():
    base = datetime(2026, 4, 4, 7, 0, 0)
    result = compute_next_run("0 8 * * *", after=base)
    assert result == datetime(2026, 4, 4, 8, 0, 0)


def test_compute_next_run_past_time():
    base = datetime(2026, 4, 4, 9, 0, 0)
    result = compute_next_run("0 8 * * *", after=base)
    assert result == datetime(2026, 4, 5, 8, 0, 0)


def test_compute_next_run_every_5_minutes():
    base = datetime(2026, 4, 4, 12, 3, 0)
    result = compute_next_run("*/5 * * * *", after=base)
    assert result == datetime(2026, 4, 4, 12, 5, 0)


async def test_register_job(orch_ctx):
    job = await register_job(
        name="test_etl",
        entrypoint="myapp.pipelines.etl_job",
    )
    assert job.name == "test_etl"
    assert job.entrypoint == "myapp.pipelines.etl_job"
    assert job.enabled is True
    assert job.schedule is None
    assert job.next_run_at is None
    assert job.default_kwargs is None


async def test_register_job_with_schedule(orch_ctx):
    job = await register_job(
        name="scheduled_etl",
        entrypoint="myapp.pipelines.etl_job",
        schedule="0 8 * * *",
    )
    assert job.schedule == "0 8 * * *"
    assert job.next_run_at is not None
    assert job.next_run_at > datetime.utcnow()


async def test_register_job_with_kwargs(orch_ctx):
    job = await register_job(
        name="param_etl",
        entrypoint="myapp.pipelines.etl_job",
        default_kwargs={"url": "https://example.com/data.parquet"},
    )
    assert job.default_kwargs == {"url": "https://example.com/data.parquet"}


async def test_register_job_duplicate_raises(orch_ctx):
    await register_job(name="dup_job", entrypoint="myapp.dup")
    with pytest.raises(ValueError, match="already exists"):
        await register_job(name="dup_job", entrypoint="myapp.dup")


async def test_register_job_disabled_no_next_run(orch_ctx):
    job = await register_job(
        name="disabled_etl",
        entrypoint="myapp.pipelines.etl_job",
        schedule="0 8 * * *",
        enabled=False,
    )
    assert job.enabled is False
    assert job.next_run_at is None


async def test_get_registered_job(orch_ctx):
    await register_job(name="lookup_job", entrypoint="myapp.lookup")
    job = await get_registered_job("lookup_job")
    assert job is not None
    assert job.name == "lookup_job"


async def test_get_registered_job_not_found(orch_ctx):
    result = await get_registered_job("nonexistent")
    assert result is None


async def test_upsert_creates_new(orch_ctx):
    job = await upsert_registered_job(
        name="upsert_new",
        entrypoint="myapp.upsert_new",
        schedule="*/10 * * * *",
    )
    assert job.name == "upsert_new"
    assert job.schedule == "*/10 * * * *"
    assert job.next_run_at is not None


async def test_upsert_updates_existing(orch_ctx):
    original = await register_job(
        name="upsert_existing",
        entrypoint="myapp.old_entrypoint",
    )
    updated = await upsert_registered_job(
        name="upsert_existing",
        entrypoint="myapp.new_entrypoint",
        schedule="0 12 * * *",
        default_kwargs={"key": "value"},
    )
    assert updated.id == original.id
    assert updated.entrypoint == "myapp.new_entrypoint"
    assert updated.schedule == "0 12 * * *"
    assert updated.default_kwargs == {"key": "value"}
    assert updated.next_run_at is not None
    assert updated.updated_at > original.updated_at


async def test_enable_job(orch_ctx):
    created = await register_job(
        name="to_enable",
        entrypoint="myapp.to_enable",
        schedule="0 6 * * *",
        enabled=False,
    )
    job_id = await enable_job("to_enable")
    assert job_id == created.id
    job = await get_registered_job("to_enable")
    assert job is not None
    assert job.enabled is True
    assert job.next_run_at is not None


async def test_enable_job_not_found(orch_ctx):
    with pytest.raises(ValueError, match="not found"):
        await enable_job("ghost_job")


async def test_disable_job(orch_ctx):
    created = await register_job(
        name="to_disable",
        entrypoint="myapp.to_disable",
        schedule="0 6 * * *",
    )
    job_id = await disable_job("to_disable")
    assert job_id == created.id
    job = await get_registered_job("to_disable")
    assert job is not None
    assert job.enabled is False
    assert job.next_run_at is None


async def test_disable_job_not_found(orch_ctx):
    with pytest.raises(ValueError, match="not found"):
        await disable_job("ghost_job")


async def test_list_registered_jobs(orch_ctx):
    await register_job(name="list_a", entrypoint="myapp.a")
    await register_job(name="list_b", entrypoint="myapp.b", enabled=False)
    await register_job(name="list_c", entrypoint="myapp.c")

    all_jobs = await list_registered_jobs()
    names = [j.name for j in all_jobs]
    assert "list_a" in names
    assert "list_b" in names
    assert "list_c" in names

    enabled_jobs = await list_registered_jobs(enabled_only=True)
    enabled_names = [j.name for j in enabled_jobs]
    assert "list_a" in enabled_names
    assert "list_b" not in enabled_names
    assert "list_c" in enabled_names


async def test_run_job_auto_registers(orch_ctx):
    job = await run_job("auto_reg", "myapp.auto_reg")
    assert job.run_type == RunType.MANUAL
    assert job.registered_job_id is not None

    reg = await get_registered_job("auto_reg")
    assert reg is not None
    assert reg.entrypoint == "myapp.auto_reg"


async def test_run_job_creates_entry_task(orch_ctx):
    job = await run_job("task_check", "myapp.task_check", kwargs={"x": 1})

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        tasks = list(result.scalars().all())

    assert len(tasks) == 1
    assert tasks[0].entrypoint == "myapp.task_check"
    assert tasks[0].kwargs == {"x": 1}


async def test_run_job_merges_default_kwargs(orch_ctx):
    await register_job(
        name="merge_kw",
        entrypoint="myapp.merge_kw",
        default_kwargs={"a": 1, "b": 2},
    )
    job = await run_job("merge_kw", "myapp.merge_kw", kwargs={"b": 99, "c": 3})

    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.job_id == job.id))
        task = result.scalar_one()

    assert task.kwargs == {"a": 1, "b": 99, "c": 3}


async def test_run_job_with_existing_registration(orch_ctx):
    reg = await register_job(
        name="existing_reg",
        entrypoint="myapp.existing",
        schedule="0 8 * * *",
    )
    job = await run_job("existing_reg", "myapp.existing")
    assert job.registered_job_id == reg.id
    assert job.run_type == RunType.MANUAL


# Phase 4 — resolve_job_config precedence chain


def test_resolve_explicit_mode_wins_over_registered(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    registered = RegisteredJob(
        id=1,
        name="x",
        entrypoint="foo",
        preservation_mode=PreservationMode.FULL,
    )
    resolved = resolve_job_config(PreservationMode.NONE, None, registered)
    assert resolved.preservation_mode is PreservationMode.NONE
    assert resolved.sampling_strategy is None


def test_resolve_registered_mode_wins_over_env(monkeypatch):
    monkeypatch.setenv("AAICLICK_DEFAULT_PRESERVATION_MODE", "NONE")
    registered = RegisteredJob(
        id=1,
        name="x",
        entrypoint="foo",
        preservation_mode=PreservationMode.FULL,
    )
    resolved = resolve_job_config(None, None, registered)
    assert resolved.preservation_mode is PreservationMode.FULL


def test_resolve_env_wins_when_no_registered_default(monkeypatch):
    monkeypatch.setenv("AAICLICK_DEFAULT_PRESERVATION_MODE", "FULL")
    registered = RegisteredJob(id=1, name="x", entrypoint="foo")
    resolved = resolve_job_config(None, None, registered)
    assert resolved.preservation_mode is PreservationMode.FULL


def test_resolve_hardcoded_fallback(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    resolved = resolve_job_config(None, None, None)
    assert resolved.preservation_mode is PreservationMode.NONE
    assert resolved.sampling_strategy is None


def test_resolve_explicit_strategy_wins_over_registered(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    registered = RegisteredJob(
        id=1,
        name="x",
        entrypoint="foo",
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy={"p_foo": "x = 1"},
    )
    resolved = resolve_job_config(
        None,
        {"p_foo": "x = 99"},
        registered,
    )
    assert resolved.preservation_mode is PreservationMode.STRATEGY
    assert resolved.sampling_strategy == {"p_foo": "x = 99"}


def test_resolve_inherits_registered_strategy(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    registered = RegisteredJob(
        id=1,
        name="x",
        entrypoint="foo",
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy={"p_foo": "x = 1"},
    )
    resolved = resolve_job_config(None, None, registered)
    assert resolved.preservation_mode is PreservationMode.STRATEGY
    assert resolved.sampling_strategy == {"p_foo": "x = 1"}


def test_resolve_strategy_mode_requires_strategy(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    with pytest.raises(ValueError, match="requires a non-empty sampling_strategy"):
        resolve_job_config(PreservationMode.STRATEGY, None, None)


def test_resolve_strategy_without_strategy_mode_rejected(monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    with pytest.raises(ValueError, match="only valid with preservation_mode=STRATEGY"):
        resolve_job_config(PreservationMode.NONE, {"p_foo": "x = 1"}, None)


# Phase 4 — register_job accepts preservation defaults


async def test_register_job_persists_preservation_mode(orch_ctx):
    reg = await register_job(
        name="full_job",
        entrypoint="myapp.full_pipeline",
        preservation_mode=PreservationMode.FULL,
    )
    assert reg.preservation_mode is PreservationMode.FULL
    assert reg.sampling_strategy is None

    fetched = await get_registered_job("full_job")
    assert fetched is not None
    assert fetched.preservation_mode is PreservationMode.FULL


async def test_register_job_persists_sampling_strategy(orch_ctx):
    strategy = {"p_foo": "x = 1"}
    reg = await register_job(
        name="strat_job",
        entrypoint="myapp.strat_pipeline",
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy=strategy,
    )
    assert reg.preservation_mode is PreservationMode.STRATEGY
    assert reg.sampling_strategy == strategy


async def test_register_job_strategy_without_mode_raises(orch_ctx):
    with pytest.raises(ValueError, match="only valid with preservation_mode=STRATEGY"):
        await register_job(
            name="bad_strat",
            entrypoint="myapp.foo",
            preservation_mode=PreservationMode.NONE,
            sampling_strategy={"p_foo": "x = 1"},
        )


async def test_register_job_strategy_mode_without_strategy_raises(orch_ctx):
    with pytest.raises(ValueError, match="requires a non-empty sampling_strategy"):
        await register_job(
            name="empty_strat",
            entrypoint="myapp.foo",
            preservation_mode=PreservationMode.STRATEGY,
        )


# Phase 4 — run_job inherits registered-job defaults


async def test_run_job_inherits_registered_preservation_mode(orch_ctx, monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    await register_job(
        name="full_inherited",
        entrypoint="myapp.full_pipeline",
        preservation_mode=PreservationMode.FULL,
    )
    job = await run_job("full_inherited", "myapp.full_pipeline")
    assert job.preservation_mode is PreservationMode.FULL


async def test_run_job_override_beats_registered_default(orch_ctx, monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    await register_job(
        name="override_test",
        entrypoint="myapp.foo",
        preservation_mode=PreservationMode.FULL,
    )
    job = await run_job(
        "override_test",
        "myapp.foo",
        preservation_mode=PreservationMode.NONE,
    )
    assert job.preservation_mode is PreservationMode.NONE


async def test_run_job_inherits_registered_sampling_strategy(orch_ctx, monkeypatch):
    monkeypatch.delenv("AAICLICK_DEFAULT_PRESERVATION_MODE", raising=False)
    strategy = {"p_foo": "x = 1"}
    await register_job(
        name="strat_inherited",
        entrypoint="myapp.strat_pipeline",
        preservation_mode=PreservationMode.STRATEGY,
        sampling_strategy=strategy,
    )
    job = await run_job("strat_inherited", "myapp.strat_pipeline")
    assert job.preservation_mode is PreservationMode.STRATEGY
    assert job.sampling_strategy == strategy
