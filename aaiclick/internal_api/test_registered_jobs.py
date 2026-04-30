"""Tests for ``aaiclick.internal_api.registered_jobs``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.models import PRESERVATION_FULL, PreservationMode
from aaiclick.orchestration.registered_jobs import register_job as _register_job_impl
from aaiclick.orchestration.view_models import RegisteredJobView
from aaiclick.view_models import Page, RegisteredJobFilter, RegisterJobRequest

from . import errors, registered_jobs


async def test_list_registered_jobs_returns_page_with_total(orch_ctx):
    await _register_job_impl(name="rj_a", entrypoint="myapp.rj_a")
    await _register_job_impl(name="rj_b", entrypoint="myapp.rj_b")

    page = await registered_jobs.list_registered_jobs()

    assert isinstance(page, Page)
    assert page.total is not None and page.total >= 2
    assert all(isinstance(rj, RegisteredJobView) for rj in page.items)
    names = [rj.name for rj in page.items]
    assert "rj_a" in names and "rj_b" in names


async def test_list_registered_jobs_filter_by_enabled(orch_ctx):
    await _register_job_impl(name="rj_on", entrypoint="myapp.on", enabled=True)
    await _register_job_impl(name="rj_off", entrypoint="myapp.off", enabled=False)

    enabled = await registered_jobs.list_registered_jobs(RegisteredJobFilter(enabled=True))
    disabled = await registered_jobs.list_registered_jobs(RegisteredJobFilter(enabled=False))

    enabled_names = [rj.name for rj in enabled.items]
    disabled_names = [rj.name for rj in disabled.items]
    assert "rj_on" in enabled_names and "rj_off" not in enabled_names
    assert "rj_off" in disabled_names and "rj_on" not in disabled_names


async def test_list_registered_jobs_name_like_and_pagination(orch_ctx):
    for i in range(5):
        await _register_job_impl(name=f"page_{i}", entrypoint=f"myapp.page_{i}")

    first = await registered_jobs.list_registered_jobs(
        RegisteredJobFilter(name="page_%", limit=2, offset=0),
    )
    second = await registered_jobs.list_registered_jobs(
        RegisteredJobFilter(name="page_%", limit=2, offset=2),
    )

    assert first.total == 5
    assert len(first.items) == 2 and len(second.items) == 2
    assert {rj.id for rj in first.items}.isdisjoint({rj.id for rj in second.items})


async def test_register_job_returns_view_and_persists(orch_ctx):
    request = RegisterJobRequest(
        name="new_reg",
        entrypoint="myapp.new_reg",
        schedule="0 8 * * *",
        preservation_mode=PRESERVATION_FULL,
    )

    view = await registered_jobs.register_job(request)

    assert isinstance(view, RegisteredJobView)
    assert view.name == "new_reg"
    assert view.schedule == "0 8 * * *"
    assert view.preservation_mode == PRESERVATION_FULL
    assert view.next_run_at is not None

    page = await registered_jobs.list_registered_jobs(RegisteredJobFilter(name="new_reg"))
    assert [rj.name for rj in page.items] == ["new_reg"]


async def test_register_job_duplicate_raises_conflict(orch_ctx):
    request = RegisterJobRequest(name="dup_reg", entrypoint="myapp.dup_reg")

    await registered_jobs.register_job(request)

    with pytest.raises(errors.Conflict):
        await registered_jobs.register_job(request)


async def test_enable_job_returns_view_and_recomputes_next_run(orch_ctx):
    await _register_job_impl(
        name="to_enable",
        entrypoint="myapp.to_enable",
        schedule="0 6 * * *",
        enabled=False,
    )

    view = await registered_jobs.enable_job("to_enable")

    assert isinstance(view, RegisteredJobView)
    assert view.enabled is True
    assert view.next_run_at is not None


async def test_enable_job_missing_raises_not_found(orch_ctx):
    with pytest.raises(errors.NotFound):
        await registered_jobs.enable_job("ghost_job")


async def test_disable_job_clears_next_run(orch_ctx):
    await _register_job_impl(
        name="to_disable",
        entrypoint="myapp.to_disable",
        schedule="0 6 * * *",
    )

    view = await registered_jobs.disable_job("to_disable")

    assert view.enabled is False
    assert view.next_run_at is None


async def test_disable_job_missing_raises_not_found(orch_ctx):
    with pytest.raises(errors.NotFound):
        await registered_jobs.disable_job("ghost_job")
