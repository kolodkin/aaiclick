"""Tests for ``aaiclick.internal_api.registered_jobs``."""

from __future__ import annotations

import pytest

from aaiclick.orchestration.models import PRESERVATION_FULL
from aaiclick.orchestration.registered_jobs import register_job as _register_job_impl
from aaiclick.orchestration.view_models import RegisteredJobView
from aaiclick.view_models import Page, RegisteredJobFilter, RegisterJobRequest

from . import errors, registered_jobs

# A real importable callable, used wherever a registration must pass the
# entrypoint-resolution check that ``register_job`` runs before persisting.
_VALID_ENTRYPOINT = "aaiclick.orchestration.fixtures.sample_tasks.simple_task"
# A real module attribute that resolves but is not callable (a str constant).
_NON_CALLABLE_ENTRYPOINT = "aaiclick.orchestration.fixtures.sample_tasks.not_callable"


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
        entrypoint=_VALID_ENTRYPOINT,
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
    request = RegisterJobRequest(name="dup_reg", entrypoint=_VALID_ENTRYPOINT)

    await registered_jobs.register_job(request)

    with pytest.raises(errors.Conflict):
        await registered_jobs.register_job(request)


async def test_register_job_unresolvable_attribute_raises_invalid(orch_ctx):
    request = RegisterJobRequest(
        name="bad_attr",
        entrypoint="aaiclick.orchestration.fixtures.sample_tasks.no_such_task",
    )

    with pytest.raises(errors.Invalid):
        await registered_jobs.register_job(request)


async def test_register_job_non_callable_attribute_raises_invalid(orch_ctx):
    request = RegisterJobRequest(name="not_callable", entrypoint=_NON_CALLABLE_ENTRYPOINT)

    with pytest.raises(errors.Invalid):
        await registered_jobs.register_job(request)


async def test_register_job_invalid_entrypoint_not_persisted(orch_ctx):
    request = RegisterJobRequest(name="ghost_reg", entrypoint="myapp.missing.etl_job")

    with pytest.raises(errors.NotFound):
        await registered_jobs.register_job(request)

    page = await registered_jobs.list_registered_jobs(RegisteredJobFilter(name="ghost_reg"))
    assert page.items == []


async def test_register_job_docker_runner_skips_local_entrypoint_validation(orch_ctx):
    """Docker runner entrypoints live inside the image, not on the host."""
    request = RegisterJobRequest(
        name="docker_job",
        entrypoint="sample_jobs.entry_task",
        runner_mode="docker",
    )

    view = await registered_jobs.register_job(request)

    assert view.name == "docker_job"


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
