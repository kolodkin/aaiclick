"""Tests for the Kubernetes submission path (factory + register)."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.orchestration.factories import create_built_job
from aaiclick.orchestration.models import RUNNER_KUBERNETES, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import get_registered_job, upsert_registered_job
from aaiclick.orchestration.runner_config import BUILD_TASK_ENTRYPOINT, ImageBuild, KubernetesRunner


@pytest.mark.usefixtures("fast_poll")
async def test_create_kubernetes_job_writes_job_and_entry_task(orch_ctx_no_ch):
    """A build-source kubernetes job carries the injected image-build task
    alongside the entry task (see ``create_built_job``)."""
    runner = KubernetesRunner(
        image=ImageBuild(git_remote="git://x/repo.git", git_sha="a" * 40, git_branch="main"),
        namespace="ml",
    )
    job = await create_built_job(
        name="k8s_submit",
        entrypoint="sample_jobs.entry",
        runner=runner,
        entry_type="module",
    )
    assert job.runner_mode == RUNNER_KUBERNETES
    assert job.runner is not None
    assert job.runner["type"] == "kubernetes"
    assert job.runner["image"]["type"] == "build"
    assert job.runner["namespace"] == "ml"

    async with get_sql_session() as session:
        tasks = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    entrypoints = {t.entrypoint for t in tasks}
    assert entrypoints == {"sample_jobs.entry", BUILD_TASK_ENTRYPOINT}


@pytest.mark.usefixtures("fast_poll")
async def test_upsert_registered_job_persists_kubernetes_config(orch_ctx_no_ch):
    await upsert_registered_job(
        name="k8s_reg",
        entrypoint="sample_jobs.entry",
        runner_mode=RUNNER_KUBERNETES,
        kubernetes_config={"namespace": "ml", "service_account": "sa"},
    )
    reg = await get_registered_job("k8s_reg")
    assert reg is not None
    assert reg.runner_mode == RUNNER_KUBERNETES
    assert reg.kubernetes_config == {"namespace": "ml", "service_account": "sa"}
