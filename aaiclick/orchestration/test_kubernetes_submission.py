"""Tests for the Kubernetes submission path (factory + register)."""

from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.orchestration.docker_config import DockerJobConfig
from aaiclick.orchestration.factories import create_kubernetes_job
from aaiclick.orchestration.models import RUNNER_KUBERNETES, Task
from aaiclick.orchestration.orch_context import get_sql_session
from aaiclick.orchestration.registered_jobs import get_registered_job, upsert_registered_job

_DOCKER_CFG = DockerJobConfig(
    git_remote="git://x/repo.git",
    git_sha="a" * 40,
    git_branch="main",
    dockerfile=None,
    image_tag="reg/aaiclick-job:" + "a" * 40,
)


@pytest.mark.usefixtures("fast_poll")
async def test_create_kubernetes_job_writes_job_and_build_task(orch_ctx_no_ch):
    job = await create_kubernetes_job(
        name="k8s_submit",
        entrypoint="sample_jobs.entry",
        docker_config=_DOCKER_CFG,
        kubernetes_config={"namespace": "ml"},
    )
    assert job.runner_mode == RUNNER_KUBERNETES
    assert job.image_tag == _DOCKER_CFG.image_tag
    assert job.kubernetes_config == {"namespace": "ml"}

    async with get_sql_session() as session:
        tasks = (await session.execute(select(Task).where(Task.job_id == job.id))).scalars().all()
    entrypoints = {t.entrypoint for t in tasks}
    assert "aaiclick.orchestration.execution.docker_build.build_image" in entrypoints
    assert "sample_jobs.entry" in entrypoints


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
