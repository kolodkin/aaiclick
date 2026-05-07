"""Tests for the docker_build task."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from ..docker_config import resolve_docker_config
from ..models import Job, RegisteredJob
from . import docker_build


def _job(**overrides) -> Job:
    base = {
        "id": 1,
        "name": "test",
        "run_type": "MANUAL",
        "runner_mode": "docker",
        "git_remote": "https://example.com/repo.git",
        "git_sha": "a" * 40,
        "git_branch": "main",
        "build_context": None,
        "dockerfile": None,
        "image_tag": "aaiclick-job:" + "a" * 40,
    }
    base.update(overrides)
    return Job(**base)


async def test_collect_build_args_omits_unset_values(monkeypatch):
    monkeypatch.delenv("AAICLICK_PIP_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_PIP_EXTRA_INDEX_URL", raising=False)

    job = _job(git_branch=None, build_context=None)
    args = docker_build._collect_build_args(job)

    assert "--build-arg" in args
    assert any(a.startswith("GIT_REMOTE=") for a in args)
    assert not any(a.startswith("GIT_BRANCH=") for a in args)
    assert not any(a.startswith("BUILD_CONTEXT=") for a in args)
    assert not any(a.startswith("PIP_INDEX_URL=") for a in args)


async def test_collect_build_args_forwards_pip_indices(monkeypatch):
    monkeypatch.setenv("AAICLICK_PIP_INDEX_URL", "http://pypi.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_EXTRA_INDEX_URL", "http://extra.test/simple/")

    args = docker_build._collect_build_args(_job())

    assert "PIP_INDEX_URL=http://pypi.test/simple/" in args
    assert "PIP_EXTRA_INDEX_URL=http://extra.test/simple/" in args


async def test_build_image_registry_hit_short_circuits(monkeypatch):
    """When AAICLICK_DOCKER_REGISTRY is set and `docker pull` succeeds,
    the build task does no clone, no build, and no push."""
    monkeypatch.setenv("AAICLICK_DOCKER_REGISTRY", "registry.example:5000")
    job = _job()

    fetch = AsyncMock(return_value=job)
    pull = AsyncMock(return_value=True)
    inspect = AsyncMock(return_value=False)
    clone = AsyncMock()
    build = AsyncMock()
    push = AsyncMock()

    monkeypatch.setattr(docker_build, "_fetch_job", fetch)
    monkeypatch.setattr(docker_build, "_docker_pull", pull)
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", inspect)
    monkeypatch.setattr(docker_build, "_git_clone_at_sha", clone)
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image.func(job_id=job.id)

    pull.assert_awaited_once_with(job.image_tag)
    inspect.assert_not_called()
    clone.assert_not_called()
    build.assert_not_called()
    push.assert_not_called()


async def test_build_image_local_cache_hit_skips_build(monkeypatch):
    """No registry configured + local image present → no clone/build/push."""
    monkeypatch.delenv("AAICLICK_DOCKER_REGISTRY", raising=False)
    job = _job()

    monkeypatch.setattr(docker_build, "_fetch_job", AsyncMock(return_value=job))
    pull = AsyncMock(return_value=False)
    inspect = AsyncMock(return_value=True)
    clone = AsyncMock()
    build = AsyncMock()
    push = AsyncMock()
    monkeypatch.setattr(docker_build, "_docker_pull", pull)
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", inspect)
    monkeypatch.setattr(docker_build, "_git_clone_at_sha", clone)
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image.func(job_id=job.id)

    pull.assert_not_called()
    inspect.assert_awaited_once_with(job.image_tag)
    clone.assert_not_called()
    build.assert_not_called()
    push.assert_not_called()


async def test_build_image_pushes_after_local_cache_hit_when_registry_set(monkeypatch):
    """Registry set + registry pull misses + local image present → skip
    the build, but **still** attempt the push.

    Regression guard for the retry-after-push-failure path: if a previous
    attempt built locally and then failed to push, the local image is
    cached. A naive ``return-on-cache-hit`` would short-circuit the
    retry; the retry must re-attempt the push instead, otherwise the
    image would never reach the registry and other hosts couldn't pull it.
    """
    monkeypatch.setenv("AAICLICK_DOCKER_REGISTRY", "registry.example:5000")
    job = _job()

    monkeypatch.setattr(docker_build, "_fetch_job", AsyncMock(return_value=job))
    pull = AsyncMock(return_value=False)
    inspect = AsyncMock(return_value=True)
    clone = AsyncMock()
    build = AsyncMock()
    push = AsyncMock()
    monkeypatch.setattr(docker_build, "_docker_pull", pull)
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", inspect)
    monkeypatch.setattr(docker_build, "_git_clone_at_sha", clone)
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image.func(job_id=job.id)

    pull.assert_awaited_once_with(job.image_tag)
    inspect.assert_awaited_once_with(job.image_tag)
    clone.assert_not_called()
    build.assert_not_called()
    push.assert_awaited_once_with(job.image_tag)


async def test_build_image_full_build_pushes_when_registry_set(monkeypatch, tmp_path):
    """Cache miss + registry set → clone, build, push."""
    monkeypatch.setenv("AAICLICK_DOCKER_REGISTRY", "registry.example:5000")
    job = _job()

    monkeypatch.setattr(docker_build, "_fetch_job", AsyncMock(return_value=job))
    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        Path(workdir, "Dockerfile").write_text("FROM scratch\n")

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)

    build = AsyncMock()
    push = AsyncMock()
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image.func(job_id=job.id)

    build.assert_awaited_once()
    push.assert_awaited_once_with(job.image_tag)


async def test_build_image_no_push_without_registry(monkeypatch, tmp_path):
    """Cache miss + no registry → clone+build but skip push."""
    monkeypatch.delenv("AAICLICK_DOCKER_REGISTRY", raising=False)
    job = _job()

    monkeypatch.setattr(docker_build, "_fetch_job", AsyncMock(return_value=job))
    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        Path(workdir, "Dockerfile").write_text("FROM scratch\n")

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)

    build = AsyncMock()
    push = AsyncMock()
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image.func(job_id=job.id)

    build.assert_awaited_once()
    push.assert_not_called()


async def test_build_image_missing_dockerfile_raises(monkeypatch):
    monkeypatch.delenv("AAICLICK_DOCKER_REGISTRY", raising=False)
    job = _job(build_context="subdir", dockerfile="Dockerfile.missing")

    monkeypatch.setattr(docker_build, "_fetch_job", AsyncMock(return_value=job))
    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        # Don't create the Dockerfile — should trigger the check.
        return None

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)

    with pytest.raises(FileNotFoundError, match="Dockerfile not found"):
        await docker_build.build_image.func(job_id=job.id)


async def test_resolve_docker_config_kwargs_override_registered_defaults(
    monkeypatch,
):
    """The three-layer resolve picks the right value at each level."""
    monkeypatch.delenv("AAICLICK_DOCKER_REGISTRY", raising=False)

    registered = RegisteredJob(
        id=1,
        name="r",
        entrypoint="x.y",
        runner_mode="docker",
        git_remote="git@registered.example:repo.git",
        build_context="default_subdir",
        dockerfile="Dockerfile.default",
    )

    config = await resolve_docker_config(
        registered,
        git_remote="git@override.example:repo.git",
        git_sha="b" * 40,
        git_branch=None,
        build_context="override_subdir",
        dockerfile=None,
    )

    assert config.git_remote == "git@override.example:repo.git"
    assert config.git_sha == "b" * 40
    assert config.build_context == "override_subdir"
    # dockerfile inherits the registered default since kwarg is None
    assert config.dockerfile == "Dockerfile.default"
