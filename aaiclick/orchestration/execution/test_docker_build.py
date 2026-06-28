"""Tests for the docker_build task."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from .. import docker_config
from ..docker_config import resolve_runner_config
from ..models import Job, RegisteredJob
from ..runner_config import ImageBuild
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
        "dockerfile": None,
        "image_tag": "aaiclick-job:" + "a" * 40,
    }
    base.update(overrides)
    return Job(**base)


async def test_collect_build_args_omits_unset_values(monkeypatch):
    monkeypatch.delenv("AAICLICK_PIP_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_PIP_EXTRA_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_PIP_TRUSTED_HOST", raising=False)

    job = _job(git_branch=None)
    args = docker_build._collect_build_args(job)

    assert "--build-arg" in args
    assert any(a.startswith("GIT_REMOTE=") for a in args)
    assert not any(a.startswith("GIT_BRANCH=") for a in args)
    assert not any(a.startswith("PIP_INDEX_URL=") for a in args)
    assert not any(a.startswith("PIP_TRUSTED_HOST=") for a in args)


async def test_collect_build_args_forwards_pip_indices(monkeypatch):
    monkeypatch.setenv("AAICLICK_PIP_INDEX_URL", "http://pypi.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_EXTRA_INDEX_URL", "http://extra.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_TRUSTED_HOST", "pypi.test")

    args = docker_build._collect_build_args(_job())

    assert "PIP_INDEX_URL=http://pypi.test/simple/" in args
    assert "PIP_EXTRA_INDEX_URL=http://extra.test/simple/" in args
    assert "PIP_TRUSTED_HOST=pypi.test" in args


async def test_build_image_pushes_after_local_cache_hit_when_registry_set(monkeypatch):
    """Registry set + registry pull misses + local image present → skip
    the build, but **still** attempt the push.

    Regression guard for the retry-after-push-failure path: if a previous
    attempt built locally and then failed to push, the local image is
    cached. A naive ``return-on-cache-hit`` would short-circuit the
    retry; the retry must re-attempt the push instead, otherwise the
    image would never reach the registry and other hosts couldn't pull it.
    """
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
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


async def test_build_image_missing_dockerfile_raises(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    job = _job(dockerfile="Dockerfile.missing")

    monkeypatch.setattr(docker_build, "_fetch_job", AsyncMock(return_value=job))
    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        # Don't create the Dockerfile — should trigger the check.
        return None

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)

    with pytest.raises(FileNotFoundError, match="Dockerfile not found"):
        await docker_build.build_image.func(job_id=job.id)


async def test_resolve_runner_config_kwargs_override_registered_defaults(
    monkeypatch,
):
    """The three-layer resolve picks the right value at each level."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.setattr(docker_config, "auto_detect_git_branch", AsyncMock(return_value="auto-branch"))

    registered = RegisteredJob(
        id=1,
        name="r",
        entrypoint="x.y",
        runner_mode="docker",
        git_remote="git@registered.example:repo.git",
        dockerfile="Dockerfile.default",
    )

    config = await resolve_runner_config(
        registered,
        runner_mode="docker",
        git_remote="git@override.example:repo.git",
        git_sha="b" * 40,
        git_branch=None,
        dockerfile=None,
    )

    assert isinstance(config.image, ImageBuild)
    assert config.image.git_remote == "git@override.example:repo.git"
    assert config.image.git_sha == "b" * 40
    # dockerfile inherits the registered default since kwarg is None
    assert config.image.dockerfile == "Dockerfile.default"
