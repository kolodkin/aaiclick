"""Tests for the docker_build task."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from .. import docker_config
from ..runner_config import ImageBuild, ImagePrebuilt
from . import docker_build
from .docker_build import resolve_launch_image


async def test_collect_build_args_omits_unset_values(monkeypatch):
    monkeypatch.delenv("AAICLICK_PIP_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_PIP_EXTRA_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_PIP_TRUSTED_HOST", raising=False)

    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)
    args = docker_build._collect_build_args(source)

    assert "--build-arg" in args
    assert any(a.startswith("GIT_REMOTE=") for a in args)
    assert not any(a.startswith("GIT_BRANCH=") for a in args)
    assert not any(a.startswith("PIP_INDEX_URL=") for a in args)
    assert not any(a.startswith("PIP_TRUSTED_HOST=") for a in args)


async def test_collect_build_args_forwards_pip_indices(monkeypatch):
    monkeypatch.setenv("AAICLICK_PIP_INDEX_URL", "http://pypi.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_EXTRA_INDEX_URL", "http://extra.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_TRUSTED_HOST", "pypi.test")

    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40, git_branch="main")
    args = docker_build._collect_build_args(source)

    assert "PIP_INDEX_URL=http://pypi.test/simple/" in args
    assert "PIP_EXTRA_INDEX_URL=http://extra.test/simple/" in args
    assert "PIP_TRUSTED_HOST=pypi.test" in args


async def test_build_image_to_tag_pushes_after_local_cache_hit_when_registry_set(monkeypatch):
    """Registry set + registry pull misses + local image present → skip
    the build, but **still** attempt the push.

    Regression guard for the retry-after-push-failure path: if a previous
    attempt built locally and then failed to push, the local image is
    cached. A naive ``return-on-cache-hit`` would short-circuit the
    retry; the retry must re-attempt the push instead, otherwise the
    image would never reach the registry and other hosts couldn't pull it.
    """
    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)
    expected_tag = docker_config.compute_image_tag("a" * 40)

    pull = AsyncMock(return_value=False)
    inspect = AsyncMock(return_value=True)
    clone = AsyncMock()
    build = AsyncMock()
    push = AsyncMock()
    monkeypatch.setattr(docker_build, "_require_docker", AsyncMock())
    monkeypatch.setattr(docker_build, "_docker_pull", pull)
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", inspect)
    monkeypatch.setattr(docker_build, "_git_clone_at_sha", clone)
    monkeypatch.setattr(docker_build, "_docker_build", build)
    monkeypatch.setattr(docker_build, "_docker_push", push)

    await docker_build.build_image_to_tag(source, expected_tag)

    pull.assert_awaited_once_with(expected_tag)
    inspect.assert_awaited_once_with(expected_tag)
    clone.assert_not_called()
    build.assert_not_called()
    push.assert_awaited_once_with(expected_tag)


async def test_git_clone_passes_remote_and_sha_after_end_of_options(monkeypatch):
    """Both remote and SHA are positional to git; ``--`` stops git reading either as an option."""
    run = AsyncMock()
    monkeypatch.setattr(docker_build.cli, "run", run)
    sha = "a" * 40

    await docker_build._git_clone_at_sha("https://example.com/r.git", sha, "/work")

    argvs = [call.args for call in run.await_args_list]
    assert ("git", "-C", "/work", "remote", "add", "origin", "--", "https://example.com/r.git") in argvs
    assert ("git", "-C", "/work", "fetch", "--depth=1", "--quiet", "origin", "--", sha) in argvs


async def test_build_image_to_tag_missing_dockerfile_raises(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40, dockerfile="Dockerfile.missing")

    monkeypatch.setattr(docker_build, "_require_docker", AsyncMock())
    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        return None

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)

    with pytest.raises(FileNotFoundError, match="Dockerfile not found"):
        await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))


async def test_require_docker_raises_clear_error_when_cli_missing(monkeypatch):
    """A worker with no docker binary gets an actionable message, not a raw
    FileNotFoundError from the first subprocess spawn."""
    monkeypatch.setattr(docker_build.cli, "run", AsyncMock(side_effect=FileNotFoundError(2, "No such file", "docker")))

    with pytest.raises(RuntimeError, match="Docker CLI 'docker' not found on this worker"):
        await docker_build._require_docker()


async def test_require_docker_raises_clear_error_when_daemon_unreachable(monkeypatch):
    """CLI present but daemon down → a clear 'not reachable' error including the
    docker stderr, rather than failing later inside `docker build`."""
    monkeypatch.setattr(
        docker_build.cli,
        "run",
        AsyncMock(return_value=(1, "", "Cannot connect to the Docker daemon at unix:///var/run/docker.sock")),
    )

    with pytest.raises(RuntimeError, match="Docker daemon is not reachable"):
        await docker_build._require_docker()


async def test_build_image_to_tag_preflights_docker(monkeypatch):
    """build_image_to_tag runs the docker preflight before any build step."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    require = AsyncMock()
    monkeypatch.setattr(docker_build, "_require_docker", require)
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=True))
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))

    require.assert_awaited_once()


async def test_resolve_launch_image_prebuilt_tag_verbatim():
    source = ImagePrebuilt(image_tag="ghcr.io/x/y:1")
    assert await resolve_launch_image(source, task_id=1) == "ghcr.io/x/y:1"


async def test_resolve_launch_image_rejects_missing_source():
    with pytest.raises(ValueError, match="no image_source"):
        await resolve_launch_image(None, task_id=42)


async def test_resolve_launch_image_never_builds_without_registry(monkeypatch):
    """The build task in the graph owns the build in both modes; launch only
    computes the tag."""
    calls = []

    async def fake_build(source, image_tag):
        calls.append(image_tag)

    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.setattr(docker_build, "build_image_to_tag", fake_build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)
    tag = await resolve_launch_image(source, task_id=1)
    assert calls == []
    assert tag == "aaiclick-job:" + "a" * 40


async def test_resolve_launch_image_skips_build_with_registry(monkeypatch):
    calls = []

    async def fake_build(source, image_tag):
        calls.append(image_tag)

    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    monkeypatch.setattr(docker_build, "build_image_to_tag", fake_build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)
    tag = await resolve_launch_image(source, task_id=1)
    assert calls == []  # the dependency edge guaranteed the push
    assert tag == "registry.example:5000/aaiclick-job:" + "a" * 40
