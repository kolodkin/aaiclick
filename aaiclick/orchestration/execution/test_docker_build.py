"""Tests for the docker_build task."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from .. import docker_config
from ..runner_config import ImageBuild, ImagePrebuilt
from . import docker_build
from .docker_build import resolve_launch_image
from .docker_scaffold import DEFAULT_BUILD_DOCKERFILE_TEMPLATE


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


async def test_build_image_to_tag_explicit_missing_dockerfile_raises(monkeypatch):
    """The default-Dockerfile fallback applies only to the implicit ``Dockerfile``;
    an explicitly named path that is absent is a user error."""
    _stub_build_path(monkeypatch, {})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40, dockerfile="Dockerfile.missing")

    with pytest.raises(FileNotFoundError, match="Dockerfile not found"):
        await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))


def _stub_build_path(monkeypatch, clone_files: dict[str, str]) -> list[tuple[Path, Path, str]]:
    """Stub everything around the clone + build step and return a list collecting
    ``(context, dockerfile, dockerfile content)`` as ``_docker_build`` was handed
    them (content read at call time, before the temp checkout is removed)."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.delenv("AAICLICK_BASE_IMAGE", raising=False)
    monkeypatch.setattr(docker_build, "_require_docker", AsyncMock())
    monkeypatch.setattr(docker_build, "_docker_pull", AsyncMock(return_value=False))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        for name, content in clone_files.items():
            Path(workdir, name).write_text(content)

    built: list[tuple[Path, Path, str]] = []

    async def fake_build(context, dockerfile, image_tag, build_args):
        built.append((Path(context), Path(dockerfile), Path(dockerfile).read_text()))

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)
    monkeypatch.setattr(docker_build, "_docker_build", fake_build)
    return built


async def test_build_image_to_tag_repo_without_dockerfile_builds_with_default(monkeypatch):
    """A checkout with no ``Dockerfile`` gets the default written in and builds with it."""
    built = _stub_build_path(monkeypatch, {"job.py": "print('hi')\n"})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))

    [(context, dockerfile, content)] = built
    assert dockerfile == context / "Dockerfile"
    assert content == DEFAULT_BUILD_DOCKERFILE_TEMPLATE
    assert "FROM ${BASE_IMAGE}\n" in content


async def test_build_image_to_tag_checked_in_dockerfile_wins_over_default(monkeypatch):
    built = _stub_build_path(monkeypatch, {"Dockerfile": "FROM python:3.12\n"})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag("a" * 40))

    [(context, dockerfile, content)] = built
    assert dockerfile == context / "Dockerfile"
    assert content == "FROM python:3.12\n"


@pytest.mark.parametrize(
    "version, env_base_image, expected",
    [
        pytest.param("1.2.3", None, "ghcr.io/kolodkin/aaiclick:v1.2.3", id="release"),
        # A dev checkout reports a PEP 440 local version (``+g<sha>...``); ``+`` is
        # not a legal Docker tag character, so only the public part is kept.
        pytest.param("0.0.1.dev50+gfc8c68213.d20261008", None, "ghcr.io/kolodkin/aaiclick:v0.0.1.dev50", id="dev"),
        # AAICLICK_BASE_IMAGE wins verbatim: rc workers (release tag not promoted
        # yet) and dev installs pointing at a locally built base.
        pytest.param("1.2.3", "ghcr.io/kolodkin/aaiclick:v1.2.3-rc", "ghcr.io/kolodkin/aaiclick:v1.2.3-rc", id="env"),
    ],
)
def test_collect_build_args_base_image(monkeypatch, version, env_base_image, expected):
    monkeypatch.setattr(docker_build, "_aaiclick_version", lambda: version)
    if env_base_image is None:
        monkeypatch.delenv("AAICLICK_BASE_IMAGE", raising=False)
    else:
        monkeypatch.setenv("AAICLICK_BASE_IMAGE", env_base_image)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    assert f"BASE_IMAGE={expected}" in docker_build._collect_build_args(source)


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


@pytest.mark.parametrize(
    "registry, expected_tag",
    [
        # Empty reads as unset (``get_registry``): the local-build mode.
        pytest.param("", "aaiclick-job:" + "a" * 40, id="no-registry"),
        pytest.param("registry.example:5000", "registry.example:5000/aaiclick-job:" + "a" * 40, id="registry"),
    ],
)
async def test_resolve_launch_image_never_builds(monkeypatch, registry, expected_tag):
    """The build task in the graph owns the build in both modes (its dependency
    edge guaranteed the push); launch only computes the tag."""
    build = AsyncMock()
    monkeypatch.setenv("AAICLICK_REGISTRY", registry)
    monkeypatch.setattr(docker_build, "build_image_to_tag", build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)

    assert await resolve_launch_image(source, task_id=1) == expected_tag
    build.assert_not_awaited()
