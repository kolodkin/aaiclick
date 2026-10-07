from unittest.mock import AsyncMock

import pytest

from aaiclick.orchestration import docker_config
from aaiclick.orchestration.docker_config import (
    BUILD_MODE_LOCAL,
    BUILD_MODE_REGISTRY,
    compute_image_tag,
    get_build_mode,
    image_key,
    resolve_image_source,
)
from aaiclick.orchestration.models import RegisteredJob
from aaiclick.orchestration.runner_config import ImageBuild, ImagePrebuilt


def test_image_key_stable_and_distinguishes_fields():
    a = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, dockerfile=None)
    a_again = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, git_branch="ignored", dockerfile=None)
    b = ImageBuild(git_remote="git@x:r.git", git_sha="b" * 40, dockerfile=None)
    c = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")

    assert len(image_key(a)) == 64
    assert image_key(a) == image_key(a_again)  # git_branch is not part of identity
    assert image_key(a) != image_key(b)  # sha matters
    assert image_key(a) != image_key(c)  # dockerfile matters


@pytest.mark.parametrize(
    "registry, local_build, expected",
    [
        pytest.param("registry.example:5000", "", BUILD_MODE_REGISTRY, id="registry"),
        pytest.param("", "1", BUILD_MODE_LOCAL, id="local"),
        pytest.param("", "0", BUILD_MODE_LOCAL, id="local-any-value"),
    ],
)
def test_get_build_mode(monkeypatch, registry, local_build, expected):
    monkeypatch.setenv("AAICLICK_REGISTRY", registry)
    monkeypatch.setenv("AAICLICK_LOCAL_BUILD", local_build)
    assert get_build_mode() == expected


@pytest.mark.parametrize(
    "registry, local_build, match",
    [
        # both set: the two modes are mutually exclusive
        pytest.param("registry.example:5000", "1", "mutually exclusive", id="both"),
        # neither set: the error names both vars so the operator knows the choice
        pytest.param("", "", r"AAICLICK_REGISTRY.*AAICLICK_LOCAL_BUILD", id="neither"),
    ],
)
def test_get_build_mode_rejects_ambiguous_env(monkeypatch, registry, local_build, match):
    monkeypatch.setenv("AAICLICK_REGISTRY", registry)
    monkeypatch.setenv("AAICLICK_LOCAL_BUILD", local_build)
    with pytest.raises(RuntimeError, match=match):
        get_build_mode()


@pytest.mark.parametrize(
    "registered, kwargs, expected",
    [
        pytest.param(None, {}, None, id="nothing-subprocess"),
        pytest.param(None, {"image": "python:3.12"}, ImagePrebuilt(image_tag="python:3.12"), id="run-image"),
        pytest.param(
            None,
            {"build": True},
            ImageBuild(git_remote="git@auto:r.git", git_sha="c" * 40, git_branch="auto"),
            id="run-build-autodetect",
        ),
        pytest.param(
            None,
            {"git_sha": "b" * 40},
            ImageBuild(git_remote="git@auto:r.git", git_sha="b" * 40, git_branch="auto"),
            id="run-modifier-implies-build",
        ),
        pytest.param(
            RegisteredJob(name="r", entrypoint="m.f", image="ghcr.io/x/y:1"),
            {},
            ImagePrebuilt(image_tag="ghcr.io/x/y:1"),
            id="registered-image",
        ),
        pytest.param(
            RegisteredJob(name="r", entrypoint="m.f", build=True, git_remote="git@reg:r.git", dockerfile="D"),
            {},
            ImageBuild(git_remote="git@reg:r.git", git_sha="c" * 40, git_branch="auto", dockerfile="D"),
            id="registered-build",
        ),
        pytest.param(
            RegisteredJob(name="r", entrypoint="m.f", build=True, git_remote="git@reg:r.git"),
            {"image": "python:3.12"},
            ImagePrebuilt(image_tag="python:3.12"),
            id="run-image-outranks-registered-build",
        ),
        pytest.param(
            RegisteredJob(name="r", entrypoint="m.f", image="ghcr.io/x/y:1"),
            {"build": True},
            ImageBuild(git_remote="git@auto:r.git", git_sha="c" * 40, git_branch="auto"),
            id="run-build-outranks-registered-image",
        ),
        pytest.param(
            RegisteredJob(name="r", entrypoint="m.f", build=True, git_remote="git@reg:r.git", dockerfile="D"),
            {"git_remote": "git@override:r.git", "git_sha": "b" * 40},
            ImageBuild(git_remote="git@override:r.git", git_sha="b" * 40, git_branch="auto", dockerfile="D"),
            id="run-modifiers-override-registered-defaults",
        ),
    ],
)
async def test_resolve_image_source(monkeypatch, registered, kwargs, expected):
    monkeypatch.setattr(docker_config, "auto_detect_git_remote", AsyncMock(return_value="git@auto:r.git"))
    monkeypatch.setattr(docker_config, "auto_detect_git_sha", AsyncMock(return_value="c" * 40))
    monkeypatch.setattr(docker_config, "auto_detect_git_branch", AsyncMock(return_value="auto"))
    assert await resolve_image_source(registered, **kwargs) == expected


def test_compute_image_tag_without_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    assert compute_image_tag("b" * 40) == f"aaiclick-job:{'b' * 40}"
