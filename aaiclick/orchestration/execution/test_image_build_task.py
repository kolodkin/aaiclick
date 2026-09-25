"""Tests for the registry-mode image-build task body."""

import pytest

from ..runner_config import ImageBuild
from . import image_build_task
from .image_build_task import build_task_name, run_image_build


@pytest.mark.parametrize(
    "source, expected",
    [
        pytest.param(
            ImageBuild(
                git_remote="https://example.com/org/myrepo.git", git_sha="abcdef1234" + "0" * 30, git_branch="main"
            ),
            "build-image:myrepo@main:abcdef12",
            id="repo_branch_and_short_sha",
        ),
        pytest.param(
            ImageBuild(git_remote="git@example.com:org/myrepo.git", git_sha="a" * 40),
            f"build-image:myrepo:{'a' * 8}",
            id="scp_remote_omits_unknown_branch",
        ),
        pytest.param(
            ImageBuild(git_remote="https://example.com/r.git", git_sha="b" * 40, dockerfile="Dockerfile.gpu"),
            f"build-image:r:{'b' * 8} (Dockerfile.gpu)",
            id="appends_non_default_dockerfile",
        ),
    ],
)
def test_build_task_name(source, expected):
    assert build_task_name(source) == expected


async def test_run_image_build_delegates_to_build_image_to_tag(monkeypatch):
    calls: list[tuple[ImageBuild, str]] = []

    async def fake_build(source: ImageBuild, image_tag: str) -> None:
        calls.append((source, image_tag))

    monkeypatch.setenv("AAICLICK_REGISTRY", "registry.example:5000")
    monkeypatch.setattr(image_build_task, "build_image_to_tag", fake_build)
    await run_image_build(git_remote="https://example.com/r.git", git_sha="a" * 40)
    source, tag = calls[0]
    assert source.git_sha == "a" * 40
    assert tag == "registry.example:5000/aaiclick-job:" + "a" * 40


async def test_run_image_build_local_mode_builds_unprefixed_tag(monkeypatch):
    calls: list[str] = []

    async def fake_build(source: ImageBuild, image_tag: str) -> None:
        calls.append(image_tag)

    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.setenv("AAICLICK_LOCAL_BUILD", "1")
    monkeypatch.setattr(image_build_task, "build_image_to_tag", fake_build)
    await run_image_build(git_remote="https://example.com/r.git", git_sha="a" * 40)
    assert calls == ["aaiclick-job:" + "a" * 40]


async def test_run_image_build_requires_a_build_mode(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.delenv("AAICLICK_LOCAL_BUILD", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_LOCAL_BUILD"):
        await run_image_build(git_remote="https://example.com/r.git", git_sha="a" * 40)
