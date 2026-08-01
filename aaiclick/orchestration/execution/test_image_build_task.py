"""Tests for the registry-mode image-build task body."""

import pytest

from ..runner_config import ImageBuild
from . import image_build_task
from .image_build_task import build_task_name, run_image_build


def test_build_task_name_carries_repo_branch_and_short_sha():
    source = ImageBuild(
        git_remote="https://example.com/org/myrepo.git", git_sha="abcdef1234" + "0" * 30, git_branch="main"
    )
    assert build_task_name(source) == "build-image:myrepo@main:abcdef12"


def test_build_task_name_handles_scp_remote_and_omits_unknown_branch():
    source = ImageBuild(git_remote="git@example.com:org/myrepo.git", git_sha="a" * 40)
    assert build_task_name(source) == f"build-image:myrepo:{'a' * 8}"


def test_build_task_name_appends_non_default_dockerfile():
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="b" * 40, dockerfile="Dockerfile.gpu")
    assert build_task_name(source).endswith(" (Dockerfile.gpu)")


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


async def test_run_image_build_requires_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_REGISTRY"):
        await run_image_build(git_remote="https://example.com/r.git", git_sha="a" * 40)
