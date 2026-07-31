"""Tests for the registry-mode image-build task body."""

import pytest

from ..runner_config import ImageBuild
from . import image_build_task
from .image_build_task import IMAGE_BUILD_ENTRYPOINT, build_task_name, is_image_build_task, run_image_build


def test_is_image_build_task_matches_only_the_constant():
    assert is_image_build_task(IMAGE_BUILD_ENTRYPOINT)
    assert not is_image_build_task("myapp.pipelines.etl_job")


def test_build_task_name_uses_short_sha():
    assert build_task_name("abcdef1234" + "0" * 30) == "build-image:abcdef12"


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
