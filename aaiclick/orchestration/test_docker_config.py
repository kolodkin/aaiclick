import pytest

from aaiclick.orchestration.runner_config import DockerRunner, ImageBuild, ImagePrebuilt
from aaiclick.orchestration.docker_config import effective_image_tag, resolve_runner_config


async def test_resolve_prebuilt_image_skips_git(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    cfg = await resolve_runner_config(
        registered=None, runner_mode="docker", image="python:3.12", git_remote=None, git_sha=None
    )
    assert isinstance(cfg, DockerRunner)
    assert isinstance(cfg.image, ImagePrebuilt)
    assert effective_image_tag(cfg) == "python:3.12"


async def test_resolve_build_image_computes_tag(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    cfg = await resolve_runner_config(
        registered=None, runner_mode="docker", image=None,
        git_remote="git@x:r.git", git_sha="b" * 40, git_branch="main",
    )
    assert isinstance(cfg.image, ImageBuild)
    assert effective_image_tag(cfg) == f"aaiclick-job:{'b' * 40}"
