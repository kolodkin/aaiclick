from aaiclick.orchestration.docker_config import effective_image_tag, image_key, resolve_runner_config
from aaiclick.orchestration.runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    KubernetesRunner,
)


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
        registered=None,
        runner_mode="docker",
        image=None,
        git_remote="git@x:r.git",
        git_sha="b" * 40,
        git_branch="main",
    )
    assert isinstance(cfg, DockerRunner)
    assert isinstance(cfg.image, ImageBuild)
    assert effective_image_tag(cfg) == f"aaiclick-job:{'b' * 40}"


async def test_resolve_kubernetes_runner_preserves_resources(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    cfg = await resolve_runner_config(
        registered=None,
        runner_mode="kubernetes",
        image="python:3.12",
        kubernetes_config={"namespace": "ml", "resources": {"limits": {"cpu": "2"}}},
    )
    assert isinstance(cfg, KubernetesRunner)
    assert cfg.namespace == "ml"
    assert cfg.resources == {"limits": {"cpu": "2"}}


def test_image_key_stable_and_distinguishes_fields():
    a = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, dockerfile=None)
    a_again = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, git_branch="ignored", dockerfile=None)
    b = ImageBuild(git_remote="git@x:r.git", git_sha="b" * 40, dockerfile=None)
    c = ImageBuild(git_remote="git@x:r.git", git_sha="a" * 40, dockerfile="Dockerfile.gpu")

    assert len(image_key(a)) == 64
    assert image_key(a) == image_key(a_again)  # git_branch is not part of identity
    assert image_key(a) != image_key(b)  # sha matters
    assert image_key(a) != image_key(c)  # dockerfile matters
