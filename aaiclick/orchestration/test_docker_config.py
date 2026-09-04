import pytest

from aaiclick.orchestration.docker_config import (
    BUILD_MODE_LOCAL,
    BUILD_MODE_REGISTRY,
    compute_image_tag,
    get_build_mode,
    image_key,
    resolve_image_source,
    resolve_runner_config,
)
from aaiclick.orchestration.models import RegisteredJob
from aaiclick.orchestration.runner_config import (
    DockerRunner,
    ImageBuild,
    ImagePrebuilt,
    KubernetesRunner,
)


async def test_resolve_prebuilt_image_skips_git(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    source = await resolve_image_source(
        None, image="python:3.12", git_remote=None, git_sha=None, git_branch=None, dockerfile=None
    )
    assert isinstance(source, ImagePrebuilt)
    assert source.image_tag == "python:3.12"


async def test_resolve_build_image_computes_tag(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    source = await resolve_image_source(
        None, image=None, git_remote="git@x:r.git", git_sha="b" * 40, git_branch="main", dockerfile=None
    )
    assert isinstance(source, ImageBuild)
    assert compute_image_tag(source.git_sha) == f"aaiclick-job:{'b' * 40}"


async def test_registered_prebuilt_image_default():
    registered = RegisteredJob(id=1, name="j", entrypoint="m.e", image="ghcr.io/x/y:1")
    source = await resolve_image_source(
        registered, image=None, git_remote=None, git_sha=None, git_branch=None, dockerfile=None
    )
    assert isinstance(source, ImagePrebuilt)
    assert source.image_tag == "ghcr.io/x/y:1"


def test_resolve_kubernetes_runner_preserves_resources():
    cfg = resolve_runner_config(
        runner_mode="kubernetes",
        kubernetes_config={"namespace": "ml", "resources": {"limits": {"cpu": "2"}}},
    )
    assert isinstance(cfg, KubernetesRunner)
    assert cfg.namespace == "ml"
    assert cfg.resources == {"limits": {"cpu": "2"}}


def test_resolve_docker_runner_is_bare_marker():
    cfg = resolve_runner_config(runner_mode="docker")
    assert isinstance(cfg, DockerRunner)


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
