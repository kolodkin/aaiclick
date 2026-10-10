import subprocess
from unittest.mock import AsyncMock

import pytest

from aaiclick.orchestration import docker_config
from aaiclick.orchestration.docker_config import (
    BUILD_MODE_LOCAL,
    BUILD_MODE_REGISTRY,
    GitDetectionError,
    RemoteHead,
    compute_image_tag,
    get_build_mode,
    image_key,
    resolve_image_source,
    resolve_remote_head,
    resource_flags,
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
            ImageBuild(git_remote="git@reg:r.git", git_sha="d" * 40, git_branch="remote-main", dockerfile="D"),
            id="registered-build-resolves-remote-head",
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
            ImageBuild(git_remote="git@override:r.git", git_sha="b" * 40, git_branch=None, dockerfile="D"),
            id="run-modifiers-override-registered-defaults",
        ),
        pytest.param(
            None,
            {"git_remote": "git@sandbox:r.git"},
            ImageBuild(git_remote="git@sandbox:r.git", git_sha="d" * 40, git_branch="remote-main"),
            id="run-remote-without-sha-resolves-remote-head",
        ),
        pytest.param(
            None,
            {"git_remote": "git@sandbox:r.git", "git_branch": "feature"},
            ImageBuild(git_remote="git@sandbox:r.git", git_sha="d" * 40, git_branch="feature"),
            id="run-remote-and-branch-without-sha-resolves-branch-head",
        ),
    ],
)
async def test_resolve_image_source(monkeypatch, registered, kwargs, expected):
    monkeypatch.setattr(docker_config, "auto_detect_git_remote", AsyncMock(return_value="git@auto:r.git"))
    monkeypatch.setattr(docker_config, "auto_detect_git_sha", AsyncMock(return_value="c" * 40))
    monkeypatch.setattr(docker_config, "auto_detect_git_branch", AsyncMock(return_value="auto"))

    async def fake_remote_head(remote: str, branch: str | None) -> RemoteHead:
        return RemoteHead(sha="d" * 40, branch=branch or "remote-main")

    monkeypatch.setattr(docker_config, "resolve_remote_head", fake_remote_head)
    assert await resolve_image_source(registered, **kwargs) == expected


async def test_resolve_image_source_known_remote_never_reads_working_tree(monkeypatch):
    """A sandbox URL names a repo the submitter's cwd need not contain: the
    scheduler or an API server has no working tree at all."""
    tree = AsyncMock(side_effect=GitDetectionError("not a git repository"))
    monkeypatch.setattr(docker_config, "auto_detect_git_remote", tree)
    monkeypatch.setattr(docker_config, "auto_detect_git_sha", tree)
    monkeypatch.setattr(docker_config, "auto_detect_git_branch", tree)
    monkeypatch.setattr(docker_config, "resolve_remote_head", AsyncMock(return_value=RemoteHead("d" * 40, "main")))
    source = await resolve_image_source(None, git_remote="git@sandbox:r.git")
    assert source == ImageBuild(git_remote="git@sandbox:r.git", git_sha="d" * 40, git_branch="main")
    tree.assert_not_awaited()


def _git(*args: str, cwd: str) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def remote_repo(tmp_path):
    """A bare-ish local repo with ``main`` (two commits) and ``feature`` (one more)."""
    repo = tmp_path / "remote"
    repo.mkdir()
    _git("init", "--quiet", "--initial-branch=main", cwd=str(repo))
    _git("config", "user.email", "t@example.com", cwd=str(repo))
    _git("config", "user.name", "t", cwd=str(repo))
    (repo / "a").write_text("1")
    _git("add", "a", cwd=str(repo))
    _git("commit", "--quiet", "-m", "one", cwd=str(repo))
    main_sha = _git("rev-parse", "HEAD", cwd=str(repo))
    _git("checkout", "--quiet", "-b", "feature", cwd=str(repo))
    (repo / "b").write_text("2")
    _git("add", "b", cwd=str(repo))
    _git("commit", "--quiet", "-m", "two", cwd=str(repo))
    feature_sha = _git("rev-parse", "HEAD", cwd=str(repo))
    _git("checkout", "--quiet", "main", cwd=str(repo))
    return str(repo), main_sha, feature_sha


async def test_resolve_remote_head_default_branch(remote_repo):
    url, main_sha, _ = remote_repo
    assert await resolve_remote_head(url, None) == RemoteHead(sha=main_sha, branch="main")


async def test_resolve_remote_head_named_branch(remote_repo):
    url, _, feature_sha = remote_repo
    assert await resolve_remote_head(url, "feature") == RemoteHead(sha=feature_sha, branch="feature")


async def test_resolve_remote_head_missing_branch(remote_repo):
    url, _, _ = remote_repo
    with pytest.raises(GitDetectionError, match="no-such-branch"):
        await resolve_remote_head(url, "no-such-branch")


async def test_resolve_remote_head_unreachable_remote(tmp_path):
    with pytest.raises(GitDetectionError, match="ls-remote"):
        await resolve_remote_head(str(tmp_path / "nowhere"), None)


def test_compute_image_tag_without_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    assert compute_image_tag("b" * 40) == f"aaiclick-job:{'b' * 40}"


@pytest.mark.parametrize(
    "resources, expected",
    [
        pytest.param(None, [], id="none"),
        pytest.param({}, [], id="empty"),
        pytest.param({"requests": {"cpu": "1"}}, [], id="requests-only"),
        pytest.param({"limits": {"cpu": "500m"}}, ["--cpus", "0.5"], id="cpu-millis"),
        pytest.param({"limits": {"cpu": "2"}}, ["--cpus", "2"], id="cpu-whole"),
        pytest.param({"limits": {"cpu": 1.5}}, ["--cpus", "1.5"], id="cpu-json-number"),
        pytest.param({"limits": {"memory": "512Mi"}}, ["--memory", str(512 * 1024**2)], id="mem-binary"),
        pytest.param({"limits": {"memory": "1G"}}, ["--memory", "1000000000"], id="mem-decimal"),
        pytest.param({"limits": {"memory": "128974848"}}, ["--memory", "128974848"], id="mem-bytes"),
        pytest.param({"limits": {"memory": "1e9"}}, ["--memory", "1000000000"], id="mem-exponent"),
        pytest.param({"limits": {"memory": "1.5Gi"}}, ["--memory", str(3 * 512 * 1024**2)], id="mem-fraction"),
        pytest.param(
            {"limits": {"cpu": "250m", "memory": "2Gi"}},
            ["--cpus", "0.25", "--memory", str(2 * 1024**3)],
            id="both",
        ),
    ],
)
def test_resource_flags(resources, expected):
    assert resource_flags(resources) == expected


@pytest.mark.parametrize(
    "resources, match",
    [
        pytest.param({"limits": {"cpu": "fast"}}, "limits.cpu", id="cpu-garbage"),
        pytest.param({"limits": {"cpu": "1Gi"}}, "limits.cpu", id="cpu-memory-suffix"),
        pytest.param({"limits": {"memory": "1Xi"}}, "limits.memory", id="mem-bad-suffix"),
        pytest.param({"limits": {"memory": "-1Gi"}}, "limits.memory", id="mem-negative"),
        pytest.param({"limits": {"memory": ""}}, "limits.memory", id="mem-empty"),
    ],
)
def test_resource_flags_rejects_malformed_quantity(resources, match):
    with pytest.raises(ValueError, match=match):
        resource_flags(resources)


def test_resource_flags_warns_on_requests(caplog):
    """Docker has no scheduler, so ``requests`` cannot be honoured; say so
    rather than silently dropping them."""
    with caplog.at_level("WARNING", logger="aaiclick.orchestration.docker_config"):
        flags = resource_flags({"requests": {"cpu": "1"}, "limits": {"cpu": "2"}})
    assert flags == ["--cpus", "2"]
    assert "requests" in caplog.text and "docker" in caplog.text


def test_resource_flags_limits_only_does_not_warn(caplog):
    with caplog.at_level("WARNING", logger="aaiclick.orchestration.docker_config"):
        resource_flags({"limits": {"cpu": "2"}})
    assert caplog.text == ""
