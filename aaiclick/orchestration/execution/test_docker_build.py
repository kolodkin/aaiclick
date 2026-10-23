"""Tests for the docker_build task."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import ANY, AsyncMock

import pytest

from .. import docker_config
from ..runner_config import ImageBuild, ImagePrebuilt
from . import docker_build
from .docker_build import resolve_launch_image
from .docker_scaffold import MANIFEST_NONE, MANIFEST_REQUIREMENTS, render_default_dockerfile


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


async def test_index_urls_are_secrets_not_build_args(monkeypatch):
    monkeypatch.setenv("AAICLICK_PIP_INDEX_URL", "http://tok@pypi.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_EXTRA_INDEX_URL", "http://extra.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_TRUSTED_HOST", "pypi.test")

    args = docker_build._collect_build_args(ImageBuild(git_remote="https://e/r.git", git_sha="a" * 40))

    assert not any("pypi.test/simple" in a or "extra.test" in a for a in args)
    assert "PIP_TRUSTED_HOST=pypi.test" in args
    assert docker_build._collect_secret_flags() == [
        "--secret",
        "id=pip_index_url,env=AAICLICK_PIP_INDEX_URL",
        "--secret",
        "id=pip_extra_index_url,env=AAICLICK_PIP_EXTRA_INDEX_URL",
    ]


async def test_collect_secret_flags_omits_unset(monkeypatch):
    monkeypatch.delenv("AAICLICK_PIP_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_PIP_EXTRA_INDEX_URL", raising=False)
    assert docker_build._collect_secret_flags() == []


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
        await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))


@pytest.mark.parametrize("dockerfile", ["/etc/passwd", "../outside/Dockerfile"])
async def test_build_image_to_tag_dockerfile_outside_checkout_raises(monkeypatch, dockerfile):
    """A declared ``dockerfile`` must stay inside the checkout: buildx would
    otherwise read a worker-host file and echo it in parse errors."""
    _stub_build_path(monkeypatch, {})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40, dockerfile=dockerfile)

    with pytest.raises(ValueError, match="inside the repo"):
        await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))


def _stub_build_path(monkeypatch, clone_files: dict[str, str]) -> list[dict[str, str]]:
    """Stub everything around the clone + build step and return a list collecting,
    per ``_buildx_build`` call, the checkout's files by name (read at call time,
    before the temp checkout is removed). The Dockerfile must sit in the context."""
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    monkeypatch.setenv("AAICLICK_LOCAL_BUILD", "1")
    monkeypatch.setattr(docker_build, "_require_builder", AsyncMock())
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=False))

    async def fake_clone(remote, sha, workdir):
        for name, content in clone_files.items():
            Path(workdir, name).write_text(content)

    built: list[dict[str, str]] = []

    async def fake_build(context, dockerfile, image_tag, build_args, *, push):
        assert Path(dockerfile) == Path(context) / "Dockerfile"
        built.append({f.name: f.read_text() for f in Path(context).iterdir()})

    monkeypatch.setattr(docker_build, "_git_clone_at_sha", fake_clone)
    monkeypatch.setattr(docker_build, "_buildx_build", fake_build)
    return built


@pytest.mark.parametrize(
    "clone_files, kind",
    [
        pytest.param({"job.py": "print('hi')\n"}, MANIFEST_NONE, id="thin-layer"),
        pytest.param({"requirements.txt": "six\n"}, MANIFEST_REQUIREMENTS, id="manifest"),
    ],
)
async def test_build_image_to_tag_repo_without_dockerfile_builds_with_default(monkeypatch, clone_files, kind):
    """A checkout with no ``Dockerfile`` gets the default for its root manifest
    written in, plus a ``.dockerignore`` keeping ``.git`` out of the image."""
    built = _stub_build_path(monkeypatch, clone_files)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))

    assert built == [{**clone_files, "Dockerfile": render_default_dockerfile(kind), ".dockerignore": ".git\n"}]


async def test_build_image_to_tag_default_keeps_repo_dockerignore(monkeypatch):
    built = _stub_build_path(monkeypatch, {".dockerignore": "data/\n"})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))

    assert built[0][".dockerignore"] == "data/\n"


async def test_build_image_to_tag_checked_in_dockerfile_wins_over_default(monkeypatch):
    """A checked-in Dockerfile builds as is: nothing is written into the checkout."""
    built = _stub_build_path(monkeypatch, {"Dockerfile": "FROM python:3.12\n", "requirements.txt": "six\n"})
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))

    assert built == [{"Dockerfile": "FROM python:3.12\n", "requirements.txt": "six\n"}]


@pytest.mark.parametrize(
    "version, env_base_image, expected",
    [
        pytest.param("1.2.3", None, "ghcr.io/kolodkin/aaiclick:v1.2.3", id="release"),
        pytest.param("0.0.1.dev50+gfc8c68213.d20261008", None, "ghcr.io/kolodkin/aaiclick:v0.0.1.dev50", id="dev"),
        pytest.param("1.2.3", "ghcr.io/kolodkin/aaiclick:v1.2.3-rc", "ghcr.io/kolodkin/aaiclick:v1.2.3-rc", id="env"),
    ],
)
def test_collect_build_args_base_image(monkeypatch, version, env_base_image, expected):
    """``AAICLICK_BASE_IMAGE`` wins verbatim, else the GHCR tag for the host version."""
    monkeypatch.setattr(docker_build, "_aaiclick_version", lambda: version)
    monkeypatch.delenv("AAICLICK_BASE_IMAGE", raising=False)
    if env_base_image:
        monkeypatch.setenv("AAICLICK_BASE_IMAGE", env_base_image)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    assert f"BASE_IMAGE={expected}" in docker_build._collect_build_args(source)


def _fake_cli(monkeypatch, rcs: dict[tuple[str, ...], int]) -> list[tuple[str, ...]]:
    """Stub ``cli.run``: each argv's rc is looked up by its leading words after
    the binary (default 0); returns the recorded argvs."""
    calls: list[tuple[str, ...]] = []

    async def run(*cmd, check=True, stream=True, cwd=None, env=None):
        calls.append(cmd)
        rc = next((v for k, v in rcs.items() if cmd[1 : 1 + len(k)] == k), 0)
        return rc, "", "boom" if rc else ""

    monkeypatch.setattr(docker_build.cli, "run", run)
    return calls


async def test_require_builder_creates_remote_builder_on_miss(monkeypatch):
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    name = docker_build._builder_name()
    calls = _fake_cli(monkeypatch, {("buildx", "inspect", name): 1})

    await docker_build._require_builder()

    assert ("docker", "buildx", "create", "--name", name, "--driver", "remote", "tcp://buildkitd:1234") in calls
    assert calls[-1] == ("docker", "buildx", "inspect", "--bootstrap", name)


def test_builder_name_follows_address(monkeypatch):
    """A changed address gets a fresh builder instead of reusing the stale one."""
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://old:1234")
    old = docker_build._builder_name()
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://new:1234")
    assert docker_build._builder_name() != old


async def test_require_builder_reports_create_failure(monkeypatch):
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    _fake_cli(monkeypatch, {("buildx", "inspect"): 1, ("buildx", "create"): 1})

    with pytest.raises(RuntimeError, match="buildx create failed: boom"):
        await docker_build._require_builder()


async def test_require_builder_raises_without_addr(monkeypatch):
    monkeypatch.delenv("AAICLICK_BUILDKIT_ADDR", raising=False)
    with pytest.raises(RuntimeError, match="AAICLICK_BUILDKIT_ADDR"):
        await docker_build._require_builder()


async def test_require_builder_raises_when_unreachable(monkeypatch):
    """An unreachable buildkitd fails the build; nothing falls back to the host daemon."""
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    calls = _fake_cli(monkeypatch, {("buildx", "inspect", "--bootstrap"): 1})

    with pytest.raises(RuntimeError, match="tcp://buildkitd:1234"):
        await docker_build._require_builder()

    assert all(c[1] == "buildx" for c in calls)


async def test_require_builder_raises_clear_error_when_cli_missing(monkeypatch):
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    monkeypatch.setattr(docker_build.cli, "run", AsyncMock(side_effect=FileNotFoundError(2, "No such file", "docker")))

    with pytest.raises(RuntimeError, match="Docker CLI 'docker' not found on this worker"):
        await docker_build._require_builder()


def _stub_modes(monkeypatch, *, registry: str | None, have_image: bool) -> AsyncMock:
    if registry:
        monkeypatch.setenv("AAICLICK_REGISTRY", registry)
        monkeypatch.delenv("AAICLICK_LOCAL_BUILD", raising=False)
    else:
        monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
        monkeypatch.setenv("AAICLICK_LOCAL_BUILD", "1")
    monkeypatch.setattr(docker_build, "_require_builder", AsyncMock())
    monkeypatch.setattr(docker_build, "_registry_has_image", AsyncMock(return_value=have_image))
    monkeypatch.setattr(docker_build, "_docker_image_exists_locally", AsyncMock(return_value=have_image))
    monkeypatch.setattr(docker_build, "_git_clone_at_sha", AsyncMock())
    monkeypatch.setattr(docker_build, "_resolve_dockerfile", lambda source, ctx: ctx / "Dockerfile")
    build = AsyncMock()
    monkeypatch.setattr(docker_build, "_buildx_build", build)
    return build


@pytest.mark.parametrize(
    "registry, push",
    [pytest.param("registry.example:5000", True, id="registry"), pytest.param(None, False, id="local")],
)
async def test_build_image_to_tag_builds_on_miss(monkeypatch, registry, push):
    build = _stub_modes(monkeypatch, registry=registry, have_image=False)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))

    build.assert_awaited_once_with(ANY, ANY, ANY, ANY, push=push)


@pytest.mark.parametrize(
    "registry", [pytest.param("registry.example:5000", id="registry"), pytest.param(None, id="local")]
)
async def test_build_image_to_tag_cache_hit_skips_build(monkeypatch, registry):
    """A hit needs neither a build nor buildkitd."""
    build = _stub_modes(monkeypatch, registry=registry, have_image=True)
    source = ImageBuild(git_remote="https://example.com/repo.git", git_sha="a" * 40)

    await docker_build.build_image_to_tag(source, docker_config.compute_image_tag(source))

    build.assert_not_awaited()
    docker_build._require_builder.assert_not_awaited()


@pytest.mark.parametrize("push, flag, absent", [(True, "--push", "--load"), (False, "--load", "--push")])
async def test_buildx_build_argv(monkeypatch, push, flag, absent):
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    monkeypatch.setenv("AAICLICK_PIP_INDEX_URL", "http://tok@pypi.test/simple/")
    monkeypatch.delenv("AAICLICK_PIP_EXTRA_INDEX_URL", raising=False)
    monkeypatch.delenv("AAICLICK_DOCKER_BUILD_ADD_HOST", raising=False)
    calls = _fake_cli(monkeypatch, {})

    await docker_build._buildx_build("/ctx", "/ctx/Dockerfile", "reg/aaiclick-job:x", ["--build-arg", "A=1"], push=push)

    (argv,) = calls
    assert argv[:5] == ("docker", "buildx", "build", "--builder", docker_build._builder_name())
    assert flag in argv and absent not in argv
    assert "id=pip_index_url,env=AAICLICK_PIP_INDEX_URL" in argv
    assert argv[-1] == "/ctx"


async def test_buildx_build_resolves_host_gateway(monkeypatch):
    """buildkitd does not know dockerd's ``host-gateway`` keyword, so the worker
    passes the gateway IP itself."""
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    monkeypatch.setenv("AAICLICK_DOCKER_BUILD_ADD_HOST", "host.docker.internal:host-gateway")
    monkeypatch.setattr(docker_build, "_host_gateway_ip", AsyncMock(return_value="172.17.0.1"))
    calls = _fake_cli(monkeypatch, {})

    await docker_build._buildx_build("/ctx", "/ctx/Dockerfile", "t", [], push=True)

    assert "host.docker.internal:172.17.0.1" in calls[0]
    assert not any("host-gateway" in a for a in calls[0])


async def test_resolve_launch_image_prebuilt_tag_verbatim():
    source = ImagePrebuilt(image_tag="ghcr.io/x/y:1")
    assert await resolve_launch_image(source, task_id=1) == "ghcr.io/x/y:1"


async def test_resolve_launch_image_rejects_missing_source():
    with pytest.raises(ValueError, match="no image_source"):
        await resolve_launch_image(None, task_id=42)


@pytest.mark.parametrize(
    "registry, expected_prefix",
    [
        # Empty reads as unset (``get_registry``): the local-build mode.
        pytest.param("", "aaiclick-job:", id="no-registry"),
        pytest.param("registry.example:5000", "registry.example:5000/aaiclick-job:", id="registry"),
    ],
)
async def test_resolve_launch_image_never_builds(monkeypatch, registry, expected_prefix):
    """The build task in the graph owns the build in both modes (its dependency
    edge guaranteed the push); launch only computes the tag."""
    build = AsyncMock()
    monkeypatch.setenv("AAICLICK_REGISTRY", registry)
    monkeypatch.setattr(docker_build, "build_image_to_tag", build)
    source = ImageBuild(git_remote="https://example.com/r.git", git_sha="a" * 40)

    expected_tag = f"{expected_prefix}{'a' * 40}-{docker_config.image_key(source)[:12]}"
    assert await resolve_launch_image(source, task_id=1) == expected_tag
    build.assert_not_awaited()


async def test_require_builder_passes_tls_opts(monkeypatch):
    monkeypatch.setenv("AAICLICK_BUILDKIT_ADDR", "tcp://buildkitd:1234")
    monkeypatch.setenv("AAICLICK_BUILDKIT_TLS_CACERT", "/certs/ca.pem")
    monkeypatch.delenv("AAICLICK_BUILDKIT_TLS_CERT", raising=False)
    monkeypatch.setenv("AAICLICK_BUILDKIT_TLS_KEY", "/certs/key.pem")
    calls = _fake_cli(monkeypatch, {("buildx", "inspect", docker_build._builder_name()): 1})

    await docker_build._require_builder()

    create = next(c for c in calls if c[2] == "create")
    assert create[-3:] == ("--driver-opt", "cacert=/certs/ca.pem,key=/certs/key.pem", "tcp://buildkitd:1234")
