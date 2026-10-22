"""Low-level build helpers for Docker-runner jobs.

Used by the injected image-build task (``execution.image_build_task``);
``resolve_launch_image`` only derives the tag a container launches with.

Builds run on buildkitd (``AAICLICK_BUILDKIT_ADDR``) through the buildx
remote driver, never on the host daemon: Dockerfile steps run in buildkitd's
sandbox, away from the worker's credentials and the daemon that runs tasks.

Cache check, then build (first hit short-circuits):

1. registry mode: ``docker buildx imagetools inspect <tag>`` finds it.
2. local mode: ``docker image inspect <tag>`` finds it in the host daemon.
3. Fall through: ``git clone`` at SHA, ``docker buildx build`` with
   ``--push`` (registry) or ``--load`` (local).
"""

from __future__ import annotations

import functools
import hashlib
import importlib.metadata
import os
import tempfile
from pathlib import Path

from ...ghcr import base_image
from ..docker_config import add_host_flags, compute_image_tag, get_buildkit_addr, get_registry
from ..runner_config import ImageBuild, ImageSourceT
from . import cli
from .docker_scaffold import detect_manifest, render_default_dockerfile


def _docker_bin() -> str:
    return os.environ.get("AAICLICK_DOCKER_BIN", "docker")


_TLS_ENV = {
    "cacert": "AAICLICK_BUILDKIT_TLS_CACERT",
    "cert": "AAICLICK_BUILDKIT_TLS_CERT",
    "key": "AAICLICK_BUILDKIT_TLS_KEY",
}


def _tls_opts() -> list[str]:
    """Remote-driver TLS options for the client cert/key/CA paths that are set."""
    opts = [f"{opt}={os.environ[env]}" for opt, env in _TLS_ENV.items() if os.environ.get(env)]
    return ["--driver-opt", ",".join(opts)] if opts else []


def _builder_name() -> str:
    """buildx builder for the current address + TLS settings — keyed by them, so
    changing either creates a fresh builder instead of reusing a stale one."""
    config = "\x00".join([get_buildkit_addr(), *_tls_opts()])
    return f"aaiclick-{hashlib.sha256(config.encode()).hexdigest()[:12]}"


async def _require_builder() -> None:
    """Preflight the build: the buildx builder for ``AAICLICK_BUILDKIT_ADDR``
    exists (created on first use) and its buildkitd answers.

    A concurrent create on the same worker loses with "existing instance",
    which is fine; the bootstrap inspect is the check that counts."""
    addr = get_buildkit_addr()
    name = _builder_name()
    bin_ = _docker_bin()
    try:
        rc, _, _ = await cli.run(bin_, "buildx", "inspect", name, check=False, stream=False)
    except FileNotFoundError as e:
        raise RuntimeError(
            f"Docker CLI {bin_!r} not found on this worker, but this job builds its "
            f"image from source. Install the docker CLI with buildx, set "
            f"AAICLICK_DOCKER_BIN to the docker binary, or submit the job with a "
            f"prebuilt image (image=...)."
        ) from e
    create_err = ""
    if rc != 0:
        rc, _, create_err = await cli.run(
            bin_,
            "buildx",
            "create",
            "--name",
            name,
            "--driver",
            "remote",
            *_tls_opts(),
            addr,
            check=False,
            stream=False,
        )
        if rc != 0 and "existing instance" not in create_err:
            raise RuntimeError(f"buildx create failed: {create_err.strip()}")
    rc, _, stderr = await cli.run(bin_, "buildx", "inspect", "--bootstrap", name, check=False, stream=False)
    if rc != 0:
        raise RuntimeError(
            f"BuildKit daemon at {addr} (AAICLICK_BUILDKIT_ADDR) is not reachable from this worker: "
            f"{stderr.strip()}. Start buildkitd there, or fix the address."
        )


async def _docker_image_exists_locally(image_tag: str) -> bool:
    rc, _, _ = await cli.run(_docker_bin(), "image", "inspect", image_tag, check=False)
    return rc == 0


async def _registry_has_image(image_tag: str) -> bool:
    """Whether the registry already holds ``image_tag`` — asked of the registry
    directly, so registry mode needs no host daemon. A miss only costs a rebuild."""
    rc, _, _ = await cli.run(_docker_bin(), "buildx", "imagetools", "inspect", image_tag, check=False, stream=False)
    return rc == 0


async def _host_gateway_ip() -> str:
    """The docker bridge gateway, standing in for ``host-gateway``."""
    rc, out, _ = await cli.run(
        _docker_bin(),
        "network",
        "inspect",
        "bridge",
        "--format",
        "{{(index .IPAM.Config 0).Gateway}}",
        check=False,
        stream=False,
    )
    if rc != 0 or not out.strip():
        raise RuntimeError(
            "AAICLICK_DOCKER_BUILD_ADD_HOST uses host-gateway, which buildkitd cannot resolve and this "
            "worker has no docker daemon to look up. Write the host's IP literally (host:1.2.3.4)."
        )
    return out.strip()


async def _git_clone_at_sha(remote: str, sha: str, workdir: str) -> None:
    """Clone the SHA into ``workdir``. Uses ``git init`` + ``fetch`` + ``checkout``
    so we avoid pulling the full default branch when only one commit is needed,
    and so the remote can be a non-default-branch SHA."""
    await cli.run("git", "init", "--quiet", workdir)
    await cli.run("git", "-C", workdir, "remote", "add", "origin", "--", remote)
    await cli.run("git", "-C", workdir, "fetch", "--depth=1", "--quiet", "origin", "--", sha)
    await cli.run("git", "-C", workdir, "checkout", "--quiet", sha)


@functools.cache
def _aaiclick_version() -> str:
    """Best-effort version of the running aaiclick package, for the
    ``AAICLICK_VERSION`` build-arg and the default Dockerfile's base tag.
    Cached: the installed version cannot change within a process."""
    try:
        return importlib.metadata.version("aaiclick")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0"


def _collect_build_args(source: ImageBuild) -> list[str]:
    """Build the ``["--build-arg", "K=V", ...]`` slice for ``docker build``.

    Only emits args whose value is non-empty / non-None — leaves the
    user's Dockerfile defaults in charge for missing values."""
    args: list[str] = []

    def add(key: str, value: str | None) -> None:
        if value:
            args.extend(["--build-arg", f"{key}={value}"])

    add("GIT_REMOTE", source.git_remote)
    add("GIT_SHA", source.git_sha)
    add("GIT_BRANCH", source.git_branch)
    add("PIP_TRUSTED_HOST", os.environ.get("AAICLICK_PIP_TRUSTED_HOST"))
    version = _aaiclick_version()
    add("AAICLICK_VERSION", version)
    add("BASE_IMAGE", os.environ.get("AAICLICK_BASE_IMAGE") or base_image(version))
    return args


_SECRET_ENV = {"pip_index_url": "AAICLICK_PIP_INDEX_URL", "pip_extra_index_url": "AAICLICK_PIP_EXTRA_INDEX_URL"}


def _collect_secret_flags() -> list[str]:
    """``--secret`` flags for the index URLs that are set. They may embed a
    token, so a build step sees them only where it mounts them, never as a
    build-arg that lands in image history."""
    flags: list[str] = []
    for secret_id, env in _SECRET_ENV.items():
        if os.environ.get(env):
            flags.extend(["--secret", f"id={secret_id},env={env}"])
    return flags


async def _buildx_build(context: str, dockerfile: str, image_tag: str, build_args: list[str], *, push: bool) -> None:
    gateway_ip = None
    if "host-gateway" in os.environ.get("AAICLICK_DOCKER_BUILD_ADD_HOST", ""):
        gateway_ip = await _host_gateway_ip()
    cmd = [
        _docker_bin(),
        "buildx",
        "build",
        "--builder",
        _builder_name(),
        "-t",
        image_tag,
        "-f",
        dockerfile,
        *build_args,
        *_collect_secret_flags(),
        *add_host_flags("AAICLICK_DOCKER_BUILD_ADD_HOST", gateway_ip=gateway_ip),
        "--push" if push else "--load",
        context,
    ]
    await cli.run(*cmd)


def _resolve_dockerfile(source: ImageBuild, context_dir: Path) -> Path:
    """Path of the Dockerfile to build with inside the checkout.

    A checkout without the implicit ``Dockerfile`` gets a default written in,
    rendered for its root manifest (``docker_scaffold.detect_manifest``), plus a
    ``.dockerignore`` (when the repo has none) keeping the clone's ``.git`` out
    of the context — user Dockerfiles keep it, since a ``pip install`` of a
    setuptools-scm project reads it. An explicitly named ``dockerfile=`` that
    is absent, or that resolves outside the checkout, is a user error and raises."""
    dockerfile = context_dir / (source.dockerfile or "Dockerfile")
    if not dockerfile.resolve().is_relative_to(context_dir.resolve()):
        raise ValueError(f"dockerfile={source.dockerfile!r} must be a path inside the repo")
    if dockerfile.is_file():
        return dockerfile
    if source.dockerfile:
        raise FileNotFoundError(
            f"Dockerfile not found at {source.dockerfile} in repo {source.git_remote}@{source.git_sha}. "
            f"Check the dockerfile= path, or omit it to build with the repo's Dockerfile "
            f"(or the default aaiclick layer when there is none)."
        )
    try:
        kind = detect_manifest(context_dir)
    except ValueError as e:
        raise ValueError(f"{e} (repo {source.git_remote}@{source.git_sha})") from e
    dockerfile.write_text(render_default_dockerfile(kind))
    dockerignore = context_dir / ".dockerignore"
    if not dockerignore.exists():
        dockerignore.write_text(".git\n")
    return dockerfile


async def resolve_launch_image(image_source: ImageSourceT | None, *, task_id: int) -> str:
    """Resolve the image tag a container actually launches with.

    The tag is derived from the source (prebuilt tag verbatim, computed
    ``aaiclick-job:<sha>-<key>`` for a build). Never builds: the ``build >> task``
    dependency edge guarantees the build task has already pushed the tag
    (registry mode) or left it in this host's daemon (local mode), so the
    launch path pulls or runs directly (spec: docs/designs/orchestration.md
    "Image source").

    A ``None`` source cannot occur through dispatch (``_resolve_dispatch``
    routes NULL-image tasks to the subprocess vehicle); the raise here is the
    single owner of that invariant for all container launch paths."""
    if image_source is None:
        raise ValueError(f"container task {task_id} has no image_source")
    if isinstance(image_source, ImageBuild):
        return compute_image_tag(image_source)
    return image_source.image_tag


async def build_image_to_tag(source: ImageBuild, image_tag: str) -> None:
    """Ensure ``image_tag`` exists where dependents launch it — the registry,
    or this host's daemon in local mode — building ``source`` on buildkitd if
    it is missing. Idempotent: the tag is content-addressed by build identity."""
    registry = get_registry()
    if registry:
        if await _registry_has_image(image_tag):
            return
    elif await _docker_image_exists_locally(image_tag):
        return

    await _require_builder()
    with tempfile.TemporaryDirectory(prefix="aaiclick-build-") as workdir:
        await _git_clone_at_sha(source.git_remote, source.git_sha, workdir)

        context_dir = Path(workdir)
        dockerfile = _resolve_dockerfile(source, context_dir)

        build_args = _collect_build_args(source)
        await _buildx_build(str(context_dir), str(dockerfile), image_tag, build_args, push=registry is not None)
