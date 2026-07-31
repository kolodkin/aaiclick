"""Low-level build helpers for Docker-runner jobs.

Used by the injected image-build task (``execution.image_build_task``) in
registry mode, and by ``resolve_launch_image`` for the inline no-registry
build at container launch.

Cache hierarchy (first hit short-circuits):

1. ``AAICLICK_REGISTRY`` set + ``docker pull`` succeeds — image is
   now in the local daemon.
2. ``docker image inspect <tag>`` succeeds — local cache hit.
3. Fall through: ``git clone`` at SHA, ``docker build``, then
   ``docker push`` if a registry is configured.
"""

from __future__ import annotations

import importlib.metadata
import os
import tempfile
from pathlib import Path

from ..docker_config import add_host_flags, compute_image_tag, get_registry
from ..runner_config import ImageBuild, ImageSourceT
from . import cli


def _docker_bin() -> str:
    return os.environ.get("AAICLICK_DOCKER_BIN", "docker")


async def _require_docker() -> None:
    """Preflight the build: the docker CLI is on PATH *and* its daemon is reachable.

    A ``build`` image source builds on the worker's own host, so a worker with no
    Docker fails here with a clear, actionable message instead of a raw
    ``FileNotFoundError`` (missing CLI) or a daemon-connection error surfacing deep
    inside the first ``docker`` call. ``docker version`` needs the server, so it
    validates connectivity, not just the binary."""
    bin_ = _docker_bin()
    try:
        rc, _, stderr = await cli.run(bin_, "version", "--format", "{{.Server.Version}}", check=False, stream=False)
    except FileNotFoundError as e:
        raise RuntimeError(
            f"Docker CLI {bin_!r} not found on this worker, but this job builds its "
            f"image from source. Install Docker on the worker host, set "
            f"AAICLICK_DOCKER_BIN to the docker binary, or submit the job with a "
            f"prebuilt image (image=...)."
        ) from e
    if rc != 0:
        raise RuntimeError(
            f"Docker daemon is not reachable from this worker (`{bin_} version` exited "
            f"{rc}): {stderr.strip()}. A build image source builds on the worker host; "
            f"ensure the daemon is running and this user can access it."
        )


async def _docker_image_exists_locally(image_tag: str) -> bool:
    rc, _, _ = await cli.run(_docker_bin(), "image", "inspect", image_tag, check=False)
    return rc == 0


async def _docker_pull(image_tag: str) -> bool:
    """Returns True on cache hit, False if the registry doesn't have it."""
    rc, _, _ = await cli.run(_docker_bin(), "pull", image_tag, check=False)
    return rc == 0


async def _docker_push(image_tag: str) -> None:
    await cli.run(_docker_bin(), "push", image_tag)


async def _git_clone_at_sha(remote: str, sha: str, workdir: str) -> None:
    """Clone the SHA into ``workdir``. Uses ``git init`` + ``fetch`` + ``checkout``
    so we avoid pulling the full default branch when only one commit is needed,
    and so the remote can be a non-default-branch SHA."""
    await cli.run("git", "init", "--quiet", workdir)
    await cli.run("git", "-C", workdir, "remote", "add", "origin", remote)
    await cli.run("git", "-C", workdir, "fetch", "--depth=1", "--quiet", "origin", sha)
    await cli.run("git", "-C", workdir, "checkout", "--quiet", sha)


def _aaiclick_version() -> str:
    """Best-effort version of the running aaiclick package, for the
    ``AAICLICK_VERSION`` build-arg."""
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
    add("PIP_INDEX_URL", os.environ.get("AAICLICK_PIP_INDEX_URL"))
    add("PIP_EXTRA_INDEX_URL", os.environ.get("AAICLICK_PIP_EXTRA_INDEX_URL"))
    add("PIP_TRUSTED_HOST", os.environ.get("AAICLICK_PIP_TRUSTED_HOST"))
    add("AAICLICK_VERSION", _aaiclick_version())
    return args


async def _docker_build(context: str, dockerfile: str, image_tag: str, build_args: list[str]) -> None:
    cmd = [
        _docker_bin(),
        "build",
        "-t",
        image_tag,
        "-f",
        dockerfile,
        *build_args,
        *add_host_flags("AAICLICK_DOCKER_BUILD_ADD_HOST"),
        context,
    ]
    await cli.run(*cmd)


async def resolve_launch_image(image_source: ImageSourceT | None, *, task_id: int) -> str:
    """Resolve the image tag a container actually launches with.

    The tag is derived from the source (prebuilt tag verbatim, computed
    ``aaiclick-job:<sha>`` for a build). Registry mode: the ``build >> task``
    dependency edge guarantees the tag is already pushed — the launch path
    pulls. No registry + build source: build inline on this host
    (``build_image_to_tag`` short-circuits on a local-cache hit), holding the
    worker slot for a cold build — accepted, no-registry is de facto
    single-host mode (spec: docs/designs/orchestration.md "Image source").

    A ``None`` source cannot occur through dispatch (``_resolve_dispatch``
    routes NULL-image tasks to the subprocess vehicle); the raise here is the
    single owner of that invariant for all container launch paths."""
    if image_source is None:
        raise ValueError(f"container task {task_id} has no image_source")
    if isinstance(image_source, ImageBuild):
        image_tag = compute_image_tag(image_source.git_sha)
        if get_registry() is None:
            await build_image_to_tag(image_source, image_tag)
        return image_tag
    return image_source.image_tag


async def build_image_to_tag(source: ImageBuild, image_tag: str) -> None:
    """Ensure ``image_tag`` exists in the local docker daemon, building from
    ``source`` if needed; push when a registry is configured.

    Idempotent and content-addressed by SHA. A local-cache hit short-circuits
    the *build* but not the push: a prior attempt that built locally then failed
    to push must re-push on retry, or other hosts could never pull the image."""
    await _require_docker()

    registry = get_registry()

    if registry and await _docker_pull(image_tag):
        return

    if not await _docker_image_exists_locally(image_tag):
        with tempfile.TemporaryDirectory(prefix="aaiclick-build-") as workdir:
            await _git_clone_at_sha(source.git_remote, source.git_sha, workdir)

            context_dir = Path(workdir)
            dockerfile = context_dir / (source.dockerfile or "Dockerfile")
            if not dockerfile.is_file():
                raise FileNotFoundError(
                    f"Dockerfile not found at "
                    f"{source.dockerfile or 'Dockerfile'} "
                    f"in repo {source.git_remote}@{source.git_sha}. "
                    f"Run `python -m aaiclick docker init` in the user's repo "
                    f"to scaffold a starter Dockerfile."
                )

            build_args = _collect_build_args(source)
            await _docker_build(str(context_dir), str(dockerfile), image_tag, build_args)

    if registry:
        await _docker_push(image_tag)
