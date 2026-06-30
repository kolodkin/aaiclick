"""Low-level build helpers for Docker-runner jobs.

Used by the on-demand build seam (see ``execution.image_builder``) to
produce the image a docker/kubernetes job's container/pod tasks need.

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

from ..docker_config import add_host_flags
from ..runner_config import ImageBuild
from . import cli


def _docker_bin() -> str:
    return os.environ.get("AAICLICK_DOCKER_BIN", "docker")


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


async def build_image_to_tag(source: ImageBuild, image_tag: str) -> None:
    """Ensure ``image_tag`` exists in the local docker daemon, building from
    ``source`` if needed; push when a registry is configured.

    Idempotent and content-addressed by SHA. A local-cache hit short-circuits
    the *build* but not the push: a prior attempt that built locally then failed
    to push must re-push on retry, or other hosts could never pull the image."""
    registry = os.environ.get("AAICLICK_REGISTRY")

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
