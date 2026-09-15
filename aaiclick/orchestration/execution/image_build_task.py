"""Image-build task body.

An ordinary module task injected at commit points (see
``orchestration.image_injection``) for every distinct build-source image in a
docker/kubernetes job. It runs on the dispatching worker host
(``image_source=NULL`` ⇒ subprocess vehicle) because it needs the docker CLI
and daemon socket. The body is cache-first via ``build_image_to_tag``:
registry mode pulls (someone already pushed this SHA → done), else clone +
build + push; local mode checks the host daemon, else clone + build. Cross-job
dedup is the registry (or the daemon cache) itself — a lost race
double-builds, which is wasteful but correct.
"""

from __future__ import annotations

from ..docker_config import compute_image_tag, get_build_mode
from ..runner_config import ImageBuild
from .docker_build import build_image_to_tag

IMAGE_BUILD_ENTRYPOINT = "aaiclick.orchestration.execution.image_build_task.run_image_build"


def _repo_slug(git_remote: str) -> str:
    """Last path segment of a git remote, minus ``.git`` — ``repo`` for both
    ``https://host/org/repo.git`` and ``git@host:org/repo.git``."""
    slug = git_remote.rstrip("/").rsplit("/", 1)[-1].rsplit(":", 1)[-1]
    return slug.removesuffix(".git")


def build_task_name(source: ImageBuild) -> str:
    """Display name for an injected build task — repo, branch when known, and
    short SHA (e.g. ``build-image:myrepo@main:a1b2c3d4``), so a job graph with
    several build tasks says what each one builds. A non-default Dockerfile is
    appended because two images can differ by nothing else."""
    name = f"build-image:{_repo_slug(source.git_remote)}"
    if source.git_branch:
        name += f"@{source.git_branch}"
    name += f":{source.git_sha[:8]}"
    if source.dockerfile:
        name += f" ({source.dockerfile})"
    return name


async def run_image_build(
    *,
    git_remote: str,
    git_sha: str,
    git_branch: str | None = None,
    dockerfile: str | None = None,
) -> None:
    """Ensure the image for these build coordinates exists where dependents
    will launch it: pushed to the registry, or in this host's daemon in local
    mode.

    ``get_build_mode`` raises when the worker has neither or both of
    ``AAICLICK_REGISTRY`` / ``AAICLICK_LOCAL_BUILD`` — the mode is decided
    here, on the worker, never at submission.
    """
    get_build_mode()
    source = ImageBuild(git_remote=git_remote, git_sha=git_sha, git_branch=git_branch, dockerfile=dockerfile)
    await build_image_to_tag(source, compute_image_tag(git_sha))
