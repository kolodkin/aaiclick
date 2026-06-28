"""Typed, discriminated configs for a job's runner and a task's entry.

A job's image/runner settings (formerly the flat ``git_*``/``image_tag``/
``kubernetes_config`` columns) collapse into one ``RunnerConfig`` serialized to
a JSON column; the ``entry_type`` discriminator selects how the container is
invoked. Pure data + validation — no env, no I/O — so any layer can import it.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter, field_validator

# --- entry_type discriminator (lives on Task) -----------------------------
ENTRY_MODULE = "module"
ENTRY_SHELL = "shell"
EntryType = Literal["module", "shell"]
ENTRY_TYPES: list[EntryType] = [ENTRY_MODULE, ENTRY_SHELL]


# --- image source (nested in docker/kubernetes runners) -------------------
class ImageBuild(BaseModel):
    """Build the image from a git repo at a SHA. ``image_tag`` is computed
    (``aaiclick-job:<sha>``), not stored here."""

    type: Literal["build"] = "build"
    git_remote: str
    git_sha: str
    git_branch: str | None = None
    dockerfile: str | None = None


class ImagePrebuilt(BaseModel):
    """Use an existing image verbatim; no build task is injected."""

    type: Literal["prebuilt"] = "prebuilt"
    image_tag: str

    @field_validator("image_tag")
    @classmethod
    def _nonempty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("image_tag must be a non-empty image reference")
        return v


ImageSource = Annotated[ImageBuild | ImagePrebuilt, Field(discriminator="type")]


# --- runner (lives on Job / RegisteredJob) --------------------------------
class SubprocessRunner(BaseModel):
    type: Literal["subprocess"] = "subprocess"


class DockerRunner(BaseModel):
    type: Literal["docker"] = "docker"
    image: ImageSource


class KubernetesRunner(BaseModel):
    type: Literal["kubernetes"] = "kubernetes"
    image: ImageSource
    namespace: str | None = None
    service_account: str | None = None
    image_pull_secret: str | None = None


RunnerConfig = Annotated[
    SubprocessRunner | DockerRunner | KubernetesRunner,
    Field(discriminator="type"),
]

_RUNNER_ADAPTER: TypeAdapter[RunnerConfig] = TypeAdapter(RunnerConfig)
_IMAGE_ADAPTER: TypeAdapter[ImageSource] = TypeAdapter(ImageSource)

RunnerConfigT = SubprocessRunner | DockerRunner | KubernetesRunner
ImageSourceT = ImageBuild | ImagePrebuilt


def parse_runner_config(data: dict) -> RunnerConfigT:
    """Validate a JSON dict into the matching runner model."""
    return _RUNNER_ADAPTER.validate_python(data)


def dump_runner_config(cfg: RunnerConfigT) -> dict:
    """Serialize a runner model to a JSON-safe dict for the DB column."""
    return _RUNNER_ADAPTER.dump_python(cfg, mode="json")
