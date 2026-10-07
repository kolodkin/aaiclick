"""Typed, discriminated configs for a task's image source and entry.

A task's ``image_source`` JSON is one of ``ImageBuild`` / ``ImagePrebuilt``;
the ``entry_type`` discriminator selects how the container is invoked. Pure
data + validation — no env, no I/O — so any layer can import it.
"""

from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter, field_validator

# --- entry_type discriminator (lives on Task) -----------------------------
ENTRY_MODULE = "module"
ENTRY_SHELL = "shell"
ENTRY_JVM = "jvm"
EntryType = Literal["module", "shell", "jvm"]
ENTRY_TYPES: list[EntryType] = [ENTRY_MODULE, ENTRY_SHELL, ENTRY_JVM]


# --- image source (lives on Task) -----------------------------------------
_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


class ImageBuild(BaseModel):
    """Build the image from a git repo at a SHA. ``image_tag`` is computed
    (``aaiclick-job:<sha>``), not stored here."""

    type: Literal["build"] = "build"
    git_remote: str
    git_sha: str
    git_branch: str | None = None
    dockerfile: str | None = None

    @field_validator("git_sha")
    @classmethod
    def _full_lowercase_hex(cls, v: str) -> str:
        """``git_sha`` lands in ``git fetch`` argv; anything but a SHA could read as an option."""
        if not _SHA_RE.match(v):
            raise ValueError(f"git_sha must be a 40-char lowercase hex string; got {v!r}")
        return v


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


_IMAGE_ADAPTER: TypeAdapter[ImageSource] = TypeAdapter(ImageSource)

ImageSourceT = ImageBuild | ImagePrebuilt


def parse_image_source(data: dict) -> ImageSourceT:
    """Validate a JSON dict into the matching image-source model."""
    return _IMAGE_ADAPTER.validate_python(data)


def dump_image_source(source: ImageSourceT) -> dict:
    """Serialize an image-source model to a JSON-safe dict for the DB column."""
    return _IMAGE_ADAPTER.dump_python(source, mode="json")


def validate_image_exclusivity(image: str | None, build: bool, *build_fields: str | None) -> None:
    """A prebuilt ``image`` and the build side (``build`` flag, ``git_*`` /
    ``dockerfile`` modifiers) are mutually exclusive — shared by every
    submission surface so the rule and its message live in one place. Raises
    ``ValueError``."""
    if image is not None and (build or any(v is not None for v in build_fields)):
        raise ValueError("image (prebuilt) and build/git_* fields are mutually exclusive")


def validate_task_entry(*, entry_type: EntryType, command: list[str] | None) -> None:
    """Enforce the entry cross-field rules (spec "Validation").

    ``shell`` is runner-agnostic — valid on subprocess, docker, and kubernetes —
    so there is no runner argument. Raises ``ValueError`` on violation."""
    if entry_type == ENTRY_SHELL:
        if not command:
            raise ValueError("shell entry_type requires a non-empty command list")
    elif entry_type in (ENTRY_MODULE, ENTRY_JVM):
        if command:
            raise ValueError(f"{entry_type} entry_type does not take a command")
    else:
        raise ValueError(f"unknown entry_type {entry_type!r}")
