"""Typed, discriminated configs for a task's image source and entry, plus
the job's ``resources`` quantities.

A task's ``image_source`` JSON is one of ``ImageBuild`` / ``ImagePrebuilt``;
the ``entry_type`` discriminator selects how the container is invoked. Pure
data + validation — no env, no I/O — so any layer can import it.
"""

from __future__ import annotations

import re
from decimal import Decimal
from typing import Annotated, Literal

from pydantic import BaseModel, Field, TypeAdapter, field_validator

# --- entry_type discriminator (lives on Task) -----------------------------
ENTRY_MODULE = "module"
ENTRY_SHELL = "shell"
ENTRY_JVM = "jvm"
EntryType = Literal["module", "shell", "jvm"]
ENTRY_TYPES: list[EntryType] = [ENTRY_MODULE, ENTRY_SHELL, ENTRY_JVM]


# --- image source (lives on Task) -----------------------------------------
IMAGE_PREBUILT = "prebuilt"
IMAGE_BUILD = "build"
ImageKind = Literal["prebuilt", "build"]
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


def build_requested(build: bool, *modifiers: str | None) -> bool:
    """Whether a submission asks for a git build: the ``build`` flag, or any
    set modifier (``git_*`` / ``dockerfile``), which implies it."""
    return build or any(v is not None for v in modifiers)


def validate_image_exclusivity(image: str | None, *build_fields: str | None, build: bool = False) -> None:
    """A prebuilt ``image`` and the build side (``build`` flag, ``git_*`` /
    ``dockerfile`` modifiers) are mutually exclusive — shared by every
    submission surface so the rule and its message live in one place. Raises
    ``ValueError``."""
    if image is not None and build_requested(build, *build_fields):
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


# --- resources (lives on Job / RegisteredJob) -----------------------------
# Kubernetes quantity suffixes: decimal SI (n…E) and binary SI (Ki…Ei).
_QUANTITY_SUFFIXES: dict[str, Decimal] = {
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "": Decimal(1),
    **{p: Decimal(1000) ** i for i, p in enumerate("kMGTPE", 1)},
    **{p + "i": Decimal(1024) ** i for i, p in enumerate("KMGTPE", 1)},
}
# A number then either a decimal exponent or an SI suffix, never both.
_QUANTITY_RE = re.compile(
    r"^(?P<number>[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:(?P<exponent>[eE][+-]?[0-9]+)|(?P<suffix>[A-Za-z]*))$"
)
# Top-level keys of a Kubernetes ResourceRequirements.
_RESOURCE_SECTIONS = ("requests", "limits", "claims")


def parse_quantity(key: str, value: object) -> Decimal:
    """A Kubernetes quantity (``"500m"``, ``"1.5Gi"``, ``2``) in base units."""
    match = _QUANTITY_RE.match(str(value))
    if match is None or (match["exponent"] is None and match["suffix"] not in _QUANTITY_SUFFIXES):
        raise ValueError(f"resources.{key} {value!r} is not a valid Kubernetes quantity")
    try:
        if match["exponent"] is not None:
            return +Decimal(match["number"] + match["exponent"])
        return Decimal(match["number"]) * _QUANTITY_SUFFIXES[match["suffix"]]
    except ArithmeticError as e:
        raise ValueError(f"resources.{key} {value!r} is out of range") from e


def validate_resources(resources: dict | None) -> None:
    """Reject a malformed ``resources`` at submission, for every runner: a
    non-mapping, an unknown section, or a bad ``cpu`` / ``memory`` quantity.
    Other keys (GPUs, storage) pass through to Kubernetes unchecked."""
    if resources is None:
        return
    if not isinstance(resources, dict):
        raise ValueError(f"resources must be a mapping, got {resources!r}")
    if unknown := sorted(set(resources) - set(_RESOURCE_SECTIONS)):
        raise ValueError(f"unknown resources section(s) {unknown}; expected {list(_RESOURCE_SECTIONS)}")
    for section in ("requests", "limits"):
        values = resources.get(section) or {}
        if not isinstance(values, dict):
            raise ValueError(f"resources.{section} must be a mapping, got {values!r}")
        for key in ("cpu", "memory"):
            if (value := values.get(key)) is not None:
                parse_quantity(f"{section}.{key}", value)
