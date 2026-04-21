"""Shared view models used across aaiclick's CLI, REST, and MCP surfaces.

Cross-domain pydantic models for paging, errors, and request/filter payloads.
Domain-specific view models live in ``aaiclick.orchestration.view_models`` and
``aaiclick.data.view_models``.

The view models never import SQLModel classes; adapters do. Enums from
``aaiclick.orchestration.models`` are reused so the CLI, REST, and MCP
surfaces share one vocabulary.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field, model_validator

from .orchestration.models import JobStatus, PreservationMode, WorkerStatus

# Mirrors aaiclick.data.scope.ObjectScope — re-declared to keep this shared
# module from pulling the heavy aaiclick.data package into CLI/REST startup.
# Keep the two in lock-step.
ObjectScope = Literal["temp", "job", "global"]

T = TypeVar("T")

RefId = int | str
"""Reference to an entity by numeric ID or human-readable name."""


class Page(BaseModel, Generic[T]):
    """Generic paged list response."""

    items: list[T]
    total: int | None = None
    next_cursor: str | None = None


class Problem(BaseModel):
    """RFC 7807-style error payload used by the REST surface."""

    title: str
    status: int
    detail: str | None = None
    code: str | None = None


class RunJobRequest(BaseModel):
    """Inputs for ``internal_api.run_job``."""

    name: str
    kwargs: dict[str, Any] = Field(default_factory=dict)
    preservation_mode: PreservationMode | None = None


class RegisterJobRequest(BaseModel):
    """Inputs for ``internal_api.register_job``.

    ``name`` defaults to the last dotted segment of ``entrypoint`` (pass ``""``
    or omit to opt in), so every surface — CLI, REST, MCP — gets the same
    shorthand without redoing the derivation at the call site.
    """

    name: str = ""
    entrypoint: str
    schedule: str | None = None
    default_kwargs: dict[str, Any] | None = None
    enabled: bool = True
    preservation_mode: PreservationMode | None = None

    @model_validator(mode="after")
    def _default_name_from_entrypoint(self) -> RegisterJobRequest:
        if not self.name:
            self.name = self.entrypoint.rsplit(".", 1)[-1]
        return self


class JobListFilter(BaseModel):
    """Filter parameters for ``internal_api.list_jobs``."""

    status: JobStatus | None = None
    name: str | None = None
    since: datetime | None = None
    limit: int = 50
    offset: int = 0
    cursor: str | None = None


class RegisteredJobFilter(BaseModel):
    """Filter parameters for ``internal_api.list_registered_jobs``."""

    enabled: bool | None = None
    name: str | None = None
    limit: int = 50
    offset: int = 0
    cursor: str | None = None


class WorkerFilter(BaseModel):
    """Filter parameters for ``internal_api.list_workers``."""

    status: WorkerStatus | None = None
    limit: int = 50


class ObjectFilter(BaseModel):
    """Filter parameters for ``internal_api.list_objects``."""

    prefix: str | None = None
    scope: ObjectScope | None = None
    limit: int = 50
    cursor: str | None = None
