"""Shared view models used across aaiclick's CLI, REST, and MCP surfaces.

Cross-domain pydantic models for paging, errors, and request/filter payloads.
Domain-specific view models live in ``aaiclick.orchestration.view_models`` and
``aaiclick.data.view_models``.

The view models never import SQLModel classes; adapters do. Enums from
``aaiclick.orchestration.models`` are reused so the CLI, REST, and MCP
surfaces share one vocabulary. Primitives needed inside ``orchestration``
itself (``SnowflakeId``, the log vocabulary) live in ``aaiclick.log_models``
and are re-exported here — orchestration imports the leaf module, never this
one, so import order between the two packages doesn't matter.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Generic, Literal, TypeVar

from pydantic import BaseModel, Field, model_validator

from .log_models import (
    MAX_PAGE_LIMIT,
    STDERR_STREAM,
    STDOUT_STREAM,
    LogLine,
    LogStream,
    PageLimit,
    PageOffset,
    SnowflakeId,
    UtcDateTime,
)
from .orchestration.models import ExecutionWorkerStatus, JobStatus, PreservationMode

# Mirrors aaiclick.data.scope.ObjectScope — re-declared to keep this shared
# module from pulling the heavy aaiclick.data package into CLI/REST startup.
# Keep the two in lock-step.
ObjectScope = Literal["temp", "temp_named", "job", "global"]

# Mirrors aaiclick.orchestration.runner_config.EntryType — re-declared to match
# the ObjectScope pattern above and keep this module import-light. Keep the two
# in lock-step.
EntryType = Literal["module", "shell"]

T = TypeVar("T")

RefId = int | str
"""Reference to an entity by numeric ID or human-readable name."""


class Page(BaseModel, Generic[T]):
    """Generic paged list response."""

    items: list[T]
    total: int | None = None
    next_cursor: str | None = None


class ProblemCode(str, Enum):
    """Stable machine-readable code attached to every ``Problem`` response."""

    NOT_FOUND = "not_found"
    CONFLICT = "conflict"
    INVALID = "invalid"
    UNAUTHORIZED = "unauthorized"
    MFA_REQUIRED = "mfa_required"
    FORBIDDEN = "forbidden"
    EXECUTION_WORKER_SPAWN_FAILED = "execution_worker_spawn_failed"
    SANDBOX_UNAVAILABLE = "sandbox_unavailable"


class Problem(BaseModel):
    """RFC 7807-style error payload used by the REST surface."""

    title: str
    status: int
    detail: str | None = None
    code: ProblemCode | None = None


class RunJobRequest(BaseModel):
    """Inputs for ``internal_api.run_job``."""

    name: str
    kwargs: dict[str, Any] = Field(default_factory=dict)
    preservation_mode: PreservationMode | None = None
    entry_type: EntryType = "module"
    command: list[str] | None = None
    command_env: dict[str, str] | None = None
    # Image source: ``image`` runs a prebuilt image, ``build`` (or any git_*
    # / dockerfile modifier) builds one from the repo; neither means a host
    # subprocess. Modifiers fall through to the RegisteredJob default, then
    # to the remote's branch head (``docker_config.resolve_image_source``).
    image: str | None = None
    build: bool = False
    git_remote: str | None = None
    git_sha: str | None = None
    git_branch: str | None = None
    dockerfile: str | None = None
    # Kubernetes-style requests/limits for this run's containers; None
    # inherits the RegisteredJob default.
    resources: dict[str, Any] | None = None


class SubmitSandboxRequest(BaseModel):
    """Inputs for ``internal_api.submit_sandbox_file``: a name and one Python file."""

    name: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_]*$", max_length=64)
    source: str = Field(min_length=1, max_length=200_000)


class SandboxFileFilter(BaseModel):
    """Paging for ``internal_api.list_sandbox_files``."""

    limit: PageLimit = 50
    offset: PageOffset = 0


class SandboxConfigView(BaseModel):
    """Whether the sandbox is enabled; ``remote`` is shown to admins only."""

    enabled: bool
    remote: str | None = None


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
    # Image-source defaults; per-run fields on RunJobRequest override them.
    build: bool = False
    image: str | None = None
    git_remote: str | None = None
    dockerfile: str | None = None
    resources: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _default_name_from_entrypoint(self) -> RegisterJobRequest:
        if not self.name:
            self.name = self.entrypoint.rsplit(".", 1)[-1]
        return self


class JobListFilter(BaseModel):
    """Filter parameters for ``internal_api.list_jobs``."""

    status: JobStatus | None = None
    name: str | None = None
    since: UtcDateTime | None = None
    limit: PageLimit = 50
    offset: PageOffset = 0
    cursor: str | None = None


class RegisteredJobFilter(BaseModel):
    """Filter parameters for ``internal_api.list_registered_jobs``."""

    enabled: bool | None = None
    name: str | None = None
    limit: PageLimit = 50
    offset: PageOffset = 0
    cursor: str | None = None


class ExecutionWorkerFilter(BaseModel):
    """Filter parameters for ``internal_api.list_execution_workers``."""

    status: ExecutionWorkerStatus | None = None
    limit: PageLimit = 50
    offset: PageOffset = 0
    cursor: str | None = None


class StartExecutionWorkerRequest(BaseModel):
    """Inputs for ``internal_api.start_execution_worker``.

    ``max_tasks`` caps how many tasks the spawned execution worker executes
    before it exits; ``None`` means unlimited (run until stopped).
    """

    max_tasks: int | None = None


class ObjectFilter(BaseModel):
    """Filter parameters for ``internal_api.list_objects``."""

    prefix: str | None = None
    scope: ObjectScope | None = None
    job: RefId | None = None  # list one job's objects: id, or name → latest run
    limit: PageLimit = 50
    cursor: str | None = None


class PurgeObjectsRequest(BaseModel):
    """Inputs for ``internal_api.purge_objects``.

    At least one of ``after`` / ``before`` must be set — the internal_api
    refuses to purge everything unfiltered.
    """

    after: UtcDateTime | None = None
    before: UtcDateTime | None = None


class PurgeObjectsResult(BaseModel):
    """Response from ``internal_api.purge_objects`` — names of dropped tables."""

    deleted: list[str]


class Deleted(BaseModel):
    """Response from a delete-by-name verb: the name that was removed."""

    name: str


SetupStepStatus = Literal["ok", "skipped", "failed"]


class SetupStep(BaseModel):
    """One check/action performed during ``internal_api.setup``."""

    name: str
    status: SetupStepStatus
    detail: str | None = None


class SetupResult(BaseModel):
    """Response from ``internal_api.setup`` — per-step outcomes + resolved config."""

    root: str
    ch_url: str
    sql_url: str
    mode: Literal["local", "distributed"]
    steps: list[SetupStep]


MIGRATE_UPGRADE = "upgrade"
MIGRATE_DOWNGRADE = "downgrade"
MIGRATE_CURRENT = "current"
MIGRATE_HISTORY = "history"
MIGRATE_HEADS = "heads"
MIGRATE_SHOW = "show"
MigrationAction = Literal["upgrade", "downgrade", "current", "history", "heads", "show"]
"""Alembic subcommand executed by ``internal_api.migrate``."""


class ChVersionStatus(BaseModel):
    """One ClickHouse migration version and whether it has been applied."""

    version: str
    applied: bool


class MigrationResult(BaseModel):
    """Response from ``internal_api.migrate`` — describes what was run.

    Alembic commands emit their own output to stdout; this model captures the
    request shape plus the ClickHouse-side outcome so callers can format /
    serialise the invocation itself.
    """

    action: MigrationAction
    revision: str | None = None
    ch_versions_applied: list[str] = []
    ch_versions: list[ChVersionStatus] = []
