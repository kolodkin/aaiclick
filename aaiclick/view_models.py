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

from .ai.ollama import (
    OLLAMA_ALREADY_PRESENT,
    OLLAMA_FAILED,
    OLLAMA_NOT_OLLAMA,
    OLLAMA_PULLED,
    OLLAMA_SERVER_UNREACHABLE,
    OllamaBootstrapResult,
    OllamaBootstrapStatus,
)
from .log_models import (
    STDERR_STREAM,
    STDOUT_STREAM,
    LogLevel,
    LogLine,
    LogStream,
    SnowflakeId,
    normalize_level,
)
from .orchestration.models import ExecutionWorkerStatus, JobStatus, PreservationMode, RunnerMode

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
    FORBIDDEN = "forbidden"
    EXECUTION_WORKER_SPAWN_FAILED = "execution_worker_spawn_failed"


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
    image: str | None = None
    # Per-run docker overrides; ignored unless the registered job is
    # in docker mode. Each field falls through to the RegisteredJob
    # default, then to git auto-detect (where applicable).
    git_remote: str | None = None
    git_sha: str | None = None
    git_branch: str | None = None
    dockerfile: str | None = None
    # Per-run kubernetes overrides; ignored unless the registered job is in
    # kubernetes mode. Each field falls through to the RegisteredJob default,
    # then the AAICLICK_K8S_* env layer.
    namespace: str | None = None
    service_account: str | None = None
    image_pull_secret: str | None = None


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
    # Docker-runner defaults; per-run kwargs on RunJobRequest override them.
    runner_mode: RunnerMode = "subprocess"
    dockerfile: str | None = None
    git_remote: str | None = None
    image: str | None = None
    # Kubernetes-runner cluster defaults (namespace, service_account, ...).
    kubernetes_config: dict[str, Any] | None = None

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


class ExecutionWorkerFilter(BaseModel):
    """Filter parameters for ``internal_api.list_execution_workers``."""

    status: ExecutionWorkerStatus | None = None
    limit: int = 50
    offset: int = 0
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
    limit: int = 50
    cursor: str | None = None


class PurgeObjectsRequest(BaseModel):
    """Inputs for ``internal_api.purge_objects``.

    At least one of ``after`` / ``before`` must be set — the internal_api
    refuses to purge everything unfiltered.
    """

    after: datetime | None = None
    before: datetime | None = None


class PurgeObjectsResult(BaseModel):
    """Response from ``internal_api.purge_objects`` — names of dropped tables."""

    deleted: list[str]


class ObjectDeleted(BaseModel):
    """Response from ``internal_api.delete_object`` — name of the dropped table."""

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
    ollama: OllamaBootstrapResult | None = None


MIGRATE_UPGRADE = "upgrade"
MIGRATE_DOWNGRADE = "downgrade"
MIGRATE_CURRENT = "current"
MIGRATE_HISTORY = "history"
MIGRATE_HEADS = "heads"
MIGRATE_SHOW = "show"
MigrationAction = Literal["upgrade", "downgrade", "current", "history", "heads", "show"]
"""Alembic subcommand executed by ``internal_api.migrate``."""


class MigrationResult(BaseModel):
    """Response from ``internal_api.migrate`` — describes what was run.

    Alembic commands emit their own output to stdout; this model captures the
    request shape so callers can format / serialise the invocation itself.
    """

    action: MigrationAction
    revision: str | None = None
