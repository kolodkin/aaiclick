"""Shared business logic for aaiclick's CLI, REST, and MCP surfaces.

Every command is implemented exactly once here. Functions run inside an
active ``orch_context()`` (orchestration) or ``data_context()`` (data) and
read SQL/CH resources through the contextvar getters — same pattern as the
rest of the codebase. Inputs are primitives or ``*Request`` / ``*Filter``
view models; outputs are pydantic view models. The three surfaces
(``__main__`` / ``server/routers`` / ``server/mcp``) are thin renderers
over this module.

See ``docs/api_server.md`` for the full contract.
"""

from .errors import Conflict, InternalApiError, Invalid, NotFound
from .jobs import cancel_job, get_job, job_stats, list_jobs, run_job
from .registered_jobs import (
    disable_job,
    enable_job,
    list_registered_jobs,
    register_job,
)
from .tasks import get_task
from .workers import list_workers, stop_worker
