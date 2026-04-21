"""Shared business logic for aaiclick's CLI, REST, and MCP surfaces.

Every command is implemented exactly once here, takes explicit dependency
parameters (``AsyncSession`` for SQL, ``ChClient`` for ClickHouse), and returns
a pydantic view model. The three surfaces (``__main__`` / ``server/routers`` /
``server/mcp``) are thin renderers over this module.

See ``docs/api_server.md`` for the full contract.
"""

from .errors import Conflict, InternalApiError, Invalid, NotFound
from .jobs import cancel_job, get_job, job_stats, list_jobs, run_job
