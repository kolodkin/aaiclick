"""FastMCP tool surface over ``aaiclick.internal_api``.

Every CLI verb that has an ``internal_api`` function is exposed as an MCP
tool. Each tool is a thin wrapper that opens the context its underlying
``internal_api`` function needs (``orch_context(with_ch=...)``) — the same
scope the HTTP routers in ``server/routers/`` open per request — and
returns the pydantic view model directly. FastMCP derives the tool input
and output schemas from the pydantic types, so MCP, REST, and CLI share
one contract.

``setup`` / ``migrate`` / ``bootstrap_ollama`` are infrastructure
commands and run without an orchestration context, matching the CLI.

Mounted on the FastAPI app in ``aaiclick.server.app``; the module-level
``mcp`` instance is also usable standalone (``mcp.run()``) or from
in-process tests via ``fastmcp.Client(mcp)``.
"""

from __future__ import annotations

from fastmcp import FastMCP

from aaiclick.ai.agents.lineage_tools import DEFAULT_ROW_LIMIT, QueryResult, TableSchema
from aaiclick.data.view_models import ObjectDetail, ObjectView
from aaiclick.internal_api import jobs as jobs_api
from aaiclick.internal_api import lineage as lineage_api
from aaiclick.internal_api import objects as objects_api
from aaiclick.internal_api import registered_jobs as rj_api
from aaiclick.internal_api import setup as setup_api
from aaiclick.internal_api import tasks as tasks_api
from aaiclick.internal_api import workers as workers_api
from aaiclick.oplog.lineage import LineageDirection, OplogGraph
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.orchestration.view_models import (
    JobDetail,
    JobStatsView,
    JobView,
    RegisteredJobView,
    TaskDetail,
    WorkerView,
)
from aaiclick.view_models import (
    JobListFilter,
    MigrationAction,
    MigrationResult,
    ObjectDeleted,
    ObjectFilter,
    OllamaBootstrapResult,
    Page,
    PurgeObjectsRequest,
    PurgeObjectsResult,
    RefId,
    RegisteredJobFilter,
    RegisterJobRequest,
    RunJobRequest,
    SetupResult,
    WorkerFilter,
)

mcp: FastMCP = FastMCP(
    name="aaiclick",
    instructions=(
        "Tools mirror aaiclick's CLI verbs one-to-one. Every tool runs against "
        "the same backends as the REST surface under /api/v0 — see docs/api_server.md."
    ),
)


# --- jobs -------------------------------------------------------------


@mcp.tool
async def list_jobs(filter: JobListFilter | None = None) -> Page[JobView]:
    """Return a page of jobs ordered by ``created_at`` descending."""
    async with orch_context(with_ch=False):
        return await jobs_api.list_jobs(filter)


@mcp.tool
async def get_job(ref: RefId) -> JobDetail:
    """Return full job detail including all tasks."""
    async with orch_context(with_ch=False):
        return await jobs_api.get_job(ref)


@mcp.tool
async def job_stats(ref: RefId) -> JobStatsView:
    """Return execution statistics for a job and its tasks."""
    async with orch_context(with_ch=False):
        return await jobs_api.job_stats(ref)


@mcp.tool
async def cancel_job(ref: RefId) -> JobView:
    """Cancel a job and its non-terminal tasks."""
    async with orch_context(with_ch=False):
        return await jobs_api.cancel_job(ref)


@mcp.tool
async def run_job(request: RunJobRequest) -> JobView:
    """Run a job immediately, auto-registering if needed."""
    async with orch_context(with_ch=True):
        return await jobs_api.run_job(request)


# --- registered jobs --------------------------------------------------


@mcp.tool
async def list_registered_jobs(
    filter: RegisteredJobFilter | None = None,
) -> Page[RegisteredJobView]:
    """Return a page of registered jobs ordered by ``name``."""
    async with orch_context(with_ch=False):
        return await rj_api.list_registered_jobs(filter)


@mcp.tool
async def register_job(request: RegisterJobRequest) -> RegisteredJobView:
    """Register a new job in the catalog."""
    async with orch_context(with_ch=False):
        return await rj_api.register_job(request)


@mcp.tool
async def enable_job(name: str) -> RegisteredJobView:
    """Enable a registered job and recompute its next fire time."""
    async with orch_context(with_ch=False):
        return await rj_api.enable_job(name)


@mcp.tool
async def disable_job(name: str) -> RegisteredJobView:
    """Disable a registered job and clear its next fire time."""
    async with orch_context(with_ch=False):
        return await rj_api.disable_job(name)


# --- tasks ------------------------------------------------------------


@mcp.tool
async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID."""
    async with orch_context(with_ch=False):
        return await tasks_api.get_task(task_id)


# --- workers ----------------------------------------------------------


@mcp.tool
async def list_workers(filter: WorkerFilter | None = None) -> Page[WorkerView]:
    """Return a page of workers ordered by ``started_at`` descending."""
    async with orch_context(with_ch=False):
        return await workers_api.list_workers(filter)


@mcp.tool
async def stop_worker(worker_id: int) -> WorkerView:
    """Request a worker to stop gracefully after its current task."""
    async with orch_context(with_ch=False):
        return await workers_api.stop_worker(worker_id)


# --- objects ----------------------------------------------------------


@mcp.tool
async def list_objects(filter: ObjectFilter | None = None) -> Page[ObjectView]:
    """Return a page of persistent objects ordered by name."""
    async with orch_context(with_ch=True):
        return await objects_api.list_objects(filter)


@mcp.tool
async def get_object(name: str) -> ObjectDetail:
    """Return full object detail including its schema."""
    async with orch_context(with_ch=True):
        return await objects_api.get_object(name)


@mcp.tool
async def delete_object(name: str) -> ObjectDeleted:
    """Drop a global-scope persistent object by name (idempotent)."""
    async with orch_context(with_ch=True):
        return await objects_api.delete_object(name)


@mcp.tool
async def purge_objects(request: PurgeObjectsRequest) -> PurgeObjectsResult:
    """Drop global-scope persistent objects filtered by creation time."""
    async with orch_context(with_ch=True):
        return await objects_api.purge_objects(request)


# --- lineage primitives -----------------------------------------------
#
# These are the building blocks an MCP client (itself an LLM agent) composes
# to investigate a pipeline: walk the graph, look at schemas, sample data.
# The turnkey AI agents ``explain_lineage`` / ``debug_result`` exist in
# ``internal_api.lineage`` for CLI use but are NOT exposed here — calling
# them from MCP would force an Ollama round-trip from inside an LLM client
# that is already perfectly capable of doing the reasoning itself.


@mcp.tool
async def oplog_subgraph(
    target_table: str,
    direction: LineageDirection = "backward",
    max_depth: int = 10,
) -> OplogGraph:
    """Return the lineage graph for ``target_table`` (backward or forward)."""
    async with orch_context(with_ch=True):
        return await lineage_api.oplog_subgraph(target_table, direction=direction, max_depth=max_depth)


@mcp.tool
async def query_table(
    sql: str,
    scope_tables: list[str],
    row_limit: int = DEFAULT_ROW_LIMIT,
) -> QueryResult:
    """Run a sandboxed read-only SELECT against tables in ``scope_tables``.

    ``scope_tables`` should come from a prior ``oplog_subgraph`` call
    (use ``OplogGraph.tables``). Rejects DDL/DML and out-of-scope refs.
    """
    async with orch_context(with_ch=True):
        return await lineage_api.query_table(sql, scope_tables=scope_tables, row_limit=row_limit)


@mcp.tool
async def get_table_schema(table: str, scope_tables: list[str]) -> TableSchema:
    """Return columns and types for ``table`` (must be in ``scope_tables``)."""
    async with orch_context(with_ch=True):
        return await lineage_api.get_table_schema(table, scope_tables=scope_tables)


# --- setup ------------------------------------------------------------


@mcp.tool
def setup(ai: bool = False) -> SetupResult:
    """Run environment setup — filesystem, SQL migrations, (optionally) AI deps."""
    return setup_api.setup(ai=ai)


@mcp.tool
def migrate(action: MigrationAction, revision: str | None = None) -> MigrationResult:
    """Run an alembic migration subcommand."""
    return setup_api.migrate(action, revision)


@mcp.tool
def bootstrap_ollama(
    model: str,
    base_url: str = setup_api.OLLAMA_BASE_URL,
) -> OllamaBootstrapResult:
    """Ensure an Ollama model is pulled on the configured server."""
    return setup_api.bootstrap_ollama(model, base_url=base_url)
