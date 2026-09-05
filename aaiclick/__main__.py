"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick setup                    # Initialize local dev environment
    python -m aaiclick setup --ai               # Also pull the configured Ollama model
    python -m aaiclick setup --force            # Recreate a local.db left behind by an older version
    python -m aaiclick migrate                  # Run database migrations
    python -m aaiclick migrate --help           # Show migration help
    python -m aaiclick local start              # Start REST + MCP server with execution workers (local mode)
    python -m aaiclick execution-worker start   # Start a distributed execution worker process
    python -m aaiclick execution-worker list    # List execution workers
    python -m aaiclick execution-worker stop <execution_worker_id>  # Stop an execution worker gracefully
    python -m aaiclick background start         # Start background cleanup worker
    python -m aaiclick job get <ref>            # Get job details (by ID or name)
    python -m aaiclick job stats <ref>          # Show job execution stats
    python -m aaiclick job cancel <ref>         # Cancel a job
    python -m aaiclick job list                 # List jobs
    python -m aaiclick job wait <ref>           # Block until a job reaches a terminal status
    python -m aaiclick job enable <name>        # Enable a registered job
    python -m aaiclick job disable <name>       # Disable a registered job
    python -m aaiclick task get <id>            # Get task details by ID
    python -m aaiclick register-job <entrypoint> # Register a job
    python -m aaiclick run-job <name>           # Run a job immediately
    python -m aaiclick run-job <name> --progress  # ...and block, showing task progress
    python -m aaiclick registered-job list      # List registered jobs
    python -m aaiclick data list                # List persistent objects
    python -m aaiclick data get <name>          # Show persistent object details
    python -m aaiclick data delete <name>       # Delete persistent object
    python -m aaiclick data purge --after ISO   # Delete persistent objects by time
    python -m aaiclick explain <table>          # AI: explain how a table was produced (needs aaiclick[ai])
    python -m aaiclick debug <table> "<question>"  # AI: debug a result with live-query tools (needs aaiclick[ai])
    python -m aaiclick docker init              # Scaffold a starter Dockerfile
    python -m aaiclick compose init             # Scaffold a docker-runner compose stack
    python -m aaiclick k8s init                 # Scaffold a helm chart
"""

import argparse
import asyncio
import json
import shlex
import sys
from contextlib import redirect_stdout
from contextvars import ContextVar
from datetime import datetime
from typing import Any, cast, get_args

from aaiclick import cli_renderers, cli_wait, internal_api
from aaiclick.ai.importing import import_ai_module
from aaiclick.auth import store as auth_store
from aaiclick.auth.models import ROLE_VIEWER, ROLES
from aaiclick.auth.view_models import CreateTenantRequest, CreateUserRequest, UserListFilter
from aaiclick.internal_api import setup as setup_api
from aaiclick.internal_api import tenants as tenants_api
from aaiclick.internal_api import users as users_api
from aaiclick.internal_api.errors import InternalApiError, NotFound
from aaiclick.orchestration.kubernetes_config import build_kubernetes_config
from aaiclick.orchestration.models import (
    JOB_COMPLETED,
    ExecutionWorkerStatus,
    JobStatus,
    PreservationMode,
    RunnerMode,
)
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.orchestration.runner_config import ENTRY_TYPES
from aaiclick.tenancy import active_tenant
from aaiclick.view_models import (
    ExecutionWorkerFilter,
    JobListFilter,
    MigrationAction,
    ObjectFilter,
    PurgeObjectsRequest,
    RefId,
    RegisteredJobFilter,
    RegisterJobRequest,
    RunJobRequest,
)

_JSON_HELP = "Emit JSON instead of a table"


def _add_json_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help=_JSON_HELP)


def _add_timeout_flag(parser: argparse.ArgumentParser, *, context: str) -> None:
    parser.add_argument(
        "--timeout",
        type=float,
        default=cli_wait.DEFAULT_WAIT_TIMEOUT,
        help=f"Seconds to wait {context} (default: {cli_wait.DEFAULT_WAIT_TIMEOUT:.0f})",
    )


def _add_set_flag(parser: argparse.ArgumentParser, *, verb: str) -> None:
    parser.add_argument(
        "--set",
        dest="set_kwargs",
        action="append",
        metavar="KEY=VALUE",
        default=None,
        help=f"{verb} kwarg as KEY=VALUE (repeatable); JSON-typed, wins over --kwargs",
    )


def _print_json(model) -> None:
    """Dump a pydantic view model as JSON on stdout."""
    print(model.model_dump_json())


def _render(args: argparse.Namespace, view, text_renderer) -> None:
    """Pick JSON or text rendering based on ``--json``."""
    if args.json:
        _print_json(view)
    else:
        text_renderer(view)


_tenant_slug: ContextVar[str | None] = ContextVar("cli_tenant_slug", default=None)
"""``--tenant`` slug for this invocation, set by ``main()`` before dispatch.

A ContextVar rather than a module global: the write site needs no ``global``
statement, and the value is scoped to the calling context instead of the
process, so it stays correct if commands are ever driven concurrently.
``asyncio.run`` copies the current context, so a value set here reaches the
coroutine.
"""


async def _resolve_tenant_id(slug: str) -> int:
    tenant = await auth_store.get_tenant_by_slug(slug)
    if tenant is None:
        raise NotFound(f"tenant '{slug}' not found")
    return tenant.id


async def _run_internal_api(coro, *, with_ch: bool = False):
    """Run ``coro`` inside ``orch_context``, mapping API errors to exit 1.

    When the top-level ``--tenant`` flag is set, the command runs with that
    tenant active (contextvars are read at await time, so setting the scope
    here covers the already-created coroutine).

    Args:
        coro: The ``internal_api`` coroutine to await.
        with_ch: Attach a ClickHouse client — required by the ``data``
            subcommands, unnecessary for the orchestration ones.
    """
    slug = _tenant_slug.get()
    try:
        async with orch_context(with_ch=with_ch):
            if slug is None:
                return await coro
            with active_tenant(await _resolve_tenant_id(slug)):
                return await coro
    except InternalApiError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


async def _run_data_api(coro):
    """Run a ``data`` subcommand with ClickHouse attached — ``open_object``
    needs the SQL ``table_registry`` session only an orch context provides."""
    return await _run_internal_api(coro, with_ch=True)


def _run_sync_api(call):
    """Invoke a sync ``internal_api`` callable, mapping API errors to exit 1."""
    try:
        return call()
    except InternalApiError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


async def _run_job_list(args: argparse.Namespace) -> None:
    filter = JobListFilter(
        status=cast(JobStatus, args.status) if args.status else None,
        name=args.like,
        limit=args.limit,
        offset=args.offset,
    )
    page = await _run_internal_api(internal_api.list_jobs(filter))
    _render(args, page, lambda p: cli_renderers.render_jobs_page(p, offset=args.offset))


async def _run_job_get(args: argparse.Namespace) -> None:
    detail = await _run_internal_api(internal_api.get_job(args.ref))
    _render(args, detail, cli_renderers.render_job_detail)


async def _run_job_stats(args: argparse.Namespace) -> None:
    stats = await _run_internal_api(internal_api.job_stats(args.ref))
    _render(args, stats, cli_renderers.render_job_stats)


async def _run_job_cancel(args: argparse.Namespace) -> None:
    view = await _run_internal_api(internal_api.cancel_job(args.ref))
    _render(args, view, cli_renderers.render_job_cancelled)


def _parse_preservation_mode(value: str | None) -> PreservationMode | None:
    return cast(PreservationMode, value) if value else None


def _parse_command_env(pairs: list[str] | None) -> dict[str, str] | None:
    """Parse repeated ``KEY=VALUE`` CLI args into a dict (None if none given)."""
    if not pairs:
        return None
    result: dict[str, str] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep:
            raise ValueError(f"--command-env expects KEY=VALUE, got {pair!r}")
        result[key] = value
    return result


def _parse_set_kwargs(pairs: list[str] | None) -> dict[str, Any]:
    """Parse repeated ``--set KEY=VALUE`` args into typed job kwargs.

    Values are JSON-parsed so ``corpus_size=300`` arrives as an ``int`` and
    ``generate=true`` as a ``bool``, falling back to the raw string when the
    value is not valid JSON (``name=hello``). Splits on the first ``=`` only,
    so ``expr=a=b`` keeps its value intact.
    """
    if not pairs:
        return {}
    result: dict[str, Any] = {}
    for pair in pairs:
        key, sep, value = pair.partition("=")
        if not sep:
            raise ValueError(f"--set expects KEY=VALUE, got {pair!r}")
        try:
            result[key] = json.loads(value)
        except json.JSONDecodeError:
            result[key] = value
    return result


async def _run_run_job(args: argparse.Namespace) -> None:
    kwargs: dict = json.loads(args.kwargs) if args.kwargs else {}
    kwargs.update(_parse_set_kwargs(args.set_kwargs))
    request = RunJobRequest(
        name=args.name,
        kwargs=kwargs,
        preservation_mode=_parse_preservation_mode(args.preservation_mode),
        git_remote=args.git_remote,
        git_sha=args.git_sha,
        git_branch=args.git_branch,
        dockerfile=args.dockerfile,
        namespace=args.namespace,
        service_account=args.k8s_service_account,
        image_pull_secret=args.k8s_image_pull_secret,
        entry_type=args.entry_type,
        command=shlex.split(args.command_str) if args.command_str else None,
        command_env=_parse_command_env(args.command_env),
        image=args.image,
    )
    view = await _run_internal_api(internal_api.run_job(request))
    # With --progress --json the final stats are the document callers parse;
    # emitting the created-job view too would put two JSON objects on stdout.
    if not (args.progress and args.json):
        _render(args, view, cli_renderers.render_job_created)
    if args.progress:
        await _wait_and_exit(view.id, args)


async def _wait_and_exit(ref: RefId, args: argparse.Namespace) -> None:
    """Block until ``ref`` is terminal, report it, and exit with its status code.

    Only ``COMPLETED`` exits 0 — a failed, cancelled, or timed-out job must be
    visible to ``set -e`` scripts and CI steps.
    """
    # Progress reports would corrupt the single JSON document --json promises.
    on_change = None if args.json else cli_renderers.render_job_stats
    try:
        stats = await _run_internal_api(cli_wait.wait_for_job(ref, timeout=args.timeout, on_change=on_change))
    except cli_wait.JobWaitTimeout as exc:
        print(exc, file=sys.stderr)
        # Progress was suppressed under --json, so nothing has named the stuck
        # task yet. Dump it to stderr — stdout stays one parseable document.
        if args.json:
            with redirect_stdout(sys.stderr):
                cli_renderers.render_job_stats(exc.stats)
        sys.exit(1)

    if args.json:
        _print_json(stats)

    if stats.job_status != JOB_COMPLETED:
        # Under --json the document above already carries every task error;
        # appending the human-readable block would make stdout unparseable.
        if not args.json:
            cli_renderers.render_job_failure(stats)
        sys.exit(1)


async def _run_job_wait(args: argparse.Namespace) -> None:
    await _wait_and_exit(args.ref, args)


async def _run_register_job(args: argparse.Namespace) -> None:
    default_kwargs: dict | None = json.loads(args.kwargs) if args.kwargs else None
    set_kwargs = _parse_set_kwargs(args.set_kwargs)
    if set_kwargs:
        default_kwargs = {**(default_kwargs or {}), **set_kwargs}
    kubernetes_config = build_kubernetes_config(
        namespace=args.namespace,
        service_account=args.k8s_service_account,
        image_pull_secret=args.k8s_image_pull_secret,
    )
    request = RegisterJobRequest(
        name=args.name or "",
        entrypoint=args.entrypoint,
        schedule=args.schedule,
        default_kwargs=default_kwargs,
        preservation_mode=_parse_preservation_mode(args.preservation_mode),
        runner_mode=args.runner,
        dockerfile=args.dockerfile,
        git_remote=args.git_remote,
        kubernetes_config=kubernetes_config,
        image=args.image,
    )
    view = await _run_internal_api(internal_api.register_job(request))
    _render(args, view, cli_renderers.render_registered_job)


async def _run_registered_job_list(args: argparse.Namespace) -> None:
    filter = RegisteredJobFilter(
        enabled=args.enabled,
        name=args.like,
        limit=args.limit,
        offset=args.offset,
    )
    page = await _run_internal_api(internal_api.list_registered_jobs(filter))
    _render(args, page, lambda p: cli_renderers.render_registered_jobs_page(p, offset=args.offset))


async def _run_job_enable(args: argparse.Namespace) -> None:
    view = await _run_internal_api(internal_api.enable_job(args.name))
    _render(args, view, cli_renderers.render_registered_job_enabled)


async def _run_job_disable(args: argparse.Namespace) -> None:
    view = await _run_internal_api(internal_api.disable_job(args.name))
    _render(args, view, cli_renderers.render_registered_job_disabled)


async def _run_task_get(args: argparse.Namespace) -> None:
    detail = await _run_internal_api(internal_api.get_task(args.task_id))
    _render(args, detail, cli_renderers.render_task_detail)


def _parse_datetime(value: str) -> datetime:
    """Parse an ISO 8601 datetime supplied at the CLI boundary."""
    value = value.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise ValueError(f"Invalid datetime format: {value!r}. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS")


async def _run_data_list(args: argparse.Namespace) -> None:
    filter = ObjectFilter(prefix=args.prefix, limit=args.limit)
    page = await _run_data_api(internal_api.list_objects(filter))
    _render(args, page, cli_renderers.render_objects_page)


async def _run_data_get(args: argparse.Namespace) -> None:
    detail = await _run_data_api(internal_api.get_object(args.name))
    _render(args, detail, cli_renderers.render_object_detail)


async def _run_data_delete(args: argparse.Namespace) -> None:
    view = await _run_data_api(internal_api.delete_object(args.name))
    _render(args, view, cli_renderers.render_object_deleted)


async def _run_data_purge(args: argparse.Namespace) -> None:
    request = PurgeObjectsRequest(
        after=_parse_datetime(args.after) if args.after else None,
        before=_parse_datetime(args.before) if args.before else None,
    )
    result = await _run_data_api(internal_api.purge_objects(request))
    _render(args, result, cli_renderers.render_objects_purged)


def _load_lineage_ai():
    """Import ``internal_api.lineage_ai`` on demand, exiting 1 without the ``ai`` extra.

    The module pulls in litellm, so it is loaded per command rather than at
    the top of this file — the rest of the CLI must keep working without the
    optional dependency.
    """
    try:
        return import_ai_module("aaiclick.internal_api.lineage_ai")
    except ImportError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


async def _run_explain(args: argparse.Namespace) -> None:
    lineage_ai = _load_lineage_ai()
    answer = await _run_data_api(lineage_ai.explain_lineage(args.table, question=args.question))
    _render(args, answer, cli_renderers.render_lineage_answer)


async def _run_debug(args: argparse.Namespace) -> None:
    lineage_ai = _load_lineage_ai()
    answer = await _run_data_api(
        lineage_ai.debug_result(args.table, question=args.question, max_iterations=args.max_iterations)
    )
    _render(args, answer, cli_renderers.render_lineage_answer)


async def _run_execution_worker_list(args: argparse.Namespace) -> None:
    filter = ExecutionWorkerFilter(
        status=cast(ExecutionWorkerStatus, args.status) if args.status else None,
        limit=args.limit,
        offset=args.offset,
    )
    page = await _run_internal_api(internal_api.list_execution_workers(filter))
    _render(args, page, lambda p: cli_renderers.render_execution_workers_page(p, offset=args.offset))


async def _run_execution_worker_stop(args: argparse.Namespace) -> None:
    view = await _run_internal_api(internal_api.stop_execution_worker(args.execution_worker_id))
    _render(args, view, cli_renderers.render_execution_worker_stopped)


async def _run_user_create(args: argparse.Namespace) -> None:
    view = await _run_internal_api(
        users_api.create_user(
            CreateUserRequest(username=args.username, password=args.password, superadmin=args.superadmin)
        )
    )
    _render(args, view, cli_renderers.render_user)


async def _run_user_list(args: argparse.Namespace) -> None:
    page = await _run_internal_api(users_api.list_users(UserListFilter(limit=args.limit, offset=args.offset)))
    _render(args, page, lambda p: cli_renderers.render_users_page(p, offset=args.offset))


async def _run_user_set_superadmin(args: argparse.Namespace) -> None:
    view = await _run_internal_api(users_api.set_superadmin(args.user_id, args.superadmin == "true"))
    _render(args, view, cli_renderers.render_user)


async def _run_user_disable(args: argparse.Namespace) -> None:
    view = await _run_internal_api(users_api.disable_user(args.user_id, True))
    _render(args, view, cli_renderers.render_user)


async def _run_user_passwd(args: argparse.Namespace) -> None:
    view = await _run_internal_api(users_api.set_password(args.user_id, args.password))
    _render(args, view, cli_renderers.render_user)


async def _run_tenant_create(args: argparse.Namespace) -> None:
    request = CreateTenantRequest(slug=args.slug, name=args.name or args.slug)
    view = await _run_internal_api(tenants_api.create_tenant(request))
    _render(args, view, cli_renderers.render_tenant)


async def _run_tenant_list(args: argparse.Namespace) -> None:
    page = await _run_internal_api(tenants_api.list_tenants())
    _render(args, page, cli_renderers.render_tenants_page)


async def _resolve_member(args: argparse.Namespace) -> tuple[int, int]:
    """Resolve ``--tenant`` slug and ``--username`` to their ids."""
    tenant_id = await _resolve_tenant_id(args.tenant)
    user = await auth_store.get_user_by_username(args.username)
    if user is None:
        raise NotFound(f"user '{args.username}' not found")
    return tenant_id, user.id


async def _run_member_set(args: argparse.Namespace) -> None:
    async def do():
        tenant_id, user_id = await _resolve_member(args)
        return await tenants_api.set_member(tenant_id, user_id, args.role)

    view = await _run_internal_api(do())
    _render(args, view, cli_renderers.render_member)


async def _run_member_remove(args: argparse.Namespace) -> None:
    async def do():
        tenant_id, user_id = await _resolve_member(args)
        await tenants_api.remove_member(tenant_id, user_id)

    await _run_internal_api(do())
    print(f"removed '{args.username}' from tenant '{args.tenant}'")


_MIGRATE_HELP = """\
Database Migration Commands
==================================================

Usage: python -m aaiclick migrate [command] [options]

Commands:
  upgrade [revision]   Upgrade to a later version (default: head)
  downgrade [revision] Revert to a previous version
  current              Display current revision
  history              List migration history
  heads                Show current available heads
  show [revision]      Show details about a revision

upgrade also applies pending ClickHouse migrations after the SQL upgrade;
current and history report both databases.

Examples:
  python -m aaiclick migrate                 # Upgrade to latest
  python -m aaiclick migrate upgrade head    # Upgrade to latest
  python -m aaiclick migrate downgrade -1    # Downgrade one revision
  python -m aaiclick migrate current         # Show current revision
  python -m aaiclick migrate history         # Show migration history

Environment Variables:
  POSTGRES_HOST       PostgreSQL host (default: localhost)
  POSTGRES_PORT       PostgreSQL port (default: 5432)
  POSTGRES_USER       PostgreSQL user (default: aaiclick)
  POSTGRES_PASSWORD   PostgreSQL password (default: secret)
  POSTGRES_DB         PostgreSQL database (default: aaiclick)\
"""


def _confirm_local_db_reset(stale: list[str]) -> bool:
    """Ask before deleting a local database whose schema predates this version."""
    print(setup_api.stale_local_db_message(stale), file=sys.stderr)
    return input("Delete and recreate it? [y/N] ").strip().lower() in {"y", "yes"}


def _run_setup_cli(args: argparse.Namespace) -> None:
    force = args.force
    # Prompt only for an interactive run: piped or --json callers fall through
    # to setup(), which raises with the same guidance rather than blocking.
    if not force and not args.json and sys.stdin.isatty():
        stale = setup_api.stale_local_db()
        if stale:
            force = _confirm_local_db_reset(stale)
    result = _run_sync_api(lambda: setup_api.setup(ai=args.ai, force=force))
    _render(args, result, cli_renderers.render_setup_result)


def _run_docker_init(args: argparse.Namespace) -> None:
    from pathlib import Path

    from aaiclick.orchestration.execution.docker_scaffold import (
        DockerfileExists,
        init_dockerfile,
    )

    target = Path(args.path)
    try:
        written = init_dockerfile(target, force=args.force)
    except DockerfileExists as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    print(f"Wrote {written}")


def _run_compose_init(args: argparse.Namespace) -> None:
    from pathlib import Path

    from aaiclick.deploy import ComposeFileExists, init_compose

    target = Path(args.path)
    try:
        written = init_compose(target, image_tag=args.image_tag, force=args.force)
    except ComposeFileExists as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    print(f"Wrote {written}")


def _run_k8s_init(args: argparse.Namespace) -> None:
    from pathlib import Path

    from aaiclick.deploy import HelmChartExists, init_helm

    target = Path(args.path)
    try:
        written = init_helm(target, image_tag=args.image_tag, force=args.force)
    except HelmChartExists as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    print(f"Wrote {written}")


def _run_migrate_cli(args: argparse.Namespace) -> None:
    raw_args = list(args.args) if hasattr(args, "args") else []
    if not raw_args or raw_args[0] in ("-h", "--help"):
        print(_MIGRATE_HELP)
        return

    action_name, *rest = raw_args
    if action_name not in get_args(MigrationAction):
        print(f"Unknown command: {action_name}", file=sys.stderr)
        print("Run 'python -m aaiclick migrate --help' for usage", file=sys.stderr)
        sys.exit(1)
    action = cast(MigrationAction, action_name)
    revision = rest[0] if rest else None

    result = _run_sync_api(lambda: setup_api.migrate(action, revision))
    _render(args, result, cli_renderers.render_migration_result)


def build_parser() -> argparse.ArgumentParser:
    """Build the top-level argparse parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="aaiclick",
        description="aaiclick command-line interface",
    )
    parser.add_argument(
        "--tenant",
        dest="global_tenant",
        default=None,
        help="Tenant slug to run the command in (default: the default tenant)",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # Add setup subcommand
    setup_parser = subparsers.add_parser(
        "setup",
        help="Initialize local dev environment (SQLite + chdb)",
    )
    setup_parser.add_argument(
        "--ai",
        action="store_true",
        default=False,
        help="Also pull the configured Ollama model (reads AAICLICK_AI_MODEL)",
    )
    setup_parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help=(
            "Delete and recreate the local SQLite database when its schema predates "
            "this version (a database already up to date is left alone)"
        ),
    )
    _add_json_flag(setup_parser)

    # Add migrate subcommand
    migrate_parser = subparsers.add_parser(
        "migrate",
        help="Run database migrations",
    )
    migrate_parser.add_argument(
        "args",
        nargs="*",
        help="Additional arguments for migration command",
    )
    _add_json_flag(migrate_parser)

    # Add local subcommand (single-process: worker + background)
    local_parser = subparsers.add_parser(
        "local",
        help="Local mode commands (single process, chdb + SQLite)",
    )
    local_subparsers = local_parser.add_subparsers(
        dest="local_command",
        help="Local commands",
    )

    # local start
    local_start_parser = local_subparsers.add_parser(
        "start",
        help="Start the combined REST + MCP server with workers (local mode)",
    )
    local_start_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind (default: 127.0.0.1)",
    )
    local_start_parser.add_argument(
        "--port",
        type=int,
        default=5255,
        help="Port to bind (default: 5255)",
    )
    local_start_parser.add_argument(
        "--reload",
        action="store_true",
        help="Auto-restart on code change (dev)",
    )

    # Add execution-worker subcommand (distributed mode)
    execution_worker_parser = subparsers.add_parser(
        "execution-worker",
        help="Distributed execution worker management commands",
    )
    execution_worker_subparsers = execution_worker_parser.add_subparsers(
        dest="execution_worker_command",
        help="Execution worker commands",
    )

    # execution-worker start
    execution_worker_start_parser = execution_worker_subparsers.add_parser(
        "start",
        help="Start a distributed execution worker process",
    )
    execution_worker_start_parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Maximum tasks to execute (default: unlimited)",
    )

    # execution-worker list
    execution_worker_list_parser = execution_worker_subparsers.add_parser(
        "list",
        help="List execution workers",
    )
    execution_worker_list_parser.add_argument(
        "--status",
        choices=list(get_args(ExecutionWorkerStatus)),
        default=None,
        help="Filter by status",
    )
    execution_worker_list_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum results (default: 50)",
    )
    execution_worker_list_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip N results (default: 0)",
    )
    _add_json_flag(execution_worker_list_parser)

    # execution-worker stop
    execution_worker_stop_parser = execution_worker_subparsers.add_parser(
        "stop",
        help="Request an execution worker to stop gracefully",
    )
    execution_worker_stop_parser.add_argument(
        "execution_worker_id",
        type=int,
        help="Execution worker ID to stop",
    )
    _add_json_flag(execution_worker_stop_parser)

    # Add job subcommand
    job_parser = subparsers.add_parser(
        "job",
        help="Job management commands",
    )
    job_subparsers = job_parser.add_subparsers(
        dest="job_command",
        help="Job commands",
    )

    # job get <ref>
    job_get_parser = job_subparsers.add_parser(
        "get",
        help="Get job details by ID or name",
    )
    job_get_parser.add_argument("ref", type=str, help="Job ID or name")
    _add_json_flag(job_get_parser)

    # job stats <ref>
    job_stats_parser = job_subparsers.add_parser(
        "stats",
        help="Show job execution stats",
    )
    job_stats_parser.add_argument("ref", type=str, help="Job ID or name")
    _add_json_flag(job_stats_parser)

    # job cancel <ref>
    job_cancel_parser = job_subparsers.add_parser(
        "cancel",
        help="Cancel a job and its non-terminal tasks",
    )
    job_cancel_parser.add_argument("ref", type=str, help="Job ID or name")
    _add_json_flag(job_cancel_parser)

    # job list
    job_list_parser = job_subparsers.add_parser(
        "list",
        help="List jobs",
    )
    job_list_parser.add_argument(
        "--status",
        choices=list(get_args(JobStatus)),
        default=None,
        help="Filter by status",
    )
    job_list_parser.add_argument(
        "--like",
        default=None,
        help="Filter by name pattern (SQL LIKE, e.g. '%%etl%%')",
    )
    job_list_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum results (default: 50)",
    )
    job_list_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip N results (default: 0)",
    )
    _add_json_flag(job_list_parser)

    # job wait <ref>
    job_wait_parser = job_subparsers.add_parser(
        "wait",
        help="Block until a job reaches a terminal status",
    )
    job_wait_parser.add_argument("ref", type=str, help="Job ID or name")
    _add_timeout_flag(job_wait_parser, context="before giving up")
    _add_json_flag(job_wait_parser)

    # job enable <name>
    job_enable_parser = job_subparsers.add_parser(
        "enable",
        help="Enable a registered job",
    )
    job_enable_parser.add_argument("name", type=str, help="Registered job name")
    _add_json_flag(job_enable_parser)

    # job disable <name>
    job_disable_parser = job_subparsers.add_parser(
        "disable",
        help="Disable a registered job",
    )
    job_disable_parser.add_argument("name", type=str, help="Registered job name")
    _add_json_flag(job_disable_parser)

    # Add task subcommand
    task_parser = subparsers.add_parser(
        "task",
        help="Task management commands",
    )
    task_subparsers = task_parser.add_subparsers(
        dest="task_command",
        help="Task commands",
    )

    # task get <id>
    task_get_parser = task_subparsers.add_parser(
        "get",
        help="Get task details by ID",
    )
    task_get_parser.add_argument("task_id", type=int, help="Task ID")
    _add_json_flag(task_get_parser)

    # Add register-job subcommand
    register_job_parser = subparsers.add_parser(
        "register-job",
        help="Register a job in the catalog",
    )
    register_job_parser.add_argument("entrypoint", type=str, help="Python dotted path (e.g. myapp.pipelines.etl_job)")
    register_job_parser.add_argument("--name", default=None, help="Job name (default: last segment of entrypoint)")
    register_job_parser.add_argument("--schedule", default=None, help="Cron expression (e.g. '0 8 * * *')")
    register_job_parser.add_argument("--kwargs", default=None, help="Default kwargs as JSON string")
    _add_set_flag(register_job_parser, verb="Default")
    register_job_parser.add_argument(
        "--preservation-mode",
        choices=list(get_args(PreservationMode)),
        default=None,
        help="Default preservation mode for every run of this job (runs can override)",
    )
    register_job_parser.add_argument(
        "--runner",
        choices=list(get_args(RunnerMode)),
        default="subprocess",
        help="Task-execution runner (default: subprocess)",
    )
    register_job_parser.add_argument(
        "--dockerfile",
        default=None,
        help="Default Dockerfile path relative to the repo root (docker runner only)",
    )
    register_job_parser.add_argument(
        "--git-remote",
        default=None,
        help="Default git remote URL (docker runner only); auto-detected if omitted",
    )
    register_job_parser.add_argument(
        "--namespace",
        default=None,
        help="Kubernetes namespace (kubernetes runner only); falls back to $AAICLICK_K8S_NAMESPACE, then 'default'",
    )
    register_job_parser.add_argument(
        "--k8s-service-account",
        default=None,
        help="Kubernetes service account name (kubernetes runner only); falls back to $AAICLICK_K8S_SERVICE_ACCOUNT",
    )
    register_job_parser.add_argument(
        "--k8s-image-pull-secret",
        default=None,
        help="Kubernetes imagePullSecret name (kubernetes runner only); falls back to $AAICLICK_K8S_IMAGE_PULL_SECRET",
    )
    register_job_parser.add_argument(
        "--image",
        default=None,
        help="Default prebuilt image to run verbatim (no build stage)",
    )
    _add_json_flag(register_job_parser)

    # Add run-job subcommand
    run_job_parser = subparsers.add_parser(
        "run-job",
        help="Run a job immediately (auto-registers if needed)",
    )
    run_job_parser.add_argument("name", type=str, help="Job name or entrypoint")
    run_job_parser.add_argument("--kwargs", default=None, help="Override kwargs as JSON string")
    _add_set_flag(run_job_parser, verb="Override")
    run_job_parser.add_argument(
        "--preservation-mode",
        choices=list(get_args(PreservationMode)),
        default=None,
        help="Table preservation mode (default: AAICLICK_DEFAULT_PRESERVATION_MODE or NONE)",
    )
    run_job_parser.add_argument(
        "--git-remote",
        default=None,
        help="Override the registered job's default git remote (docker runner only)",
    )
    run_job_parser.add_argument(
        "--git-sha",
        default=None,
        help="Pin the build to a specific commit SHA (docker runner only)",
    )
    run_job_parser.add_argument(
        "--git-branch",
        default=None,
        help="Capture branch name as build-arg metadata (docker runner only)",
    )
    run_job_parser.add_argument(
        "--dockerfile",
        default=None,
        help="Override the registered job's dockerfile path (docker runner only)",
    )
    run_job_parser.add_argument(
        "--namespace",
        default=None,
        help="Override the kubernetes namespace for this run (kubernetes runner only)",
    )
    run_job_parser.add_argument(
        "--k8s-service-account",
        default=None,
        help="Override the kubernetes service account for this run (kubernetes runner only)",
    )
    run_job_parser.add_argument(
        "--k8s-image-pull-secret",
        default=None,
        help="Override the kubernetes imagePullSecret for this run (kubernetes runner only)",
    )
    run_job_parser.add_argument(
        "--entry-type",
        choices=ENTRY_TYPES,
        default="module",
        help="How to run the task: 'module' (dotted entrypoint, default) or 'shell' (run --command in the runner's environment)",
    )
    run_job_parser.add_argument(
        "--command",
        dest="command_str",
        default=None,
        help="Shell command (argv, shlex-split) to run for --entry-type shell, e.g. --command 'python main.py'",
    )
    run_job_parser.add_argument(
        "--command-env",
        action="append",
        metavar="KEY=VALUE",
        default=None,
        help="Env var for a shell task (repeatable), e.g. --command-env K=v",
    )
    run_job_parser.add_argument(
        "--image",
        default=None,
        help="Prebuilt image to run verbatim (no build stage); mutually exclusive with --git-*",
    )
    run_job_parser.add_argument(
        "--progress",
        action="store_true",
        help="Block until the job finishes, printing task progress; exits non-zero if it fails",
    )
    _add_timeout_flag(run_job_parser, context="with --progress")
    _add_json_flag(run_job_parser)

    # Add registered-job subcommand
    registered_job_parser = subparsers.add_parser(
        "registered-job",
        help="Registered job management commands",
    )
    registered_job_subparsers = registered_job_parser.add_subparsers(
        dest="registered_job_command",
        help="Registered job commands",
    )

    # registered-job list
    registered_job_list_parser = registered_job_subparsers.add_parser(
        "list",
        help="List registered jobs",
    )
    registered_job_list_parser.add_argument(
        "--enabled",
        dest="enabled",
        action="store_const",
        const=True,
        default=None,
        help="Only list enabled registrations",
    )
    registered_job_list_parser.add_argument(
        "--disabled",
        dest="enabled",
        action="store_const",
        const=False,
        help="Only list disabled registrations",
    )
    registered_job_list_parser.add_argument(
        "--like",
        default=None,
        help="Filter by name pattern (SQL LIKE, e.g. '%%etl%%')",
    )
    registered_job_list_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum results (default: 50)",
    )
    registered_job_list_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip N results (default: 0)",
    )
    _add_json_flag(registered_job_list_parser)

    # Add data subcommand
    data_parser = subparsers.add_parser(
        "data",
        help="Persistent data management commands",
    )
    data_subparsers = data_parser.add_subparsers(
        dest="data_command",
        help="Data commands",
    )

    # data list
    data_list_parser = data_subparsers.add_parser(
        "list",
        help="List persistent objects",
    )
    data_list_parser.add_argument(
        "--prefix",
        default=None,
        help="Filter by name prefix",
    )
    data_list_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum results (default: 50)",
    )
    _add_json_flag(data_list_parser)

    # data get <name>
    data_get_parser = data_subparsers.add_parser(
        "get",
        help="Show persistent object details",
    )
    data_get_parser.add_argument("name", type=str, help="Persistent object name")
    _add_json_flag(data_get_parser)

    # data delete <name>
    data_delete_parser = data_subparsers.add_parser(
        "delete",
        help="Delete a single persistent object",
    )
    data_delete_parser.add_argument("name", type=str, help="Persistent object name")
    _add_json_flag(data_delete_parser)

    # data purge [--after] [--before]
    data_purge_parser = data_subparsers.add_parser(
        "purge",
        help="Delete persistent objects by creation time",
    )
    data_purge_parser.add_argument(
        "--after",
        default=None,
        help="Delete tables created at or after this time (ISO 8601)",
    )
    data_purge_parser.add_argument(
        "--before",
        default=None,
        help="Delete tables created before this time (ISO 8601)",
    )
    _add_json_flag(data_purge_parser)

    # explain <table> [question]
    explain_parser = subparsers.add_parser(
        "explain",
        help="AI: explain how a table was produced from its lineage (requires aaiclick[ai])",
    )
    explain_parser.add_argument("table", type=str, help="Target ClickHouse table name")
    explain_parser.add_argument(
        "question",
        nargs="?",
        default=None,
        help="Question to answer instead of the default 'how was this produced?'",
    )
    _add_json_flag(explain_parser)

    # debug <table> <question>
    debug_parser = subparsers.add_parser(
        "debug",
        help="AI: answer a 'why' question about a table using live-query tools (requires aaiclick[ai])",
    )
    debug_parser.add_argument("table", type=str, help="Target ClickHouse table name")
    debug_parser.add_argument("question", type=str, help='Question, e.g. "Why is this value negative?"')
    debug_parser.add_argument(
        "--max-iterations",
        type=int,
        default=10,
        help="Maximum tool-call rounds before the agent is asked for a final answer (default: 10)",
    )
    _add_json_flag(debug_parser)

    # Add background subcommand
    background_parser = subparsers.add_parser(
        "background",
        help="Background service commands",
    )
    background_subparsers = background_parser.add_subparsers(
        dest="background_command",
        help="Background commands",
    )

    # background start
    background_start_parser = background_subparsers.add_parser(
        "start",
        help="Start background cleanup worker",
    )
    background_start_parser.add_argument(
        "--poll-interval",
        type=float,
        default=10.0,
        help="Cleanup poll interval in seconds (default: 10)",
    )

    # Add docker subcommand
    docker_parser = subparsers.add_parser(
        "docker",
        help="Docker-runner helpers",
    )
    docker_subparsers = docker_parser.add_subparsers(
        dest="docker_command",
        help="Docker commands",
    )

    # docker init
    docker_init_parser = docker_subparsers.add_parser(
        "init",
        help="Scaffold a starter Dockerfile in the current directory",
    )
    docker_init_parser.add_argument(
        "--path",
        default="Dockerfile",
        help="Output path (default: ./Dockerfile)",
    )
    docker_init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing file",
    )

    # Add compose subcommand
    compose_parser = subparsers.add_parser(
        "compose",
        help="Docker-compose deployment helpers",
    )
    compose_subparsers = compose_parser.add_subparsers(
        dest="compose_command",
        help="Compose commands",
    )

    # compose init
    compose_init_parser = compose_subparsers.add_parser(
        "init",
        help="Scaffold a docker-runner compose stack in the current directory",
    )
    compose_init_parser.add_argument(
        "--path",
        default="docker-compose.yaml",
        help="Output path (default: ./docker-compose.yaml)",
    )
    compose_init_parser.add_argument(
        "--image-tag",
        default=None,
        help="GHCR image tag to pin (default: v<installed aaiclick version>)",
    )
    compose_init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing file",
    )

    # Add k8s subcommand
    k8s_parser = subparsers.add_parser(
        "k8s",
        help="Kubernetes deployment helpers",
    )
    k8s_subparsers = k8s_parser.add_subparsers(
        dest="k8s_command",
        help="Kubernetes commands",
    )

    # k8s init
    k8s_init_parser = k8s_subparsers.add_parser(
        "init",
        help="Scaffold a helm chart in the current directory",
    )
    k8s_init_parser.add_argument(
        "--path",
        default="aaiclick-chart",
        help="Output directory (default: ./aaiclick-chart)",
    )
    k8s_init_parser.add_argument(
        "--image-tag",
        default=None,
        help="GHCR image tag to pin (default: v<installed aaiclick version>)",
    )
    k8s_init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing directory",
    )

    # Add user subcommand (administration)
    user_parser = subparsers.add_parser(
        "user",
        help="User administration",
    )
    user_subparsers = user_parser.add_subparsers(
        dest="user_command",
        help="User commands",
    )

    user_create_parser = user_subparsers.add_parser("create", help="Create a user")
    user_create_parser.add_argument("username")
    user_create_parser.add_argument("--password", required=True)
    user_create_parser.add_argument("--superadmin", action="store_true")
    _add_json_flag(user_create_parser)

    user_list_parser = user_subparsers.add_parser("list", help="List users")
    user_list_parser.add_argument("--limit", type=int, default=50)
    user_list_parser.add_argument("--offset", type=int, default=0)
    _add_json_flag(user_list_parser)

    user_set_superadmin_parser = user_subparsers.add_parser(
        "set-superadmin", help="Grant or revoke the instance superadmin flag"
    )
    user_set_superadmin_parser.add_argument("user_id", type=int)
    user_set_superadmin_parser.add_argument("superadmin", choices=["true", "false"])
    _add_json_flag(user_set_superadmin_parser)

    user_disable_parser = user_subparsers.add_parser("disable", help="Disable a user")
    user_disable_parser.add_argument("user_id", type=int)
    _add_json_flag(user_disable_parser)

    user_passwd_parser = user_subparsers.add_parser("passwd", help="Set a user's password")
    user_passwd_parser.add_argument("user_id", type=int)
    user_passwd_parser.add_argument("--password", required=True)
    _add_json_flag(user_passwd_parser)

    # Add tenant subcommand (administration)
    tenant_parser = subparsers.add_parser("tenant", help="Tenant administration")
    tenant_subparsers = tenant_parser.add_subparsers(dest="tenant_command", help="Tenant commands")

    tenant_create_parser = tenant_subparsers.add_parser("create", help="Create a tenant")
    tenant_create_parser.add_argument("slug")
    tenant_create_parser.add_argument("--name", default=None, help="Display name (default: the slug)")
    _add_json_flag(tenant_create_parser)

    tenant_list_parser = tenant_subparsers.add_parser("list", help="List tenants")
    _add_json_flag(tenant_list_parser)

    # Add member subcommand (tenant membership administration)
    member_parser = subparsers.add_parser("member", help="Tenant membership administration")
    member_subparsers = member_parser.add_subparsers(dest="member_command", help="Membership commands")

    for member_cmd, member_help in (("add", "Add a user to a tenant"), ("set-role", "Change a member's role")):
        member_set_parser = member_subparsers.add_parser(member_cmd, help=member_help)
        member_set_parser.add_argument("--tenant", required=True, help="Tenant slug")
        member_set_parser.add_argument("--username", required=True)
        member_set_parser.add_argument("--role", choices=list(ROLES), default=ROLE_VIEWER)
        _add_json_flag(member_set_parser)

    member_remove_parser = member_subparsers.add_parser("remove", help="Remove a user from a tenant")
    member_remove_parser.add_argument("--tenant", required=True, help="Tenant slug")
    member_remove_parser.add_argument("--username", required=True)
    _add_json_flag(member_remove_parser)

    return parser


def _subcommand_parsers(parser: argparse.ArgumentParser) -> dict[str, argparse.ArgumentParser]:
    """Return the mapping of subcommand name to its parser (for help fallbacks)."""
    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return action.choices
    return {}


def main():
    """Main CLI entry point."""
    parser = build_parser()
    subcommands = _subcommand_parsers(parser)
    args = parser.parse_args()
    _tenant_slug.set(args.global_tenant)

    if args.command == "setup":
        _run_setup_cli(args)

    elif args.command == "migrate":
        _run_migrate_cli(args)

    elif args.command == "local":
        if args.local_command == "start":
            from aaiclick.orchestration.cli import start_local

            asyncio.run(start_local(host=args.host, port=args.port, reload=args.reload))

        else:
            subcommands["local"].print_help()

    elif args.command == "execution-worker":
        if args.execution_worker_command == "start":
            from aaiclick.orchestration.cli import start_execution_worker

            asyncio.run(start_execution_worker(max_tasks=args.max_tasks))

        elif args.execution_worker_command == "list":
            asyncio.run(_run_execution_worker_list(args))

        elif args.execution_worker_command == "stop":
            asyncio.run(_run_execution_worker_stop(args))

        else:
            subcommands["execution-worker"].print_help()

    elif args.command == "job":
        if args.job_command == "get":
            asyncio.run(_run_job_get(args))

        elif args.job_command == "stats":
            asyncio.run(_run_job_stats(args))

        elif args.job_command == "cancel":
            asyncio.run(_run_job_cancel(args))

        elif args.job_command == "list":
            asyncio.run(_run_job_list(args))

        elif args.job_command == "wait":
            asyncio.run(_run_job_wait(args))

        elif args.job_command == "enable":
            asyncio.run(_run_job_enable(args))

        elif args.job_command == "disable":
            asyncio.run(_run_job_disable(args))

        else:
            subcommands["job"].print_help()

    elif args.command == "task":
        if args.task_command == "get":
            asyncio.run(_run_task_get(args))

        else:
            subcommands["task"].print_help()

    elif args.command == "register-job":
        asyncio.run(_run_register_job(args))

    elif args.command == "run-job":
        asyncio.run(_run_run_job(args))

    elif args.command == "registered-job":
        if args.registered_job_command == "list":
            asyncio.run(_run_registered_job_list(args))

        else:
            subcommands["registered-job"].print_help()

    elif args.command == "data":
        if args.data_command == "list":
            asyncio.run(_run_data_list(args))

        elif args.data_command == "get":
            asyncio.run(_run_data_get(args))

        elif args.data_command == "delete":
            asyncio.run(_run_data_delete(args))

        elif args.data_command == "purge":
            asyncio.run(_run_data_purge(args))

        else:
            subcommands["data"].print_help()

    elif args.command == "explain":
        asyncio.run(_run_explain(args))

    elif args.command == "debug":
        asyncio.run(_run_debug(args))

    elif args.command == "background":
        from aaiclick.orchestration.cli import start_background

        if args.background_command == "start":
            asyncio.run(start_background(poll_interval=args.poll_interval))

        else:
            subcommands["background"].print_help()

    elif args.command == "docker":
        if args.docker_command == "init":
            _run_docker_init(args)
        else:
            subcommands["docker"].print_help()

    elif args.command == "compose":
        if args.compose_command == "init":
            _run_compose_init(args)
        else:
            subcommands["compose"].print_help()

    elif args.command == "k8s":
        if args.k8s_command == "init":
            _run_k8s_init(args)
        else:
            subcommands["k8s"].print_help()

    elif args.command == "tenant":
        if args.tenant_command == "create":
            asyncio.run(_run_tenant_create(args))
        elif args.tenant_command == "list":
            asyncio.run(_run_tenant_list(args))
        else:
            subcommands["tenant"].print_help()

    elif args.command == "member":
        if args.member_command in ("add", "set-role"):
            asyncio.run(_run_member_set(args))
        elif args.member_command == "remove":
            asyncio.run(_run_member_remove(args))
        else:
            subcommands["member"].print_help()

    elif args.command == "user":
        if args.user_command == "create":
            asyncio.run(_run_user_create(args))

        elif args.user_command == "list":
            asyncio.run(_run_user_list(args))

        elif args.user_command == "set-superadmin":
            asyncio.run(_run_user_set_superadmin(args))

        elif args.user_command == "disable":
            asyncio.run(_run_user_disable(args))

        elif args.user_command == "passwd":
            asyncio.run(_run_user_passwd(args))

        else:
            subcommands["user"].print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
