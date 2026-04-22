"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick setup                    # Initialize local dev environment
    python -m aaiclick setup --ai               # Also pull the configured Ollama model
    python -m aaiclick migrate                  # Run database migrations
    python -m aaiclick migrate --help           # Show migration help
    python -m aaiclick local start              # Start worker + background (local mode)
    python -m aaiclick local stop <worker_id>   # Stop a local worker
    python -m aaiclick worker start             # Start a distributed worker process
    python -m aaiclick worker list              # List workers
    python -m aaiclick worker stop <worker_id>  # Stop a worker gracefully
    python -m aaiclick background start         # Start background cleanup worker
    python -m aaiclick job get <ref>            # Get job details (by ID or name)
    python -m aaiclick job stats <ref>          # Show job execution stats
    python -m aaiclick job cancel <ref>         # Cancel a job
    python -m aaiclick job list                 # List jobs
    python -m aaiclick job enable <name>        # Enable a registered job
    python -m aaiclick job disable <name>       # Disable a registered job
    python -m aaiclick task get <id>            # Get task details by ID
    python -m aaiclick register-job <entrypoint> # Register a job
    python -m aaiclick run-job <name>           # Run a job immediately
    python -m aaiclick registered-job list      # List registered jobs
    python -m aaiclick data list                # List persistent objects
    python -m aaiclick data get <name>          # Show persistent object details
    python -m aaiclick data delete <name>       # Delete persistent object
    python -m aaiclick data purge --after ISO   # Delete persistent objects by time
"""

import argparse
import asyncio
import json
import sys
from datetime import datetime

from aaiclick import cli_renderers, internal_api
from aaiclick.data.data_context import data_context
from aaiclick.internal_api.errors import InternalApiError
from aaiclick.orchestration.models import JobStatus, PreservationMode, WorkerStatus
from aaiclick.orchestration.orch_context import orch_context
from aaiclick.view_models import (
    JobListFilter,
    MigrationAction,
    ObjectFilter,
    PurgeObjectsRequest,
    RegisteredJobFilter,
    RegisterJobRequest,
    RunJobRequest,
    WorkerFilter,
)

_JSON_HELP = "Emit JSON instead of a table"


def _add_json_flag(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help=_JSON_HELP)


def _print_json(model) -> None:
    """Dump a pydantic view model as JSON on stdout."""
    print(model.model_dump_json())


def _render(args: argparse.Namespace, view, text_renderer) -> None:
    """Pick JSON or text rendering based on ``--json``."""
    if args.json:
        _print_json(view)
    else:
        text_renderer(view)


async def _run_internal_api(coro):
    """Run ``coro`` inside ``orch_context(with_ch=False)``, mapping API errors to exit 1."""
    try:
        async with orch_context(with_ch=False):
            return await coro
    except InternalApiError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


async def _run_data_api(coro):
    """Run ``coro`` inside ``data_context()``, mapping API errors to exit 1."""
    try:
        async with data_context():
            return await coro
    except InternalApiError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)


async def _run_job_list(args: argparse.Namespace) -> None:
    filter = JobListFilter(
        status=JobStatus(args.status) if args.status else None,
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
    return PreservationMode(value) if value else None


async def _run_run_job(args: argparse.Namespace) -> None:
    kwargs: dict = json.loads(args.kwargs) if args.kwargs else {}
    request = RunJobRequest(
        name=args.name,
        kwargs=kwargs,
        preservation_mode=_parse_preservation_mode(args.preservation_mode),
    )
    view = await _run_internal_api(internal_api.run_job(request))
    _render(args, view, cli_renderers.render_job_created)


async def _run_register_job(args: argparse.Namespace) -> None:
    default_kwargs: dict | None = json.loads(args.kwargs) if args.kwargs else None
    request = RegisterJobRequest(
        name=args.name or "",
        entrypoint=args.entrypoint,
        schedule=args.schedule,
        default_kwargs=default_kwargs,
        preservation_mode=_parse_preservation_mode(args.preservation_mode),
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
    _render(args, page, cli_renderers.render_registered_jobs_page)


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


async def _run_worker_list(args: argparse.Namespace) -> None:
    filter = WorkerFilter(
        status=WorkerStatus(args.status) if args.status else None,
        limit=args.limit,
        offset=args.offset,
    )
    page = await _run_internal_api(internal_api.list_workers(filter))
    _render(args, page, lambda p: cli_renderers.render_workers_page(p, offset=args.offset))


async def _run_worker_stop(args: argparse.Namespace) -> None:
    view = await _run_internal_api(internal_api.stop_worker(args.worker_id))
    _render(args, view, cli_renderers.render_worker_stopped)


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


def _run_setup_cli(args: argparse.Namespace) -> None:
    try:
        result = internal_api.setup.setup(ai=args.ai)
    except InternalApiError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)
    _render(args, result, cli_renderers.render_setup_result)


def _run_migrate_cli(args: argparse.Namespace) -> None:
    raw_args = list(args.args) if hasattr(args, "args") else []
    if not raw_args or raw_args[0] in ("-h", "--help"):
        print(_MIGRATE_HELP)
        return

    action_name, *rest = raw_args
    try:
        action = MigrationAction(action_name)
    except ValueError:
        print(f"Unknown command: {action_name}", file=sys.stderr)
        print("Run 'python -m aaiclick migrate --help' for usage", file=sys.stderr)
        sys.exit(1)
    revision = rest[0] if rest else None

    try:
        result = internal_api.setup.migrate(action, revision)
    except InternalApiError as exc:
        print(exc, file=sys.stderr)
        sys.exit(1)

    _render(args, result, cli_renderers.render_migration_result)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="aaiclick",
        description="aaiclick command-line interface",
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
        help="Start worker + background in a single process",
    )
    local_start_parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Maximum tasks to execute (default: unlimited)",
    )

    # local stop
    local_stop_parser = local_subparsers.add_parser(
        "stop",
        help="Request a local worker to stop gracefully",
    )
    local_stop_parser.add_argument(
        "worker_id",
        type=int,
        help="Worker ID to stop",
    )
    _add_json_flag(local_stop_parser)

    # Add worker subcommand (distributed mode)
    worker_parser = subparsers.add_parser(
        "worker",
        help="Distributed worker management commands",
    )
    worker_subparsers = worker_parser.add_subparsers(
        dest="worker_command",
        help="Worker commands",
    )

    # worker start
    worker_start_parser = worker_subparsers.add_parser(
        "start",
        help="Start a distributed worker process",
    )
    worker_start_parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Maximum tasks to execute (default: unlimited)",
    )

    # worker list
    worker_list_parser = worker_subparsers.add_parser(
        "list",
        help="List workers",
    )
    worker_list_parser.add_argument(
        "--status",
        choices=[s.value for s in WorkerStatus],
        default=None,
        help="Filter by status",
    )
    worker_list_parser.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Maximum results (default: 50)",
    )
    worker_list_parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Skip N results (default: 0)",
    )
    _add_json_flag(worker_list_parser)

    # worker stop
    worker_stop_parser = worker_subparsers.add_parser(
        "stop",
        help="Request a worker to stop gracefully",
    )
    worker_stop_parser.add_argument(
        "worker_id",
        type=int,
        help="Worker ID to stop",
    )
    _add_json_flag(worker_stop_parser)

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
        choices=[s.value for s in JobStatus],
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
    register_job_parser.add_argument(
        "--preservation-mode",
        choices=[m.value for m in PreservationMode],
        default=None,
        help="Default preservation mode for every run of this job (runs can override)",
    )
    _add_json_flag(register_job_parser)

    # Add run-job subcommand
    run_job_parser = subparsers.add_parser(
        "run-job",
        help="Run a job immediately (auto-registers if needed)",
    )
    run_job_parser.add_argument("name", type=str, help="Job name or entrypoint")
    run_job_parser.add_argument("--kwargs", default=None, help="Override kwargs as JSON string")
    run_job_parser.add_argument(
        "--preservation-mode",
        choices=[m.value for m in PreservationMode],
        default=None,
        help="Table preservation mode (default: AAICLICK_DEFAULT_PRESERVATION_MODE or NONE)",
    )
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

    args = parser.parse_args()

    if args.command == "setup":
        _run_setup_cli(args)

    elif args.command == "migrate":
        _run_migrate_cli(args)

    elif args.command == "local":
        if args.local_command == "start":
            from aaiclick.orchestration.cli import start_local

            asyncio.run(start_local(max_tasks=args.max_tasks))

        elif args.local_command == "stop":
            asyncio.run(_run_worker_stop(args))

        else:
            local_parser.print_help()

    elif args.command == "worker":
        if args.worker_command == "start":
            from aaiclick.orchestration.cli import start_worker

            asyncio.run(start_worker(max_tasks=args.max_tasks))

        elif args.worker_command == "list":
            asyncio.run(_run_worker_list(args))

        elif args.worker_command == "stop":
            asyncio.run(_run_worker_stop(args))

        else:
            worker_parser.print_help()

    elif args.command == "job":
        if args.job_command == "get":
            asyncio.run(_run_job_get(args))

        elif args.job_command == "stats":
            asyncio.run(_run_job_stats(args))

        elif args.job_command == "cancel":
            asyncio.run(_run_job_cancel(args))

        elif args.job_command == "list":
            asyncio.run(_run_job_list(args))

        elif args.job_command == "enable":
            asyncio.run(_run_job_enable(args))

        elif args.job_command == "disable":
            asyncio.run(_run_job_disable(args))

        else:
            job_parser.print_help()

    elif args.command == "task":
        if args.task_command == "get":
            asyncio.run(_run_task_get(args))

        else:
            task_parser.print_help()

    elif args.command == "register-job":
        asyncio.run(_run_register_job(args))

    elif args.command == "run-job":
        asyncio.run(_run_run_job(args))

    elif args.command == "registered-job":
        if args.registered_job_command == "list":
            asyncio.run(_run_registered_job_list(args))

        else:
            registered_job_parser.print_help()

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
            data_parser.print_help()

    elif args.command == "background":
        from aaiclick.orchestration.cli import start_background

        if args.background_command == "start":
            asyncio.run(start_background(poll_interval=args.poll_interval))

        else:
            background_parser.print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
