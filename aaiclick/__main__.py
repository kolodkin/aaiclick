"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick migrate            # Run database migrations
    python -m aaiclick migrate --help     # Show migration help
    python -m aaiclick worker start       # Start a worker process
    python -m aaiclick worker list        # List workers
    python -m aaiclick job get <ref>      # Get job details (by ID or name)
    python -m aaiclick job stats <ref>    # Show job execution stats
    python -m aaiclick job cancel <ref>   # Cancel a job
    python -m aaiclick job list           # List jobs
    python -m aaiclick data list          # List persistent objects
    python -m aaiclick data get <name>    # Show persistent object details
    python -m aaiclick data delete <name> # Delete persistent object
    python -m aaiclick background start   # Start background cleanup worker
"""

import argparse
import asyncio

from aaiclick.data.cli import (
    delete_object_cmd,
    list_objects_cmd,
    show_object_cmd,
)
from aaiclick.orchestration.cli import (
    cancel_job_cmd,
    show_job,
    show_job_stats,
    show_jobs,
    show_workers,
    start_background,
    start_worker,
)
from aaiclick.orchestration.migrate import run_migrations


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="aaiclick",
        description="aaiclick command-line interface",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

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

    # Add worker subcommand
    worker_parser = subparsers.add_parser(
        "worker",
        help="Worker management commands",
    )
    worker_subparsers = worker_parser.add_subparsers(
        dest="worker_command",
        help="Worker commands",
    )

    # worker start
    worker_start_parser = worker_subparsers.add_parser(
        "start",
        help="Start a worker process",
    )
    worker_start_parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Maximum tasks to execute (default: unlimited)",
    )

    # worker list
    worker_subparsers.add_parser(
        "list",
        help="List workers",
    )

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

    # job stats <ref>
    job_stats_parser = job_subparsers.add_parser(
        "stats",
        help="Show job execution stats",
    )
    job_stats_parser.add_argument("ref", type=str, help="Job ID or name")

    # job cancel <ref>
    job_cancel_parser = job_subparsers.add_parser(
        "cancel",
        help="Cancel a job and its non-terminal tasks",
    )
    job_cancel_parser.add_argument("ref", type=str, help="Job ID or name")

    # job list
    job_list_parser = job_subparsers.add_parser(
        "list",
        help="List jobs",
    )
    job_list_parser.add_argument(
        "--status",
        choices=["PENDING", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"],
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
    data_subparsers.add_parser(
        "list",
        help="List persistent objects",
    )

    # data get <name>
    data_get_parser = data_subparsers.add_parser(
        "get",
        help="Show persistent object details",
    )
    data_get_parser.add_argument("name", type=str, help="Persistent object name")

    # data delete <name>
    data_delete_parser = data_subparsers.add_parser(
        "delete",
        help="Delete persistent object",
    )
    data_delete_parser.add_argument("name", type=str, help="Persistent object name")
    data_delete_parser.add_argument(
        "--since",
        default=None,
        help="Delete rows created at or after this time (ISO 8601)",
    )
    data_delete_parser.add_argument(
        "--before",
        default=None,
        help="Delete rows created before this time (ISO 8601)",
    )

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

    if args.command == "migrate":
        run_migrations(args.args if hasattr(args, "args") else [])

    elif args.command == "worker":
        if args.worker_command == "start":
            asyncio.run(start_worker(max_tasks=args.max_tasks))

        elif args.worker_command == "list":
            asyncio.run(show_workers())

        else:
            worker_parser.print_help()

    elif args.command == "job":
        if args.job_command == "get":
            asyncio.run(show_job(args.ref))

        elif args.job_command == "stats":
            asyncio.run(show_job_stats(args.ref))

        elif args.job_command == "cancel":
            asyncio.run(cancel_job_cmd(args.ref))

        elif args.job_command == "list":
            asyncio.run(show_jobs(
                status=args.status,
                name_like=args.like,
                limit=args.limit,
                offset=args.offset,
            ))

        else:
            job_parser.print_help()

    elif args.command == "data":
        if args.data_command == "list":
            asyncio.run(list_objects_cmd())

        elif args.data_command == "get":
            asyncio.run(show_object_cmd(args.name))

        elif args.data_command == "delete":
            asyncio.run(delete_object_cmd(
                args.name,
                since=args.since,
                before=args.before,
            ))

        else:
            data_parser.print_help()

    elif args.command == "background":
        if args.background_command == "start":
            asyncio.run(start_background(poll_interval=args.poll_interval))

        else:
            background_parser.print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
