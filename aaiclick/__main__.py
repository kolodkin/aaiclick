"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick migrate         # Run database migrations
    python -m aaiclick migrate --help  # Show migration help
    python -m aaiclick worker start    # Start a worker process
    python -m aaiclick worker list     # List workers
"""

import argparse
import asyncio


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

    args = parser.parse_args()

    if args.command == "migrate":
        from aaiclick.orchestration.migrate import run_migrations

        # Pass remaining args to migration command
        run_migrations(args.args if hasattr(args, "args") else [])

    elif args.command == "worker":
        if args.worker_command == "start":
            from aaiclick.orchestration import OrchContext, worker_main_loop

            async def start_worker():
                async with OrchContext():
                    await worker_main_loop(max_tasks=args.max_tasks)

            asyncio.run(start_worker())

        elif args.worker_command == "list":
            from aaiclick.orchestration import OrchContext, list_workers

            async def show_workers():
                async with OrchContext():
                    workers = await list_workers()
                    if not workers:
                        print("No workers found")
                        return

                    print(f"{'ID':<20} {'Status':<10} {'Host':<20} {'PID':<8} {'Completed':<10} {'Failed':<8}")
                    print("-" * 80)
                    for w in workers:
                        print(f"{w.id:<20} {w.status.value:<10} {w.hostname:<20} {w.pid:<8} {w.tasks_completed:<10} {w.tasks_failed:<8}")

            asyncio.run(show_workers())

        else:
            worker_parser.print_help()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
