"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick setup                    # Initialize local dev environment
    python -m aaiclick setup --ai               # Also pull the configured Ollama model
    python -m aaiclick migrate                  # Run database migrations
    python -m aaiclick migrate --help           # Show migration help
    python -m aaiclick worker start             # Start a worker process
    python -m aaiclick worker list              # List workers
    python -m aaiclick job get <ref>            # Get job details (by ID or name)
    python -m aaiclick job stats <ref>          # Show job execution stats
    python -m aaiclick job cancel <ref>         # Cancel a job
    python -m aaiclick job list                 # List jobs
    python -m aaiclick job enable <name>        # Enable a registered job
    python -m aaiclick job disable <name>       # Disable a registered job
    python -m aaiclick register-job <entrypoint> # Register a job
    python -m aaiclick run-job <name>           # Run a job immediately
    python -m aaiclick registered-job list      # List registered jobs
    python -m aaiclick data list                # List persistent objects
    python -m aaiclick data get <name>          # Show persistent object details
    python -m aaiclick data delete <name>       # Delete persistent object
    python -m aaiclick background start         # Start background cleanup worker
"""

import argparse
import asyncio
import json
import os
import urllib.error
import urllib.request

from aaiclick.data.object.cli import (
    delete_object_cmd,
    delete_objects_cmd,
    list_objects_cmd,
    show_object_cmd,
)


def _setup_ollama_model(model: str) -> None:
    """Pull an Ollama model via the Ollama HTTP API."""
    # model is like "ollama/llama3.2:3b" — strip the provider prefix
    model_name = model.removeprefix("ollama/")
    base_url = "http://localhost:11434"

    print(f"\nAI model: {model}")

    # Check if Ollama server is reachable
    try:
        urllib.request.urlopen(base_url, timeout=2)  # noqa: S310
        print("  ollama server: running")
    except (urllib.error.URLError, OSError):
        print("  ollama server: NOT RUNNING")
        print("  Start with:    ollama serve &")
        print("  Or install:    curl -fsSL https://ollama.com/install.sh | sh")
        return

    # Check if model is already present via HTTP API
    req = urllib.request.Request(  # noqa: S310
        f"{base_url}/api/show",
        data=json.dumps({"model": model_name}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)  # noqa: S310
        print(f"  model '{model_name}': already downloaded")
        return
    except urllib.error.HTTPError as e:
        if e.code != 404:
            raise

    # Pull via HTTP API (stream=false waits for completion)
    print(f"  Pulling '{model_name}' (this may take a few minutes)...")
    pull_req = urllib.request.Request(  # noqa: S310
        f"{base_url}/api/pull",
        data=json.dumps({"model": model_name, "stream": False}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(pull_req, timeout=600) as resp:  # noqa: S310
            result = json.loads(resp.read())
        if result.get("status") == "success":
            print(f"  model '{model_name}': OK")
        else:
            print(f"  model '{model_name}': unexpected response: {result}")
    except (urllib.error.URLError, OSError) as e:
        print(f"  model '{model_name}': pull failed: {e}")


def _run_setup(ai: bool = False):
    """Initialize local dev environment."""
    from pathlib import Path

    from aaiclick.backend import get_ch_url, get_sql_url, is_chdb, is_sqlite

    print(f"CH URL:  {get_ch_url()}")
    print(f"SQL URL: {get_sql_url()}")

    if is_chdb():
        from chdb.session import Session

        from aaiclick.data.data_context.chdb_client import get_chdb_data_path

        chdb_path = get_chdb_data_path()
        Path(chdb_path).mkdir(parents=True, exist_ok=True)
        sess = Session(chdb_path)
        sess.query("SELECT 1")
        sess.cleanup()
        print(f"  chdb: OK ({chdb_path})")
    else:
        print("  ClickHouse: remote server — requires pip install aaiclick[distributed]")

    if is_sqlite():
        from sqlalchemy import create_engine

        from aaiclick.orchestration.env import get_db_url
        from aaiclick.orchestration.models import SQLModel

        db_url = get_db_url()
        sync_url = db_url.replace("sqlite+aiosqlite", "sqlite")
        engine = create_engine(sync_url)
        SQLModel.metadata.create_all(engine)
        engine.dispose()
        print(f"  SQLite DB: OK ({db_url})")
    else:
        print("  PostgreSQL: requires pip install aaiclick[distributed]")
        print("  Run migrations: python -m aaiclick migrate upgrade head")

    if ai:
        model = os.environ.get("AAICLICK_AI_MODEL", "ollama/llama3.1:8b")
        if model.startswith("ollama/"):
            _setup_ollama_model(model)
        else:
            print(f"\nAI model: {model} (not an Ollama model — nothing to pull)")

    print("Setup complete.")


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
    worker_start_parser.add_argument(
        "--mp",
        action="store_true",
        default=False,
        help="Run each task in a dedicated child process (multiprocessing mode)",
    )

    # worker list
    worker_subparsers.add_parser(
        "list",
        help="List workers",
    )

    # worker stop
    worker_stop_parser = worker_subparsers.add_parser(
        "stop",
        help="Request a worker to stop gracefully",
    )
    worker_stop_parser.add_argument(
        "worker_id",
        help="Worker ID to stop",
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

    # job enable <name>
    job_enable_parser = job_subparsers.add_parser(
        "enable",
        help="Enable a registered job",
    )
    job_enable_parser.add_argument("name", type=str, help="Registered job name")

    # job disable <name>
    job_disable_parser = job_subparsers.add_parser(
        "disable",
        help="Disable a registered job",
    )
    job_disable_parser.add_argument("name", type=str, help="Registered job name")

    # Add register-job subcommand
    register_job_parser = subparsers.add_parser(
        "register-job",
        help="Register a job in the catalog",
    )
    register_job_parser.add_argument("entrypoint", type=str, help="Python dotted path (e.g. myapp.pipelines.etl_job)")
    register_job_parser.add_argument("--name", default=None, help="Job name (default: last segment of entrypoint)")
    register_job_parser.add_argument("--schedule", default=None, help="Cron expression (e.g. '0 8 * * *')")
    register_job_parser.add_argument("--kwargs", default=None, help="Default kwargs as JSON string")

    # Add run-job subcommand
    run_job_parser = subparsers.add_parser(
        "run-job",
        help="Run a job immediately (auto-registers if needed)",
    )
    run_job_parser.add_argument("name", type=str, help="Job name or entrypoint")
    run_job_parser.add_argument("--kwargs", default=None, help="Override kwargs as JSON string")

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
    registered_job_subparsers.add_parser(
        "list",
        help="List registered jobs",
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
        help="Delete a single persistent object",
    )
    data_delete_parser.add_argument("name", type=str, help="Persistent object name")

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
        _run_setup(ai=args.ai)

    elif args.command == "migrate":
        from aaiclick.orchestration.migrate import run_migrations

        run_migrations(args.args if hasattr(args, "args") else [])

    elif args.command == "worker":
        from aaiclick.orchestration.cli import show_workers, start_worker, stop_worker_cmd

        if args.worker_command == "start":
            asyncio.run(start_worker(max_tasks=args.max_tasks, multiprocessing=args.mp))

        elif args.worker_command == "list":
            asyncio.run(show_workers())

        elif args.worker_command == "stop":
            asyncio.run(stop_worker_cmd(args.worker_id))

        else:
            worker_parser.print_help()

    elif args.command == "job":
        from aaiclick.orchestration.cli import cancel_job_cmd, show_job, show_job_stats

        if args.job_command == "get":
            asyncio.run(show_job(args.ref))

        elif args.job_command == "stats":
            asyncio.run(show_job_stats(args.ref))

        elif args.job_command == "cancel":
            asyncio.run(cancel_job_cmd(args.ref))

        elif args.job_command == "list":
            from aaiclick.orchestration.cli import show_jobs

            asyncio.run(show_jobs(
                status=args.status,
                name_like=args.like,
                limit=args.limit,
                offset=args.offset,
            ))

        elif args.job_command == "enable":
            from aaiclick.orchestration.cli import enable_job_cmd

            asyncio.run(enable_job_cmd(args.name))

        elif args.job_command == "disable":
            from aaiclick.orchestration.cli import disable_job_cmd

            asyncio.run(disable_job_cmd(args.name))

        else:
            job_parser.print_help()

    elif args.command == "register-job":
        from aaiclick.orchestration.cli import register_job_cmd

        asyncio.run(register_job_cmd(
            args.entrypoint,
            name=args.name,
            schedule=args.schedule,
            kwargs_json=args.kwargs,
        ))

    elif args.command == "run-job":
        from aaiclick.orchestration.cli import run_job_cmd

        asyncio.run(run_job_cmd(args.name, kwargs_json=args.kwargs))

    elif args.command == "registered-job":
        from aaiclick.orchestration.cli import show_registered_jobs

        if args.registered_job_command == "list":
            asyncio.run(show_registered_jobs())

        else:
            registered_job_parser.print_help()

    elif args.command == "data":
        if args.data_command == "list":
            asyncio.run(list_objects_cmd())

        elif args.data_command == "get":
            asyncio.run(show_object_cmd(args.name))

        elif args.data_command == "delete":
            asyncio.run(delete_object_cmd(args.name))

        elif args.data_command == "purge":
            asyncio.run(delete_objects_cmd(
                after=args.after,
                before=args.before,
            ))

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
