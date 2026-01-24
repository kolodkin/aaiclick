"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick migrate        # Run database migrations
    python -m aaiclick migrate --help # Show migration help
"""

import argparse


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

    args = parser.parse_args()

    if args.command == "migrate":
        from aaiclick.orchestration.migrate import run_migrations

        # Pass remaining args to migration command
        run_migrations(args.args if hasattr(args, "args") else [])
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
