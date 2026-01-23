"""CLI entry point for aaiclick package.

Usage:
    python -m aaiclick migrate        # Run database migrations
    python -m aaiclick migrate --help # Show migration help
"""

import sys


def main():
    """Main CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python -m aaiclick <command>")
        print("\nAvailable commands:")
        print("  migrate    Run database migrations")
        sys.exit(1)

    command = sys.argv[1]

    if command == "migrate":
        from aaiclick.orchestration.migrate import run_migrations

        # Pass remaining args to migration command
        run_migrations(sys.argv[2:])
    else:
        print(f"Unknown command: {command}")
        print("\nAvailable commands:")
        print("  migrate    Run database migrations")
        sys.exit(1)


if __name__ == "__main__":
    main()
