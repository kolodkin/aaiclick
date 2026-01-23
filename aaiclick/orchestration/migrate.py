"""Programmatic database migration runner using Alembic."""

import os
import sys
from pathlib import Path

from alembic import command
from alembic.config import Config


def get_alembic_config() -> Config:
    """Create Alembic configuration for programmatic execution.

    Returns:
        Alembic Config object configured for orchestration migrations
    """
    # Get the orchestration package directory (where alembic.ini lives)
    orchestration_dir = Path(__file__).parent
    alembic_ini = orchestration_dir / "alembic.ini"

    if not alembic_ini.exists():
        raise FileNotFoundError(
            f"alembic.ini not found at {alembic_ini}. "
            "Ensure aaiclick is installed correctly with migration files."
        )

    # Create Alembic config
    config = Config(str(alembic_ini))

    # Set script location to migrations directory
    migrations_dir = orchestration_dir / "migrations"
    config.set_main_option("script_location", str(migrations_dir))

    return config


def run_migrations(args: list[str] = None):
    """Run database migrations programmatically.

    Args:
        args: Command-line arguments (default: upgrade to head)

    Examples:
        run_migrations()              # Upgrade to latest
        run_migrations(["upgrade", "head"])
        run_migrations(["downgrade", "-1"])
        run_migrations(["current"])
    """
    args = args or []

    # Parse command
    if not args or args[0] in ["-h", "--help"]:
        print("Database Migration Commands")
        print("=" * 50)
        print()
        print("Usage: python -m aaiclick migrate [command] [options]")
        print()
        print("Commands:")
        print("  upgrade [revision]   Upgrade to a later version (default: head)")
        print("  downgrade [revision] Revert to a previous version")
        print("  current              Display current revision")
        print("  history              List migration history")
        print("  heads                Show current available heads")
        print("  show [revision]      Show details about a revision")
        print()
        print("Examples:")
        print("  python -m aaiclick migrate                 # Upgrade to latest")
        print("  python -m aaiclick migrate upgrade head    # Upgrade to latest")
        print("  python -m aaiclick migrate downgrade -1    # Downgrade one revision")
        print("  python -m aaiclick migrate current         # Show current revision")
        print("  python -m aaiclick migrate history         # Show migration history")
        print()
        print("Environment Variables:")
        print("  POSTGRES_HOST       PostgreSQL host (default: localhost)")
        print("  POSTGRES_PORT       PostgreSQL port (default: 5432)")
        print("  POSTGRES_USER       PostgreSQL user (default: aaiclick)")
        print("  POSTGRES_PASSWORD   PostgreSQL password (default: secret)")
        print("  POSTGRES_DB         PostgreSQL database (default: aaiclick)")
        return

    cmd = args[0]
    cmd_args = args[1:] if len(args) > 1 else []

    try:
        config = get_alembic_config()

        if cmd == "upgrade":
            revision = cmd_args[0] if cmd_args else "head"
            print(f"Upgrading database to revision: {revision}")
            command.upgrade(config, revision)
            print("✓ Database upgraded successfully")

        elif cmd == "downgrade":
            if not cmd_args:
                print("Error: downgrade requires a revision argument")
                print("Example: python -m aaiclick migrate downgrade -1")
                sys.exit(1)
            revision = cmd_args[0]
            print(f"Downgrading database to revision: {revision}")
            command.downgrade(config, revision)
            print("✓ Database downgraded successfully")

        elif cmd == "current":
            print("Current database revision:")
            command.current(config, verbose=True)

        elif cmd == "history":
            print("Migration history:")
            command.history(config, verbose=True)

        elif cmd == "heads":
            print("Available heads:")
            command.heads(config, verbose=True)

        elif cmd == "show":
            if not cmd_args:
                print("Error: show requires a revision argument")
                sys.exit(1)
            revision = cmd_args[0]
            command.show(config, revision)

        else:
            print(f"Unknown command: {cmd}")
            print("Run 'python -m aaiclick migrate --help' for usage")
            sys.exit(1)

    except Exception as e:
        print(f"Error running migration: {e}", file=sys.stderr)
        sys.exit(1)
