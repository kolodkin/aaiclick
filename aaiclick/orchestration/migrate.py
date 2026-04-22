"""Alembic configuration helper for programmatic migrations.

The CLI surface lives in ``aaiclick.internal_api.setup.migrate``; this module
exposes only the alembic ``Config`` builder shared between the CLI and tests.
"""

from pathlib import Path

from alembic.config import Config


def get_alembic_config() -> Config:
    """Create Alembic configuration for programmatic execution."""
    orchestration_dir = Path(__file__).parent
    alembic_ini = orchestration_dir / "alembic.ini"

    if not alembic_ini.exists():
        raise FileNotFoundError(
            f"alembic.ini not found at {alembic_ini}. Ensure aaiclick is installed correctly with migration files."
        )

    config = Config(str(alembic_ini))
    migrations_dir = orchestration_dir / "migrations"
    config.set_main_option("script_location", str(migrations_dir))
    return config
