import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, inspect, pool
from sqlmodel import SQLModel

# Import models module to ensure all models are registered with SQLModel metadata
import aaiclick.auth.models  # noqa: F401  # register users/refresh_tokens with SQLModel.metadata
import aaiclick.orchestration.models  # noqa: F401

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Set SQLModel metadata as the target for autogenerate
target_metadata = SQLModel.metadata


def _make_include_name(connection):
    """Build an ``include_name`` filter that skips reflected FK constraints
    whose referred table is gone from the model metadata.

    When one revision drops both a table and the FK column referencing it
    (e.g. build_tasks + tasks.build_task_id), autogenerate crashes resolving
    the reflected FK (``NoReferencedTableError``). Name filters run before
    that resolution (``include_object`` runs after it, too late). Postgres
    drops the constraint together with the column, so skipping the
    comparison loses nothing."""
    model_tables = set(target_metadata.tables)
    dangling_fk_names: set[str] = set()
    inspector = inspect(connection)
    for table_name in inspector.get_table_names():
        for fk in inspector.get_foreign_keys(table_name):
            if fk.get("referred_table") not in model_tables and fk.get("name"):
                dangling_fk_names.add(fk["name"])

    def include_name(name, type_, parent_names) -> bool:
        if type_ == "foreign_key_constraint" and name in dangling_fk_names:
            return False
        return True

    return include_name


def get_url() -> str:
    """
    Get sync PostgreSQL connection URL for Alembic.

    Reads AAICLICK_SQL_URL first (stripping async driver prefix),
    falls back to individual POSTGRES_* env vars.
    """
    sql_url = os.getenv("AAICLICK_SQL_URL")
    if not sql_url or sql_url.startswith("postgresql"):
        try:
            import psycopg2  # noqa: F401
        except ImportError as e:
            raise ImportError(
                "PostgreSQL migrations require the aaiclick[distributed] extra. "
                "Install with: pip install aaiclick[distributed]"
            ) from e
    if sql_url:
        return sql_url.replace("postgresql+asyncpg://", "postgresql://")

    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "aaiclick")
    password = os.getenv("POSTGRES_PASSWORD", "secret")
    database = os.getenv("POSTGRES_DB", "aaiclick")

    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    configuration = config.get_section(config.config_ini_section, {})
    configuration["sqlalchemy.url"] = get_url()

    connectable = engine_from_config(
        configuration,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # The dangling-FK filter only matters for autogenerate comparison — a
    # plain upgrade never consults include_name, so skip the reflection pass
    # there. Inspect on a dedicated connection: reflection implicitly begins
    # a transaction, and sharing the migration connection would leave
    # alembic's begin_transaction() without ownership — the upgrade's DDL
    # would roll back when the connection closes.
    include_name = None
    if getattr(config.cmd_opts, "autogenerate", False):
        with connectable.connect() as inspect_connection:
            include_name = _make_include_name(inspect_connection)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_name=include_name,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
