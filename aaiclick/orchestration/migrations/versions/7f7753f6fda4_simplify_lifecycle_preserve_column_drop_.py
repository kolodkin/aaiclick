"""simplify lifecycle preserve column drop run_refs and context_refs add task_name_locks

Revision ID: 7f7753f6fda4
Revises: 161cfe0f1117
Create Date: 2026-04-28 06:39:25.105739

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7f7753f6fda4"
down_revision: str | Sequence[str] | None = "161cfe0f1117"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    # 1. Add `preserve` JSON column to `jobs` and `registered_jobs`
    op.add_column("jobs", sa.Column("preserve", sa.JSON(), nullable=True))
    op.add_column("registered_jobs", sa.Column("preserve", sa.JSON(), nullable=True))

    # 2. Backfill preserve from preservation_mode where the column exists.
    #    NONE -> NULL (default already), FULL -> '"*"'.
    op.execute("UPDATE jobs SET preserve = '\"*\"' WHERE preservation_mode = 'FULL'")
    op.execute("UPDATE registered_jobs SET preserve = '\"*\"' WHERE preservation_mode = 'FULL'")

    # 3. Drop preservation_mode columns and the Postgres enum type
    op.drop_column("jobs", "preservation_mode")
    op.drop_column("registered_jobs", "preservation_mode")
    sa.Enum(name="preservationmode").drop(op.get_bind(), checkfirst=True)

    # 4. Drop obsolete tables (their indexes drop with them)
    op.drop_table("table_run_refs")
    op.drop_table("table_context_refs")

    # 5. Trim `table_registry`: drop `run_id`, add `preserved`.
    #    Leave `schema_doc` alone — owned by migration 161cfe0f1117.
    op.drop_column("table_registry", "run_id")
    op.add_column(
        "table_registry",
        sa.Column("preserved", sa.Boolean(), nullable=False, server_default=sa.false()),
    )

    # 6. Create `task_name_locks`
    op.create_table(
        "task_name_locks",
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("task_id", sa.BigInteger(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("job_id", "name"),
    )
    op.create_index(
        "ix_task_name_locks_task_id",
        "task_name_locks",
        ["task_id"],
    )


def downgrade() -> None:
    """Downgrade schema."""
    # 6. Drop task_name_locks
    op.drop_index("ix_task_name_locks_task_id", table_name="task_name_locks")
    op.drop_table("task_name_locks")

    # 5. Restore table_registry: drop `preserved`, add `run_id`.
    #    Leave `schema_doc` alone — owned by migration 161cfe0f1117.
    op.drop_column("table_registry", "preserved")
    op.add_column("table_registry", sa.Column("run_id", sa.BigInteger(), nullable=True))

    # 4. Recreate table_run_refs and table_context_refs (empty — historical
    #    data is unrecoverable; the goal is to allow ORM imports against the
    #    prior schema, not to round-trip data).
    op.create_table(
        "table_run_refs",
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("table_name", "run_id"),
    )
    op.create_index("ix_table_run_refs_run_id", "table_run_refs", ["run_id"])
    op.create_table(
        "table_context_refs",
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("context_id", sa.BigInteger(), nullable=False),
        sa.Column("advisory_id", sa.BigInteger(), nullable=True),
        sa.Column("job_id", sa.BigInteger(), nullable=True),
        sa.PrimaryKeyConstraint("table_name", "context_id"),
    )
    op.create_index(
        "ix_table_context_refs_context_id",
        "table_context_refs",
        ["context_id"],
    )

    # 3. Restore preservation_mode columns and the Postgres enum type
    preservation_mode = sa.Enum("NONE", "FULL", name="preservationmode")
    preservation_mode.create(op.get_bind(), checkfirst=True)
    op.add_column(
        "jobs",
        sa.Column("preservation_mode", preservation_mode, nullable=False, server_default="NONE"),
    )
    op.add_column(
        "registered_jobs",
        sa.Column("preservation_mode", preservation_mode, nullable=False, server_default="NONE"),
    )

    # 2. Backfill preservation_mode from preserve. Only "*" round-trips to FULL;
    #    list values cannot be expressed and trigger an explicit failure.
    op.execute("UPDATE jobs SET preservation_mode = 'FULL' WHERE preserve = '\"*\"'")
    op.execute("UPDATE registered_jobs SET preservation_mode = 'FULL' WHERE preserve = '\"*\"'")

    bind = op.get_bind()
    bad_jobs = bind.execute(
        sa.text("SELECT id, preserve FROM jobs WHERE preserve IS NOT NULL AND preserve <> '\"*\"'")
    ).fetchall()
    if bad_jobs:
        raise RuntimeError(
            f"Cannot downgrade: jobs have list-shaped preserve values that don't map to PreservationMode: {bad_jobs!r}"
        )
    bad_reg = bind.execute(
        sa.text("SELECT id, preserve FROM registered_jobs WHERE preserve IS NOT NULL AND preserve <> '\"*\"'")
    ).fetchall()
    if bad_reg:
        raise RuntimeError(f"Cannot downgrade: registered_jobs have list-shaped preserve values: {bad_reg!r}")

    # 1. Drop preserve columns
    op.drop_column("jobs", "preserve")
    op.drop_column("registered_jobs", "preserve")
