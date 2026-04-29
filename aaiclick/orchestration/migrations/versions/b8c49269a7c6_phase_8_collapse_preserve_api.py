"""phase 8: drop task_name_locks; preserve JSON -> preserve_all BOOLEAN

Revision ID: b8c49269a7c6
Revises: 7f7753f6fda4
Create Date: 2026-04-28 21:15:45.400951

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b8c49269a7c6"
down_revision: str | Sequence[str] | None = "7f7753f6fda4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Phase 8 collapse:

    1. Drop ``task_name_locks`` — naming a job-scoped table is itself the
       preservation signal, so the per-task lock is no longer needed.
    2. Replace ``jobs.preserve`` / ``registered_jobs.preserve`` (JSON,
       ``Literal["*"] | None``) with ``preserve_all`` (BOOLEAN). Existing
       ``"*"`` rows backfill to TRUE.
    """
    op.drop_index("ix_task_name_locks_task_id", table_name="task_name_locks")
    op.drop_table("task_name_locks")

    for table in ("jobs", "registered_jobs"):
        op.add_column(
            table,
            sa.Column("preserve_all", sa.Boolean(), nullable=False, server_default=sa.false()),
        )
        # The Phase 1 migration only ever wrote ``'"*"'`` or NULL into
        # ``preserve``; backfill IS NOT NULL → preserve_all = TRUE without
        # casting (PG's JSON `=` operator vs SQLite's TEXT diverge here).
        op.execute(f"UPDATE {table} SET preserve_all = TRUE WHERE preserve IS NOT NULL")
        op.drop_column(table, "preserve")


def downgrade() -> None:
    """Recreate the JSON column and the lock table empty."""
    for table in ("jobs", "registered_jobs"):
        op.add_column(table, sa.Column("preserve", sa.JSON(), nullable=True))
        # PG: JSON column accepts a JSON-encoded text literal via parameterized
        # bind; raw '"*"' would need a cast. Use a parameterized UPDATE so the
        # driver handles the JSON conversion uniformly on both backends.
        op.execute(sa.text(f"UPDATE {table} SET preserve = :v WHERE preserve_all = TRUE").bindparams(v='"*"'))
        op.drop_column(table, "preserve_all")

    op.create_table(
        "task_name_locks",
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("task_id", sa.BigInteger(), nullable=False),
        sa.Column("acquired_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("job_id", "name"),
    )
    op.create_index("ix_task_name_locks_task_id", "task_name_locks", ["task_id"])
