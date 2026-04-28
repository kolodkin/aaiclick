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
    # 1. Add `preserve` JSON column to `jobs` and `registered_jobs`.
    #    The legacy `preservation_mode` column and its Postgres enum stay —
    #    Phase 6 drops both alongside the ORM field removal.
    op.add_column("jobs", sa.Column("preserve", sa.JSON(), nullable=True))
    op.add_column("registered_jobs", sa.Column("preserve", sa.JSON(), nullable=True))

    # 2. Backfill preserve from preservation_mode.
    #    NONE -> NULL (default already), FULL -> '"*"'.
    op.execute("UPDATE jobs SET preserve = '\"*\"' WHERE preservation_mode = 'FULL'")
    op.execute("UPDATE registered_jobs SET preserve = '\"*\"' WHERE preservation_mode = 'FULL'")

    # 3. Create `task_name_locks`.
    #    Phase 6 drops table_run_refs / table_context_refs alongside the
    #    SQLModel deletions in db_lifecycle.py and the locks.py /
    #    background/handler.py call sites that still reference them.
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
    # 3. Drop task_name_locks
    op.drop_index("ix_task_name_locks_task_id", table_name="task_name_locks")
    op.drop_table("task_name_locks")

    # 1. Drop preserve columns. preservation_mode was never dropped in
    #    upgrade, so nothing to restore.
    op.drop_column("jobs", "preserve")
    op.drop_column("registered_jobs", "preserve")
