"""replace_refcount_with_run_ids

Revision ID: b7d3e2f19a4c
Revises: f3a8b1c42d5e
Create Date: 2026-04-08 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7d3e2f19a4c"
down_revision: str | Sequence[str] | None = "f3a8b1c42d5e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Replace integer refcount with table_run_refs junction table.

    table_run_refs tracks which run_ids hold references to which tables.
    table_context_refs keeps only pin state (job_id).
    """
    op.create_table(
        "table_run_refs",
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("run_id", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("table_name", "run_id"),
    )
    op.create_index("ix_table_run_refs_run_id", "table_run_refs", ["run_id"])
    op.drop_column("table_context_refs", "refcount")


def downgrade() -> None:
    """Restore integer refcount, drop table_run_refs."""
    op.add_column(
        "table_context_refs",
        sa.Column("refcount", sa.BigInteger(), nullable=False, server_default="0"),
    )
    op.drop_index("ix_table_run_refs_run_id", table_name="table_run_refs")
    op.drop_table("table_run_refs")
