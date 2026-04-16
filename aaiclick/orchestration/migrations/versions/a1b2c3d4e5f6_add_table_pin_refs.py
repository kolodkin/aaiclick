"""add_table_pin_refs

Revision ID: a1b2c3d4e5f6
Revises: b7d3e2f19a4c
Create Date: 2026-04-08 19:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "b7d3e2f19a4c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create table_pin_refs and drop job_id from table_context_refs.

    Pin state moves from a nullable column on table_context_refs to a
    dedicated junction table keyed by (table_name, task_id).
    """
    op.create_table(
        "table_pin_refs",
        sa.Column("table_name", sa.String, primary_key=True),
        sa.Column("task_id", sa.BigInteger, primary_key=True),
    )
    op.drop_index("ix_table_context_refs_job_id", table_name="table_context_refs")
    op.drop_column("table_context_refs", "job_id")


def downgrade() -> None:
    """Restore job_id on table_context_refs and drop table_pin_refs."""
    op.add_column(
        "table_context_refs",
        sa.Column("job_id", sa.BigInteger, nullable=True),
    )
    op.create_index(
        "ix_table_context_refs_job_id",
        "table_context_refs",
        ["job_id"],
    )
    op.drop_table("table_pin_refs")
