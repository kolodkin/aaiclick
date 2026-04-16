"""add_job_id_to_table_context_refs

Revision ID: e1f4a7c29b03
Revises: 2bde1ead8ddf
Create Date: 2026-04-07 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e1f4a7c29b03"
down_revision: str | Sequence[str] | None = "886b7b988f00"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add nullable job_id column to table_context_refs.

    Non-NULL job_id marks pin refs; NULL marks plain task refs.
    """
    op.add_column(
        "table_context_refs",
        sa.Column("job_id", sa.BigInteger(), nullable=True),
    )
    op.create_index(
        "ix_table_context_refs_job_id",
        "table_context_refs",
        ["job_id"],
        unique=False,
    )


def downgrade() -> None:
    """Remove job_id column from table_context_refs."""
    op.drop_index("ix_table_context_refs_job_id", table_name="table_context_refs")
    op.drop_column("table_context_refs", "job_id")
