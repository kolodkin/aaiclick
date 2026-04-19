"""move_table_registry_to_sql

Revision ID: c8f4a2b91e57
Revises: bfb0653578fb
Create Date: 2026-04-19 09:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c8f4a2b91e57"
down_revision: str | Sequence[str] | None = "bfb0653578fb"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create table_registry in SQL.

    Previously lived in ClickHouse (MergeTree ORDER BY created_at). Moved
    to SQL because it is cleanup metadata — every read is a keyed lookup
    or owner join during background cleanup. table_name is the primary
    key because registry is strictly 1:1 per table (MergeTree had no
    PK support, but semantically this is now enforceable).

    Existing rows are copied from the CH side by init_oplog_tables() on
    the first post-upgrade startup; this migration only creates the SQL
    schema.
    """
    op.create_table(
        "table_registry",
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=True),
        sa.Column("task_id", sa.BigInteger(), nullable=True),
        sa.Column("run_id", sa.BigInteger(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("table_name"),
    )
    op.create_index("ix_table_registry_job_id", "table_registry", ["job_id"])
    op.create_index("ix_table_registry_created_at", "table_registry", ["created_at"])


def downgrade() -> None:
    """Drop the SQL table_registry. Does not restore the CH-side table."""
    op.drop_index("ix_table_registry_created_at", table_name="table_registry")
    op.drop_index("ix_table_registry_job_id", table_name="table_registry")
    op.drop_table("table_registry")
