"""add_advisory_id_to_table_context_refs

Revision ID: bfb0653578fb
Revises: b7c8d9e0f1a2
Create Date: 2026-04-18 06:22:13.635596

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

from aaiclick.snowflake_id import get_snowflake_id

# revision identifiers, used by Alembic.
revision: str = "bfb0653578fb"
down_revision: str | Sequence[str] | None = "b7c8d9e0f1a2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add advisory_id to table_context_refs.

    advisory_id is the pg_advisory_lock key that serializes concurrent
    inserts into the same shared CH table in distributed mode.

    Invariant (enforced in OrchLifecycleHandler.INCREF, not the DB): rows
    sharing the same table_name MUST share the same advisory_id value.
    Backfill below mints one Snowflake per distinct table_name and applies
    it to all rows of that name.
    """
    # 1. Nullable column so existing rows can be backfilled.
    with op.batch_alter_table("table_context_refs") as batch_op:
        batch_op.add_column(sa.Column("advisory_id", sa.BigInteger(), nullable=True))

    # 2. Backfill: one Snowflake ID per distinct table_name, applied to all rows.
    bind = op.get_bind()
    names = bind.execute(sa.text("SELECT DISTINCT table_name FROM table_context_refs")).scalars().all()
    for table_name in names:
        bind.execute(
            sa.text(
                "UPDATE table_context_refs SET advisory_id = :aid WHERE table_name = :tn"
            ),
            {"aid": get_snowflake_id(), "tn": table_name},
        )

    # 3. Lock down NOT NULL now that every row has a value.
    with op.batch_alter_table("table_context_refs") as batch_op:
        batch_op.alter_column("advisory_id", nullable=False)


def downgrade() -> None:
    """Drop advisory_id column."""
    with op.batch_alter_table("table_context_refs") as batch_op:
        batch_op.drop_column("advisory_id")
