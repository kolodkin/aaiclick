"""worker_id_to_uuid_string

Revision ID: a1b2c3d4e5f6
Revises: f3a8b1c42d5e
Create Date: 2026-04-07 21:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f3a8b1c42d5e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Change workers.id and tasks.worker_id from BIGINT to VARCHAR(36)."""
    # Drop FK constraint first
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.drop_constraint("fk_tasks_worker_id_workers", type_="foreignkey")
        batch_op.alter_column(
            "worker_id",
            existing_type=sa.BigInteger(),
            type_=sa.String(36),
            existing_nullable=True,
        )

    with op.batch_alter_table("workers") as batch_op:
        batch_op.alter_column(
            "id",
            existing_type=sa.BigInteger(),
            type_=sa.String(36),
            existing_nullable=False,
        )

    # Re-add FK constraint
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.create_foreign_key(
            "fk_tasks_worker_id_workers", "workers", ["worker_id"], ["id"]
        )


def downgrade() -> None:
    """Revert workers.id and tasks.worker_id back to BIGINT."""
    with op.batch_alter_table("tasks") as batch_op:
        batch_op.drop_constraint("fk_tasks_worker_id_workers", type_="foreignkey")
        batch_op.alter_column(
            "worker_id",
            existing_type=sa.String(36),
            type_=sa.BigInteger(),
            existing_nullable=True,
        )

    with op.batch_alter_table("workers") as batch_op:
        batch_op.alter_column(
            "id",
            existing_type=sa.String(36),
            type_=sa.BigInteger(),
            existing_nullable=False,
        )

    with op.batch_alter_table("tasks") as batch_op:
        batch_op.create_foreign_key(
            "fk_tasks_worker_id_workers", "workers", ["worker_id"], ["id"]
        )
