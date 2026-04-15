"""add_run_ids_and_run_statuses_to_tasks

Revision ID: 886b7b988f00
Revises: 2bde1ead8ddf
Create Date: 2026-04-06 15:33:49.616854

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '886b7b988f00'
down_revision: str | Sequence[str] | None = '2bde1ead8ddf'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add run_ids and run_statuses JSON columns to tasks table."""
    op.add_column('tasks', sa.Column('run_ids', sa.JSON(), nullable=False, server_default='[]'))
    op.add_column('tasks', sa.Column('run_statuses', sa.JSON(), nullable=False, server_default='[]'))


def downgrade() -> None:
    """Remove run_ids and run_statuses from tasks table."""
    op.drop_column('tasks', 'run_statuses')
    op.drop_column('tasks', 'run_ids')
