"""add_worker_stats_fields

Revision ID: d7f7e092e80c
Revises: 7e2d09513f50
Create Date: 2026-01-25 13:50:04.838233

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = 'd7f7e092e80c'
down_revision: Union[str, Sequence[str], None] = '7e2d09513f50'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('workers', sa.Column('started_at', sa.DateTime(), nullable=True))
    op.add_column('workers', sa.Column('tasks_completed', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('workers', sa.Column('tasks_failed', sa.Integer(), nullable=False, server_default='0'))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('workers', 'tasks_failed')
    op.drop_column('workers', 'tasks_completed')
    op.drop_column('workers', 'started_at')
