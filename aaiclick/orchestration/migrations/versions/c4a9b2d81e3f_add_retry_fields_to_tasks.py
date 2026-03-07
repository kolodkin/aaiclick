"""add_retry_fields_to_tasks

Revision ID: c4a9b2d81e3f
Revises: b5e2a1c93d7f
Create Date: 2026-03-07 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c4a9b2d81e3f'
down_revision: Union[str, Sequence[str], None] = 'b5e2a1c93d7f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add retry fields to tasks table."""
    op.add_column('tasks', sa.Column('max_retries', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('tasks', sa.Column('attempt', sa.Integer(), nullable=False, server_default='0'))
    op.add_column('tasks', sa.Column('retry_after', sa.DateTime(), nullable=True))


def downgrade() -> None:
    """Remove retry fields from tasks table."""
    op.drop_column('tasks', 'retry_after')
    op.drop_column('tasks', 'attempt')
    op.drop_column('tasks', 'max_retries')
