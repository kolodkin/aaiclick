"""add_name_column_to_tasks

Revision ID: e1f3a7b29d4c
Revises: c4a9b2d81e3f
Create Date: 2026-03-07 14:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'e1f3a7b29d4c'
down_revision: Union[str, Sequence[str], None] = 'c4a9b2d81e3f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add name column to tasks table."""
    op.add_column('tasks', sa.Column('name', sa.String(), nullable=False, server_default=''))


def downgrade() -> None:
    """Remove name column from tasks table."""
    op.drop_column('tasks', 'name')
