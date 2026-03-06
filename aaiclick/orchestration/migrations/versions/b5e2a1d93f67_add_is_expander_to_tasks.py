"""add_is_expander_to_tasks

Revision ID: b5e2a1d93f67
Revises: b5e2a1c93d7f
Create Date: 2026-03-06 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b5e2a1d93f67'
down_revision: Union[str, Sequence[str], None] = 'b5e2a1c93d7f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column('tasks', sa.Column('is_expander', sa.Boolean(), nullable=False, server_default='false'))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column('tasks', 'is_expander')
