"""add_table_context_refs

Revision ID: a3c1f8e72b4d
Revises: d7f7e092e80c
Create Date: 2026-02-17 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a3c1f8e72b4d'
down_revision: Union[str, Sequence[str], None] = 'd7f7e092e80c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create table_context_refs with composite PK (table_name, context_id)."""
    op.create_table(
        'table_context_refs',
        sa.Column('table_name', sa.String(), nullable=False),
        sa.Column('context_id', sa.BigInteger(), nullable=False),
        sa.Column('refcount', sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint('table_name', 'context_id'),
    )
    op.create_index(
        'ix_table_context_refs_context_id',
        'table_context_refs',
        ['context_id'],
        unique=False,
    )


def downgrade() -> None:
    """Drop table_context_refs."""
    op.drop_index('ix_table_context_refs_context_id', table_name='table_context_refs')
    op.drop_table('table_context_refs')
