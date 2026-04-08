"""replace_refcount_with_run_ids

Revision ID: b7d3e2f19a4c
Revises: f3a8b1c42d5e
Create Date: 2026-04-08 12:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'b7d3e2f19a4c'
down_revision: Union[str, Sequence[str], None] = 'f3a8b1c42d5e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Replace integer refcount with JSON run_ids array.

    Existing rows get empty arrays — any stale refs are cleaned up
    by the background worker on the next poll cycle.
    """
    op.add_column(
        'table_context_refs',
        sa.Column('run_ids', sa.JSON(), nullable=False, server_default='[]'),
    )
    op.drop_column('table_context_refs', 'refcount')


def downgrade() -> None:
    """Restore integer refcount, drop run_ids."""
    op.add_column(
        'table_context_refs',
        sa.Column('refcount', sa.BigInteger(), nullable=False, server_default='0'),
    )
    op.drop_column('table_context_refs', 'run_ids')
