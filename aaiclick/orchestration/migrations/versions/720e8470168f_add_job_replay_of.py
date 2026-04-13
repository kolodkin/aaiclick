"""add_job_replay_of

Revision ID: 720e8470168f
Revises: 8ec83b0c3148
Create Date: 2026-04-13 10:06:37.989882

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '720e8470168f'
down_revision: Union[str, Sequence[str], None] = '8ec83b0c3148'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add ``replay_of`` self-referencing FK to ``jobs``.

    Populated by ``replay_job()`` to point the replayed job back at the
    original job whose task graph it cloned. Null for non-replay jobs.
    """
    op.add_column(
        'jobs',
        sa.Column(
            'replay_of',
            sa.BigInteger(),
            sa.ForeignKey('jobs.id'),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Remove ``replay_of`` from ``jobs``."""
    op.drop_column('jobs', 'replay_of')
