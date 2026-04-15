"""add_preservation_mode_and_sampling_strategy_to_jobs

Revision ID: 3e1b4e54721c
Revises: d2a4f6b8c1e3
Create Date: 2026-04-12 15:29:13.146444

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '3e1b4e54721c'
down_revision: str | Sequence[str] | None = 'd2a4f6b8c1e3'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add preservation_mode and sampling_strategy to jobs."""
    preservation_mode = sa.Enum(
        'NONE', 'FULL', 'STRATEGY',
        name='preservationmode',
    )
    preservation_mode.create(op.get_bind(), checkfirst=True)
    op.add_column(
        'jobs',
        sa.Column(
            'preservation_mode',
            preservation_mode,
            nullable=False,
            server_default='NONE',
        ),
    )
    op.add_column(
        'jobs',
        sa.Column(
            'sampling_strategy',
            sa.JSON(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Remove preservation_mode and sampling_strategy from jobs."""
    op.drop_column('jobs', 'sampling_strategy')
    op.drop_column('jobs', 'preservation_mode')
    sa.Enum(name='preservationmode').drop(op.get_bind(), checkfirst=True)
