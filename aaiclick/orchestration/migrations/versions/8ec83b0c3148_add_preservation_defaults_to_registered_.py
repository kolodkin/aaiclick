"""add_preservation_defaults_to_registered_jobs

Revision ID: 8ec83b0c3148
Revises: 3e1b4e54721c
Create Date: 2026-04-13 05:27:52.183328

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '8ec83b0c3148'
down_revision: Union[str, Sequence[str], None] = '3e1b4e54721c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add preservation_mode and sampling_strategy defaults to registered_jobs.

    Reuses the ``preservationmode`` PostgreSQL enum type created by
    revision ``3e1b4e54721c`` (Phase 0) — passing ``create_type=False``
    prevents a duplicate CREATE TYPE.
    """
    preservation_mode = sa.Enum(
        'NONE', 'FULL', 'STRATEGY',
        name='preservationmode',
        create_type=False,
    )
    op.add_column(
        'registered_jobs',
        sa.Column('preservation_mode', preservation_mode, nullable=True),
    )
    op.add_column(
        'registered_jobs',
        sa.Column('sampling_strategy', sa.JSON(), nullable=True),
    )


def downgrade() -> None:
    """Remove preservation_mode and sampling_strategy from registered_jobs."""
    op.drop_column('registered_jobs', 'sampling_strategy')
    op.drop_column('registered_jobs', 'preservation_mode')
