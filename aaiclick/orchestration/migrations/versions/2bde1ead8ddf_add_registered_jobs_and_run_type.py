"""add_registered_jobs_and_run_type

Revision ID: 2bde1ead8ddf
Revises: e1f3a7b29d4c
Create Date: 2026-04-04 18:41:30.220334

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '2bde1ead8ddf'
down_revision: Union[str, Sequence[str], None] = 'e1f3a7b29d4c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add registered_jobs table and run_type/registered_job_id to jobs."""
    runtype_enum = sa.Enum('SCHEDULED', 'MANUAL', name='runtype')
    runtype_enum.create(op.get_bind())

    op.create_table(
        'registered_jobs',
        sa.Column('id', sa.BigInteger(), primary_key=True),
        sa.Column('name', sa.String(), nullable=False),
        sa.Column('entrypoint', sa.String(), nullable=False),
        sa.Column('enabled', sa.Boolean(), nullable=False, server_default='true'),
        sa.Column('schedule', sa.String(), nullable=True),
        sa.Column('default_kwargs', sa.JSON(), nullable=True),
        sa.Column('next_run_at', sa.DateTime(), nullable=True),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.UniqueConstraint('name'),
    )
    op.create_index('ix_registered_jobs_name', 'registered_jobs', ['name'])
    op.create_index('ix_registered_jobs_next_run_at', 'registered_jobs', ['next_run_at'])

    op.add_column('jobs', sa.Column('run_type', sa.Enum('SCHEDULED', 'MANUAL', name='runtype', create_type=False), nullable=False, server_default='MANUAL'))
    op.add_column('jobs', sa.Column('registered_job_id', sa.BigInteger(), sa.ForeignKey('registered_jobs.id'), nullable=True))
    op.create_index('ix_jobs_registered_job_id', 'jobs', ['registered_job_id'])


def downgrade() -> None:
    """Remove registered_jobs table and run_type/registered_job_id from jobs."""
    op.drop_index('ix_jobs_registered_job_id', 'jobs')
    op.drop_column('jobs', 'registered_job_id')
    op.drop_column('jobs', 'run_type')

    op.drop_index('ix_registered_jobs_next_run_at', 'registered_jobs')
    op.drop_index('ix_registered_jobs_name', 'registered_jobs')
    op.drop_table('registered_jobs')

    runtype_enum = sa.Enum('SCHEDULED', 'MANUAL', name='runtype')
    runtype_enum.drop(op.get_bind())
