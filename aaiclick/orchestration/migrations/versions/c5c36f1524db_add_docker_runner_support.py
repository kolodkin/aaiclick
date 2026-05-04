"""add docker runner support

Revision ID: c5c36f1524db
Revises: b0839badf97f
Create Date: 2026-05-04 04:21:42.668440

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c5c36f1524db'
down_revision: Union[str, Sequence[str], None] = 'b0839badf97f'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("registered_jobs") as batch:
        batch.add_column(
            sa.Column(
                "runner_mode",
                sa.String(),
                nullable=False,
                server_default="subprocess",
            )
        )
        batch.add_column(sa.Column("dockerfile", sa.String(), nullable=True))
        batch.add_column(sa.Column("git_remote", sa.String(), nullable=True))
        batch.add_column(sa.Column("build_context", sa.String(), nullable=True))
        batch.create_check_constraint(
            "ck_registered_jobs_runner_mode",
            "runner_mode IN ('subprocess', 'docker')",
        )

    with op.batch_alter_table("jobs") as batch:
        batch.add_column(
            sa.Column(
                "runner_mode",
                sa.String(),
                nullable=False,
                server_default="subprocess",
            )
        )
        batch.add_column(sa.Column("git_remote", sa.String(), nullable=True))
        batch.add_column(sa.Column("git_sha", sa.String(length=40), nullable=True))
        batch.add_column(sa.Column("git_branch", sa.String(), nullable=True))
        batch.add_column(sa.Column("build_context", sa.String(), nullable=True))
        batch.add_column(sa.Column("dockerfile", sa.String(), nullable=True))
        batch.add_column(sa.Column("image_tag", sa.String(), nullable=True))
        batch.create_check_constraint(
            "ck_jobs_runner_mode",
            "runner_mode IN ('subprocess', 'docker')",
        )


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("jobs") as batch:
        batch.drop_constraint("ck_jobs_runner_mode", type_="check")
        batch.drop_column("image_tag")
        batch.drop_column("dockerfile")
        batch.drop_column("build_context")
        batch.drop_column("git_branch")
        batch.drop_column("git_sha")
        batch.drop_column("git_remote")
        batch.drop_column("runner_mode")

    with op.batch_alter_table("registered_jobs") as batch:
        batch.drop_constraint("ck_registered_jobs_runner_mode", type_="check")
        batch.drop_column("build_context")
        batch.drop_column("git_remote")
        batch.drop_column("dockerfile")
        batch.drop_column("runner_mode")
