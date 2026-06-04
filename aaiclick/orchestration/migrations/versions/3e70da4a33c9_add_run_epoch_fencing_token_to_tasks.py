"""add run_epoch fencing token to tasks

Revision ID: 3e70da4a33c9
Revises: b72b3d09a353
Create Date: 2026-06-04 05:42:54.371286

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "3e70da4a33c9"
down_revision: str | Sequence[str] | None = "b72b3d09a353"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("tasks", sa.Column("run_epoch", sa.BigInteger(), server_default="0", nullable=False))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("tasks", "run_epoch")
