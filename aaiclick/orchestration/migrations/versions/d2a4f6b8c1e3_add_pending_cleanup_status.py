"""add_pending_cleanup_status

Revision ID: d2a4f6b8c1e3
Revises: a1b2c3d4e5f6
Create Date: 2026-04-09 12:00:00.000000

"""
from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'd2a4f6b8c1e3'
down_revision: str | Sequence[str] | None = 'a1b2c3d4e5f6'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add PENDING_CLEANUP value to taskstatus PostgreSQL enum."""
    op.execute("ALTER TYPE taskstatus ADD VALUE IF NOT EXISTS 'PENDING_CLEANUP'")


def downgrade() -> None:
    """Remove PENDING_CLEANUP from taskstatus enum.

    PostgreSQL does not support removing values from enums directly.
    Requires recreating the enum type and updating all references.
    """
    op.execute("ALTER TYPE taskstatus RENAME TO taskstatus_old")
    op.execute(
        "CREATE TYPE taskstatus AS ENUM "
        "('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')"
    )
    op.execute(
        "ALTER TABLE tasks ALTER COLUMN status TYPE taskstatus "
        "USING status::text::taskstatus"
    )
    op.execute("DROP TYPE taskstatus_old")
