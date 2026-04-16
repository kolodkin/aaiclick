"""add_stopping_status_to_workerstatus

Revision ID: f3a8b1c42d5e
Revises: e1f4a7c29b03
Create Date: 2026-04-07 16:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f3a8b1c42d5e"
down_revision: str | Sequence[str] | None = "e1f4a7c29b03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add STOPPING value to workerstatus PostgreSQL enum."""
    op.execute("ALTER TYPE workerstatus ADD VALUE IF NOT EXISTS 'STOPPING'")


def downgrade() -> None:
    """Remove STOPPING from workerstatus enum.

    PostgreSQL does not support removing values from enums directly.
    Requires recreating the enum type and updating all references.
    """
    op.execute("ALTER TYPE workerstatus RENAME TO workerstatus_old")
    op.execute("CREATE TYPE workerstatus AS ENUM ('ACTIVE', 'IDLE', 'STOPPED')")
    op.execute("ALTER TABLE workers ALTER COLUMN status TYPE workerstatus USING status::text::workerstatus")
    op.execute("DROP TYPE workerstatus_old")
