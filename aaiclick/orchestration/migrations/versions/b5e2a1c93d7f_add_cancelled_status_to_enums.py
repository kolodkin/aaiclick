"""add_cancelled_status_to_enums

Revision ID: b5e2a1c93d7f
Revises: a3c1f8e72b4d
Create Date: 2026-03-06 22:00:00.000000

"""
from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'b5e2a1c93d7f'
down_revision: str | Sequence[str] | None = 'a3c1f8e72b4d'
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add CANCELLED value to jobstatus and taskstatus PostgreSQL enums."""
    op.execute("ALTER TYPE jobstatus ADD VALUE IF NOT EXISTS 'CANCELLED'")
    op.execute("ALTER TYPE taskstatus ADD VALUE IF NOT EXISTS 'CANCELLED'")


def downgrade() -> None:
    """Remove CANCELLED from jobstatus and taskstatus enums.

    PostgreSQL does not support removing values from enums directly.
    Requires recreating the enum type and updating all references.
    """
    # Downgrade jobstatus: recreate without CANCELLED
    op.execute("ALTER TYPE jobstatus RENAME TO jobstatus_old")
    op.execute("CREATE TYPE jobstatus AS ENUM ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED')")
    op.execute("ALTER TABLE jobs ALTER COLUMN status TYPE jobstatus USING status::text::jobstatus")
    op.execute("DROP TYPE jobstatus_old")

    # Downgrade taskstatus: recreate without CANCELLED
    op.execute("ALTER TYPE taskstatus RENAME TO taskstatus_old")
    op.execute("CREATE TYPE taskstatus AS ENUM ('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED')")
    op.execute("ALTER TABLE tasks ALTER COLUMN status TYPE taskstatus USING status::text::taskstatus")
    op.execute("DROP TYPE taskstatus_old")
