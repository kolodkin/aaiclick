"""add UPSTREAM_FAILED to tasks.status CHECK constraint

Revision ID: 5d244433f837
Revises: c5c36f1524db
Create Date: 2026-05-11 17:39:33.230707

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "5d244433f837"
down_revision: str | Sequence[str] | None = "c5c36f1524db"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OLD_STATUSES = "'PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'PENDING_CLEANUP'"
_NEW_STATUSES = (
    "'PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'PENDING_CLEANUP', 'UPSTREAM_FAILED'"
)


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint("ck_tasks_status", f"status IN ({_NEW_STATUSES})")


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("tasks") as batch:
        batch.drop_constraint("ck_tasks_status", type_="check")
        batch.create_check_constraint("ck_tasks_status", f"status IN ({_OLD_STATUSES})")
