"""add composite index ix_tasks_job_id_status

Revision ID: b72b3d09a353
Revises: 5d244433f837
Create Date: 2026-05-12 04:22:15.004407

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b72b3d09a353"
down_revision: str | Sequence[str] | None = "5d244433f837"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_index("ix_tasks_job_id_status", "tasks", ["job_id", "status"], unique=False)


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index("ix_tasks_job_id_status", table_name="tasks")
