"""add schema_doc to table_registry

Revision ID: 161cfe0f1117
Revises: c8f4a2b91e57
Create Date: 2026-04-24 11:25:29.169209

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "161cfe0f1117"
down_revision: str | Sequence[str] | None = "c8f4a2b91e57"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "table_registry",
        sa.Column("schema_doc", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("table_registry", "schema_doc")
