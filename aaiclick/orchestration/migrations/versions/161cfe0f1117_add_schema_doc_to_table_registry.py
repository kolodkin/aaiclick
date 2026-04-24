"""add schema_doc to table_registry

Revision ID: 161cfe0f1117
Revises: c8f4a2b91e57
Create Date: 2026-04-24 11:25:29.169209

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '161cfe0f1117'
down_revision: Union[str, Sequence[str], None] = 'c8f4a2b91e57'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "table_registry",
        sa.Column("schema_doc", sa.Text(), nullable=True),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("table_registry", "schema_doc")
