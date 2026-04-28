"""simplify lifecycle preserve column drop run_refs and context_refs add task_name_locks

Revision ID: 7f7753f6fda4
Revises: 161cfe0f1117
Create Date: 2026-04-28 06:39:25.105739

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import sqlmodel


# revision identifiers, used by Alembic.
revision: str = '7f7753f6fda4'
down_revision: Union[str, Sequence[str], None] = '161cfe0f1117'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
