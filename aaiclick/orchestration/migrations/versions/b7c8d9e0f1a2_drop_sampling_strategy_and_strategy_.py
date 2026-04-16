"""drop_sampling_strategy_and_strategy_enum

Revision ID: b7c8d9e0f1a2
Revises: f3a8b1c42d5e
Create Date: 2026-04-16 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7c8d9e0f1a2"
down_revision: str | Sequence[str] | None = "f3a8b1c42d5e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Drop sampling_strategy columns and narrow PreservationMode enum.

    1. Migrate any STRATEGY rows to NONE (safety net for in-flight data).
    2. Drop sampling_strategy from jobs and registered_jobs.
    3. Recreate the preservationmode enum without STRATEGY.
    """
    # Data-migrate STRATEGY → NONE before dropping the enum value
    op.execute("UPDATE jobs SET preservation_mode = 'NONE' WHERE preservation_mode = 'STRATEGY'")
    op.execute("UPDATE registered_jobs SET preservation_mode = 'NONE' WHERE preservation_mode = 'STRATEGY'")

    # Drop sampling_strategy columns
    op.drop_column("jobs", "sampling_strategy")
    op.drop_column("registered_jobs", "sampling_strategy")

    # Narrow the enum: remove STRATEGY
    # PostgreSQL does not support removing values from enums directly.
    # Recreate the type with only NONE and FULL.
    op.execute("ALTER TYPE preservationmode RENAME TO preservationmode_old")
    op.execute("CREATE TYPE preservationmode AS ENUM ('NONE', 'FULL')")
    op.execute(
        "ALTER TABLE jobs ALTER COLUMN preservation_mode "
        "TYPE preservationmode USING preservation_mode::text::preservationmode"
    )
    op.execute(
        "ALTER TABLE registered_jobs ALTER COLUMN preservation_mode "
        "TYPE preservationmode USING preservation_mode::text::preservationmode"
    )
    op.execute("DROP TYPE preservationmode_old")


def downgrade() -> None:
    """Restore sampling_strategy columns and STRATEGY enum value."""
    # Re-add STRATEGY to the enum
    op.execute("ALTER TYPE preservationmode RENAME TO preservationmode_old")
    op.execute("CREATE TYPE preservationmode AS ENUM ('NONE', 'FULL', 'STRATEGY')")
    op.execute(
        "ALTER TABLE jobs ALTER COLUMN preservation_mode "
        "TYPE preservationmode USING preservation_mode::text::preservationmode"
    )
    op.execute(
        "ALTER TABLE registered_jobs ALTER COLUMN preservation_mode "
        "TYPE preservationmode USING preservation_mode::text::preservationmode"
    )
    op.execute("DROP TYPE preservationmode_old")

    # Re-add sampling_strategy columns
    op.add_column("jobs", sa.Column("sampling_strategy", sa.JSON(), nullable=True))
    op.add_column("registered_jobs", sa.Column("sampling_strategy", sa.JSON(), nullable=True))
