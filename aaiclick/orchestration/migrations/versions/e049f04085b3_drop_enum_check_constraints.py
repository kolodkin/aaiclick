"""drop enum CHECK constraints: status, run_type, preservation_mode, role

Adopts the Literal-only enum-storage policy (CLAUDE.md, "Prefer Literal").
Closed string-enum columns are validated by their ``Literal`` type plus
boundary validation (SQLModel/Pydantic, the CLI's ``choices=``, API request
models) rather than a DB CHECK constraint, so widening a closed set is a code
change instead of a hand-written constraint migration. The runner_mode CHECKs
were already dropped for this reason in 1798e1da266d; this finishes the job for
the remaining enum columns.

Revision ID: e049f04085b3
Revises: 1798e1da266d
Create Date: 2026-06-24 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e049f04085b3"
down_revision: str | Sequence[str] | None = "1798e1da266d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# (table, constraint name, IN-clause used to recreate it on downgrade)
_CHECKS: tuple[tuple[str, str, str], ...] = (
    ("jobs", "ck_jobs_status", "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')"),
    ("jobs", "ck_jobs_run_type", "run_type IN ('SCHEDULED', 'MANUAL')"),
    ("jobs", "ck_jobs_preservation_mode", "preservation_mode IN ('NONE', 'FULL')"),
    ("registered_jobs", "ck_registered_jobs_preservation_mode", "preservation_mode IN ('NONE', 'FULL')"),
    ("workers", "ck_workers_status", "status IN ('ACTIVE', 'IDLE', 'STOPPING', 'STOPPED')"),
    (
        "tasks",
        "ck_tasks_status",
        "status IN ('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', "
        "'CANCELLED', 'PENDING_CLEANUP', 'UPSTREAM_FAILED')",
    ),
    ("users", "ck_users_role", "role IN ('admin', 'viewer')"),
)


def upgrade() -> None:
    """Drop the enum CHECK constraints.

    Alembic autogenerate cannot detect CHECK-constraint changes, so these ops
    are authored by hand. ``batch_alter_table`` rebuilds the table on SQLite and
    emits ``ALTER TABLE ... DROP CONSTRAINT`` on Postgres.
    """
    for table, name, _ in _CHECKS:
        with op.batch_alter_table(table) as batch:
            batch.drop_constraint(name, type_="check")


def downgrade() -> None:
    """Recreate the enum CHECK constraints as they were before this migration."""
    for table, name, condition in reversed(_CHECKS):
        with op.batch_alter_table(table) as batch:
            batch.create_check_constraint(name, condition)
