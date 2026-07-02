"""rename workers to execution_workers

Rename the execution-worker entity at the schema level to match the
``ExecutionWorker`` model: the ``workers`` table becomes ``execution_workers``,
``tasks.worker_id`` becomes ``tasks.execution_worker_id``, and
``build_tasks.holder_worker_id`` becomes ``build_tasks.holder_execution_worker_id``.
Dependent indexes and the foreign-key constraint are renamed to match so the
schema stays free of autogenerate drift. Data is preserved (rename, not
drop/create).

Revision ID: a7d2e1f9c3b4
Revises: 1b557dfe7b15
Create Date: 2026-07-02 00:00:00.000000

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a7d2e1f9c3b4"
down_revision: str | Sequence[str] | None = "1b557dfe7b15"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.rename_table("workers", "execution_workers")
    op.execute("ALTER INDEX ix_workers_hostname RENAME TO ix_execution_workers_hostname")
    op.execute("ALTER INDEX ix_workers_last_heartbeat RENAME TO ix_execution_workers_last_heartbeat")
    op.execute("ALTER INDEX ix_workers_status RENAME TO ix_execution_workers_status")

    op.alter_column("tasks", "worker_id", new_column_name="execution_worker_id")
    op.execute("ALTER INDEX ix_tasks_worker_id RENAME TO ix_tasks_execution_worker_id")
    op.execute("ALTER TABLE tasks RENAME CONSTRAINT tasks_worker_id_fkey TO tasks_execution_worker_id_fkey")

    op.alter_column("build_tasks", "holder_worker_id", new_column_name="holder_execution_worker_id")


def downgrade() -> None:
    """Downgrade schema."""
    op.alter_column("build_tasks", "holder_execution_worker_id", new_column_name="holder_worker_id")

    op.execute("ALTER TABLE tasks RENAME CONSTRAINT tasks_execution_worker_id_fkey TO tasks_worker_id_fkey")
    op.execute("ALTER INDEX ix_tasks_execution_worker_id RENAME TO ix_tasks_worker_id")
    op.alter_column("tasks", "execution_worker_id", new_column_name="worker_id")

    op.execute("ALTER INDEX ix_execution_workers_status RENAME TO ix_workers_status")
    op.execute("ALTER INDEX ix_execution_workers_last_heartbeat RENAME TO ix_workers_last_heartbeat")
    op.execute("ALTER INDEX ix_execution_workers_hostname RENAME TO ix_workers_hostname")
    op.rename_table("execution_workers", "workers")
