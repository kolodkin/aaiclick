"""Initial migration: create jobs, tasks, workers, groups, dependencies tables

Revision ID: 001
Revises:
Create Date: 2026-01-21

"""

import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create jobs table
    op.create_table(
        "jobs",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("error", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_jobs_name"), "jobs", ["name"], unique=False)
    op.create_index(op.f("ix_jobs_status"), "jobs", ["status"], unique=False)
    op.create_index(op.f("ix_jobs_created_at"), "jobs", ["created_at"], unique=False)

    # Create groups table
    op.create_table(
        "groups",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("parent_group_id", sa.BigInteger(), nullable=True),
        sa.Column("name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
        ),
        sa.ForeignKeyConstraint(
            ["parent_group_id"],
            ["groups.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_groups_job_id"), "groups", ["job_id"], unique=False)
    op.create_index(op.f("ix_groups_parent_group_id"), "groups", ["parent_group_id"], unique=False)

    # Create workers table
    op.create_table(
        "workers",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("hostname", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("pid", sa.Integer(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("last_heartbeat", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_workers_hostname"), "workers", ["hostname"], unique=False)
    op.create_index(op.f("ix_workers_status"), "workers", ["status"], unique=False)
    op.create_index(op.f("ix_workers_last_heartbeat"), "workers", ["last_heartbeat"], unique=False)

    # Create tasks table
    op.create_table(
        "tasks",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("job_id", sa.BigInteger(), nullable=False),
        sa.Column("group_id", sa.BigInteger(), nullable=True),
        sa.Column("entrypoint", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("kwargs", sa.JSON(), nullable=False),
        sa.Column("status", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("completed_at", sa.DateTime(), nullable=True),
        sa.Column("worker_id", sa.BigInteger(), nullable=True),
        sa.Column("result_table_id", sa.BigInteger(), nullable=True),
        sa.Column("log_path", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("error", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.ForeignKeyConstraint(
            ["job_id"],
            ["jobs.id"],
        ),
        sa.ForeignKeyConstraint(
            ["group_id"],
            ["groups.id"],
        ),
        sa.ForeignKeyConstraint(
            ["worker_id"],
            ["workers.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_tasks_job_id"), "tasks", ["job_id"], unique=False)
    op.create_index(op.f("ix_tasks_group_id"), "tasks", ["group_id"], unique=False)
    op.create_index(op.f("ix_tasks_status"), "tasks", ["status"], unique=False)
    op.create_index(op.f("ix_tasks_created_at"), "tasks", ["created_at"], unique=False)
    op.create_index(op.f("ix_tasks_worker_id"), "tasks", ["worker_id"], unique=False)

    # Create dependencies table
    op.create_table(
        "dependencies",
        sa.Column("previous_id", sa.BigInteger(), nullable=False),
        sa.Column("previous_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("next_id", sa.BigInteger(), nullable=False),
        sa.Column("next_type", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("previous_id", "previous_type", "next_id", "next_type"),
    )
    op.create_index(op.f("ix_dependencies_previous_id"), "dependencies", ["previous_id"], unique=False)
    op.create_index(op.f("ix_dependencies_next_id"), "dependencies", ["next_id"], unique=False)


def downgrade() -> None:
    # Drop tables in reverse order to handle foreign key constraints
    op.drop_index(op.f("ix_dependencies_next_id"), table_name="dependencies")
    op.drop_index(op.f("ix_dependencies_previous_id"), table_name="dependencies")
    op.drop_table("dependencies")

    op.drop_index(op.f("ix_tasks_worker_id"), table_name="tasks")
    op.drop_index(op.f("ix_tasks_created_at"), table_name="tasks")
    op.drop_index(op.f("ix_tasks_status"), table_name="tasks")
    op.drop_index(op.f("ix_tasks_group_id"), table_name="tasks")
    op.drop_index(op.f("ix_tasks_job_id"), table_name="tasks")
    op.drop_table("tasks")

    op.drop_index(op.f("ix_workers_last_heartbeat"), table_name="workers")
    op.drop_index(op.f("ix_workers_status"), table_name="workers")
    op.drop_index(op.f("ix_workers_hostname"), table_name="workers")
    op.drop_table("workers")

    op.drop_index(op.f("ix_groups_parent_group_id"), table_name="groups")
    op.drop_index(op.f("ix_groups_job_id"), table_name="groups")
    op.drop_table("groups")

    op.drop_index(op.f("ix_jobs_created_at"), table_name="jobs")
    op.drop_index(op.f("ix_jobs_status"), table_name="jobs")
    op.drop_index(op.f("ix_jobs_name"), table_name="jobs")
    op.drop_table("jobs")
