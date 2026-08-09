"""Regression guard for the Postgres claim query's column completeness.

``PgDbHandler.claim_next_task`` reconstructs a ``Task`` from the shared claim
SQL's row (``sql/claim_next_task.sql`` — also embedded by the Java worker).
Every ``tasks`` column must reach ``Task(**row)``, or the field silently
falls back to its model default: dropping ``entry_type``/``command`` made
shell tasks run as module tasks; dropping ``run_epoch`` broke the fencing
guard for cleared/retried runs. Runs in every mode — it inspects the SQL
text, not a live database.
"""

from aaiclick.orchestration.execution.pg_handler import CLAIM_NEXT_TASK_SQL


def test_pg_claim_returns_all_columns_not_a_hand_listed_subset():
    # The claim must forward every tasks column to Task(**row); a hand-listed
    # subset rots as columns are added.
    assert "RETURNING *" in CLAIM_NEXT_TASK_SQL, (
        "shared claim SQL must use 'RETURNING *' so no tasks column is dropped from Task(**row)"
    )


def test_shared_claim_sql_parameterizes_worker_capabilities():
    # Worker differences (Python vs Java) must stay bound values — a worker
    # editing the query text would fork the shared contract.
    assert ":entry_types" in CLAIM_NEXT_TASK_SQL
    assert ":allow_image_tasks" in CLAIM_NEXT_TASK_SQL
