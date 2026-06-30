"""Regression guard for the Postgres claim query's column completeness.

``PgDbHandler.claim_next_task`` reconstructs a ``Task`` from the claim's
``RETURNING`` row via ``Task(**row)``. Any tasks column missing from that row
silently falls back to a model default — which is invisible on SQLite (its
handler does a full ORM reload) and only surfaces in distributed e2e. Two
real bugs came from a hand-maintained ``RETURNING`` list: dropping
``entry_type``/``command`` made shell tasks execute as module tasks, and
dropping ``run_epoch`` defaulted the fencing token to 0 and rejected status
writes for cleared/retried runs. The fix is ``RETURNING *``; this test keeps
it that way.
"""

from __future__ import annotations

import inspect

from .pg_handler import PgDbHandler


def test_pg_claim_returns_all_columns_not_a_hand_listed_subset():
    src = inspect.getsource(PgDbHandler.claim_next_task)
    # The claim must forward every tasks column to Task(**row); a hand-listed
    # RETURNING is the exact pattern that silently dropped new columns.
    assert "RETURNING *" in src, "pg claim must use 'RETURNING *' so no tasks column is dropped from Task(**row)"
