schema.sql drift policy
---

`schema.sql` is a hand-maintained mirror of the tables the worker touches
(`aaiclick/orchestration/models.py`). If a Java test fails after a Python
model change, update the fixture to match. The real guard is
`aaiclick/orchestration/execution/test_java_worker_e2e.py`, which runs the
worker in CI against a Python-migrated schema.
