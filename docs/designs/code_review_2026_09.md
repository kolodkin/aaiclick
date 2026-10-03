Code Review — September 2026
---

Whole-project review at commit `db56ac4`: every non-test package under
`aaiclick/`, the Java task SDK, the deploy templates, and the CI workflows.
Each finding was verified against the enclosing code and its callers and
names the file, the function, and a concrete trigger. Remove an item when it
is fixed.

---

# Convention Violations

- `TYPE_CHECKING` imports: `aaiclick/data/data_context/data_context.py`,
  `aaiclick/data/object/data_extraction.py`, `aaiclick/data/object/url.py`.
- Uncommented inline imports: `aaiclick/__main__.py` (compose, deploy, and
  orchestration CLI handlers), `aaiclick/testing.py`,
  `aaiclick/snowflake/snowflake_id.py`,
  `aaiclick/orchestration/execution/mp_worker.py`,
  `aaiclick/orchestration/orch_context.py`, `aaiclick/data/object/object.py`
  (`transforms` imports), `chdb_client.py`.
- `Any` as a shortcut: `aaiclick/data/object/object.py` (`_UNSET` sentinel,
  `order_by` / `limit` / `offset` overrides, `data() -> Any`),
  `mp_worker.py`, `background_worker.py`.
