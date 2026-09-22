Code Review — September 2026
---

Whole-project review at commit `db56ac4`: every non-test package under
`aaiclick/`, the Java task SDK, the deploy templates, and the CI workflows.
Each finding was verified against the enclosing code and its callers and
names the file, the function, and a concrete trigger. Remove an item when it
is fixed.

---

# Medium

## Data correctness

- **Renamed-then-selected field falls back to non-nullable Float64** —
  `aaiclick/data/object/object.py`, `Object._get_query_info()`. Looks up the
  post-rename name in the source schema. `obj.rename({'a': 'x'})['x'] + 1`
  returns floats; nullable sources get NULLs inserted into a non-nullable
  column.
- **Same-table fast path ignores computed columns, renames, and ARRAY JOIN**
  — `aaiclick/data/object/operators.py`, `_apply_operator_db()`.
  `v['d'] + obj['x']` with `d` computed emits `SELECT d + x FROM base_table`.
- **Exploded computed columns are emitted last but mapped positionally in
  dict order** — `aaiclick/data/object/object.py`, `View._select_head()` vs
  `View._effective_columns()`. With two computed columns and one exploded,
  `data()` assigns values to the wrong keys.

## Java SDK

- **`KwargsResolver.resolve` is applied to the whole kwargs object** —
  `AaiTaskShim.java`. A parameter named `native_value`, `ref_type`, or
  `object_type` unwraps or rejects the entire map.
- **JSON null bound to a primitive is coerced to 0** — `KwargsBinder.java`.
- **jvm attempts are never registered via `register_run`** —
  `aaiclick/orchestration/execution/docker_worker.py` and the kubernetes
  path. `Task.run_ids` / `run_statuses` stay empty.
- **Commit-time validation rejects only object refs** —
  `aaiclick/orchestration/image_injection.py`, `validate_jvm_tasks()`.
  Callable, group, and pydantic refs pass and fail inside the container.
- **`Class.forName` ignores the thread context classloader** —
  `TaskRegistry.java`. Breaks Spring Boot and layered fat jars.
- **Runner secrets on the `docker run` command line** —
  `aaiclick/orchestration/execution/docker_worker.py`, `-e KEY=VALUE` with DB
  URLs is visible in `ps`.

---

# Low

- **Login timing oracle**: short-circuit before bcrypt for unknown users —
  `aaiclick/internal_api/auth.py`, `login()`.
- **TOTP replay**: no last-accepted-step record — `aaiclick/auth/security.py`,
  `verify_totp()`.
- **ClickHouse URL userinfo not percent-decoded**, unlike the SQL URL —
  `aaiclick/backend.py` `parse_ch_url()` and
  `aaiclick/data/data_context/clickhouse_client.py`.
- **`setup` MCP tool returns both DB URLs with embedded passwords** —
  `aaiclick/internal_api/setup.py`, `SetupResult`.
- **`debug_result` returns provider failures as a prose answer** with exit 0
  — `aaiclick/ai/agents/debug_agent.py`.
- **`query_with_tools` crashes on empty tool arguments** —
  `aaiclick/ai/provider.py`.
- **`_materialize_array_join` leaks `tmp_*` tables** on INSERT failure —
  `aaiclick/data/object/operators.py`.
- **`insert_from_url` fails on every `aai_id=True` table** —
  `aaiclick/data/object/object.py`.
- **`delete_object` with an invalid name is a 500** —
  `aaiclick/internal_api/objects.py`.
- **Task logs without `tail` are unbounded** — `aaiclick/internal_api/tasks.py`.
- **Saved-queries `total` is the post-LIMIT count** with no offset —
  `aaiclick/internal_api/viewer.py`, `list_saved_queries()`.
- **CSV viewer path has no execution-time cap** — same file,
  `query_object_bytes()`.
- **`_pump_stream` decodes 64 KiB chunks independently** —
  `aaiclick/orchestration/execution/runner.py`. Split multibyte characters
  become U+FFFD.
- **`JobFactory.__call__` captures a kwarg named `run_type` or
  `registered_job_id`** — `aaiclick/orchestration/decorators.py`.
- **`task_scope` orphans the lifecycle loop** if oplog init raises —
  `aaiclick/orchestration/orch_context.py`.
- **Stale reference** to `_pod_manifest()` (real name `_build_pod_manifest`)
  — `docs/designs/java-sdk.md`.
- **Schema-fixture CI step duplicated verbatim** between `publish.yaml` and
  `test.yaml`; extract to a composite action.

---

# Convention Violations

- `TYPE_CHECKING` imports: `aaiclick/oplog/cleanup.py`,
  `aaiclick/data/data_context/data_context.py`, `data_extraction.py`,
  `aaiclick/data/object/url.py`.
- Uncommented inline imports: `aaiclick/__main__.py` (compose, deploy, and
  orchestration CLI handlers), `aaiclick/testing.py`,
  `aaiclick/snowflake/snowflake_id.py`,
  `aaiclick/orchestration/execution/mp_worker.py`,
  `aaiclick/orchestration/orch_context.py`, `aaiclick/data/object/object.py`
  (`transforms` imports), `chdb_client.py`.
- `Any` as a shortcut: `aaiclick/data/object/object.py` (`_UNSET` sentinel,
  `order_by` / `limit` / `offset` overrides, `data() -> Any`),
  `mp_worker.py`, `background_worker.py`, `aaiclick/ai/provider.py`.

---

# Fix Order

Remaining Mediums are grouped by area above; the Java SDK attempt and
validation items share the jvm launch path.
