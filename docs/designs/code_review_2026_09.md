Code Review — September 2026
---

Whole-project review at commit `db56ac4`: every non-test package under
`aaiclick/`, the Java task SDK, the deploy templates, and the CI workflows.
Each finding was verified against the enclosing code and its callers and
names the file, the function, and a concrete trigger. Remove an item when it
is fixed.

---

# High

## Data correctness

- **`sum()` over narrow integer columns wraps modulo the type** —
  `aaiclick/data/object/schema_compute.py`, `_determine_agg_result_type()`.
  ClickHouse widens `sum()` to Int64/UInt64; the result schema keeps the
  source type. Comparison results are UInt8, so `(a == b).sum()` over 300
  matching rows returns 44. Same for `is_null().sum()`, `month().sum()`, and
  Int32 parquet columns.
- **Binary operator result type falls through to the left operand** —
  `aaiclick/data/object/schema_compute.py`, `_result_value_type()`. Integer
  combinations not involving Bool/UInt8 return `type_a`, contradicting the
  docstring. `Int32 * Int32` stores into Int32 and wraps; `UInt64 + Int64`
  declares UInt64 and negative results wrap to 2^64-1.

## Orchestration

- **Exception from the dispatch crashes the worker and strands the task in
  RUNNING** — `aaiclick/orchestration/execution/execution_worker.py`,
  `_execution_worker_loop()`. No `try/except` around `execute_fn`, so a
  missing image tag or `kubectl apply` failure deregisters the worker as
  STOPPED, which the dead-worker sweep (ACTIVE/STOPPING only) never revisits.
- **In-process runner sends no heartbeat during execution** — same file,
  `_execute_in_process()`. Local mode heartbeats only between claims (30 s)
  against a 90 s timeout. Any task over about a minute is declared dead,
  re-queued, and run twice.
- **Cancelled tasks are resurrected and the job marked FAILED** — same file,
  `_set_pending_cleanup()`. Guards on `run_epoch` only, not on
  `TASK_CANCELLED` (unlike `update_task_status`). After `cancel_job`, every
  runner path reports the cancelled run as a failure and the next background
  cycle fails the job.
- **Worker cancellation is swallowed on local-mode shutdown** — same file,
  `_execute_in_process()`. `CancelledError` is caught without checking whether
  the inner task or the worker itself was cancelled. Ctrl+C on `local start`
  orphans the running task and the loop never exits.
- **Kubernetes `wait()` loops forever after a cancellation deletes the pod** —
  `aaiclick/orchestration/execution/kubernetes_worker.py`,
  `_KubernetesVehicle.wait()`. A missing pod yields phase `''`, neither
  Succeeded nor Failed; with no task timeout the worker stops claiming.
- **Scheduled jobs ignore the registration's config** —
  `aaiclick/orchestration/background/background_worker.py`,
  `_check_schedules()`. The raw INSERT omits preservation mode, runner, and
  `image_source`, so a docker/FULL registration runs on the host as
  subprocess/NONE and has its tables dropped.
- **`map()` output and source tables have zero lifecycle refs after the
  expander exits** — `aaiclick/orchestration/operators.py`, `_expand_map()`.
  Nothing pins `out`, so both tables are eligible for the drop sweep before
  any `_map_part` child runs.

## Java SDK

- **`validate_jvm_tasks` accepts any image, including a Python one** —
  `aaiclick/orchestration/image_injection.py`, `validate_jvm_tasks()`. A jvm
  task with no explicit image inherits the Python image and fails at runtime
  with `exec: "--task-id": executable file not found`.
- **Postgres credentials with `+` are form-decoded to spaces** —
  `SqlConfig.java`, `splitUserInfo()`. `URLDecoder.decode` turns `p+ss` into
  `p ss`; SQLAlchemy's `unquote` on the Python side does not.
- **JDBC URL drops the query string; `getHost()` is null for underscored
  hostnames** — `SqlConfig.java`, `fromUrl()`. `?ssl=require` is lost; a
  service named `postgres_db` yields `jdbc:postgresql://null:5432/...`.

## Deployment

- **Scaffolded compose and helm stacks cannot be logged into** —
  `aaiclick/deploy/templates/compose/docker-compose.yaml` and the helm
  `values.yaml`. Distributed URLs enforce auth, but neither sets
  `AAICLICK_JWT_SECRET` or `AAICLICK_ADMIN_PASSWORD`, and `_lifespan` in
  `aaiclick/server/app.py` never calls `require_jwt_secret()`. Health passes;
  login is a 500.

---

# Medium

## Security

- **`query_table` scope is caller-supplied** — `aaiclick/internal_api/lineage.py`,
  `query_table()`, exposed in `aaiclick/server/mcp.py`. `scope_tables` is a
  plain tool argument, so a read token passes `["system.query_log"]` and
  reads it.
- **`expr IN table_name` bypasses scope validation** —
  `aaiclick/ai/agents/lineage_tools.py`, `_table_expressions()`. Only
  `TableExpression` nodes are checked; the bare-identifier `IN` form is not
  one.
- **A model-written `SETTINGS` clause overrides the caps on clickhouse-connect**
  — same file, `run_select()`. `readonly=2` permits settings changes and
  query-level SETTINGS win over HTTP-param settings.
- **Refresh-token rotation is check-then-act** — `aaiclick/auth/store.py`,
  `_stamp_refresh()`. Two concurrent refreshes with the same token both
  succeed. The UPDATE has no `rotated_at IS NULL` predicate or rowcount check.

## Orchestration races

- **Dead-worker recovery does not bump `run_epoch`** —
  `aaiclick/orchestration/background/handler.py`, the retry transition. A
  stalled-but-alive worker can write COMPLETED over a task another worker has
  re-claimed.
- **`_expand_reduce` pins fan out over dependency rows that do not exist yet**
  — `aaiclick/orchestration/operators.py`, `_expand_reduce()`. The PIN handler
  queries `dependencies` live; the `_reduce_part` edges are committed later in
  `register_returned_tasks`. On Postgres the pins insert nothing.
- **`_complete_job` has no job-status guard** —
  `aaiclick/orchestration/background/handler.py` and `sql/complete_job.sql`.
  A `cancel_job` between a worker's COMPLETED write and its rollup turns the
  CANCELLED job into COMPLETED.
- **`map()` never sets `expander.group_id`** —
  `aaiclick/orchestration/operators.py`, `map()`. Until the expander runs the
  group is empty, so `group >> consumer` is vacuously satisfied on
  multi-worker deployments.
- **Heartbeat and cancel-watch loops die on the first transient error** —
  `aaiclick/orchestration/execution/execution_worker.py`,
  `_heartbeat_while_waiting()` and `_watch_for_cancellation()`.
  `gather(return_exceptions=True)` discards the failure; a long container
  task is then re-queued or never sees cancellation.
- **Lifecycle FIFO consumer dies on one SQL error; `flush()` blocks forever**
  — `aaiclick/orchestration/orch_context.py`,
  `OrchLifecycleHandler._process_loop()`.
- **`orch_context` leaks ContextVar tokens and the engine if
  `create_ch_client()` raises** — same file, `orch_context()`. The `.set()`
  calls and engine creation precede the `try`.

## Orchestration correctness

- **A Group passed as a kwarg creates no dependency** —
  `aaiclick/orchestration/decorators.py`, `_collect_upstreams()`. Only `Task`
  is collected; the consumer runs first and receives `[]`.
- **The same upstream in two kwargs raises IntegrityError on commit** — same
  file, `TaskFactory.__call__()`. Duplicate composite-PK `Dependency` rows.
- **The Postgres claim CTE never triggers change signals** —
  `aaiclick/orchestration/events/hooks.py`, `_WRITE_RE`. Matches only
  statements starting with INSERT/UPDATE/DELETE; `claim_next_task.sql` starts
  with `WITH`. No `pg_notify` for claims or job start on Postgres.
- **`run_job` silently drops `image` / `git_*` for unregistered names** —
  `aaiclick/orchestration/registered_jobs.py`, `run_job()`.
- **`_cleanup_unreferenced_tables` deletes registry rows even when the DROP
  failed** — `aaiclick/orchestration/background/background_worker.py`. The
  table is then invisible to every future sweep.

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

## Server and API

- **`expires_at` accepts aware datetimes; the comparison is naive UTC** —
  `aaiclick/auth/view_models.py` `CreateApiTokenRequest` and
  `aaiclick/internal_api/api_tokens.py` `create_token()`. The SPA sends
  `...Z`, so every token mint with an expiry is a 500.
- **`limit` / `offset` are unconstrained on every list filter** —
  `aaiclick/view_models.py` and the auth, audit, viewer filter models. Huge
  limits return whole tables; negative values are a 500 on Postgres.
- **REST scope enforcement keys purely on HTTP method** —
  `aaiclick/server/auth.py`, `require_principal()`. POST `/viewer/query` and
  dashboard runs are reads, tagged read in MCP, but reject read-scope tokens.
- **TOTP with non-ASCII input is a 500** — `aaiclick/auth/security.py`,
  `verify_totp()`. `hmac.compare_digest` raises `TypeError`.
- **LIMIT appended on the same line as a trailing `--` comment** —
  `aaiclick/ai/agents/lineage_tools.py`, `run_select()`. On chdb the SETTINGS
  clause is swallowed too, so the query runs with no caps.

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
- **`max_result_rows` without `result_overflow_mode='break'`** turns >1001
  rows into an error — `aaiclick/ai/agents/lineage_tools.py`, `run_select()`.
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

1. High data correctness (silent wrong numbers).
2. Worker crash, missing heartbeat, cancelled-task resurrection, scheduled-job
   config.
3. Deploy templates, then `SqlConfig.java`.
4. Remaining Mediums grouped by shared root cause: missing status guards,
   missing `try/except` in worker loops.
