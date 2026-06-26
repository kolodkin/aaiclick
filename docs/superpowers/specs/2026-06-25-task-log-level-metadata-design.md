# Task log `level` metadata for UI coloring

## Problem

Task output captured into ClickHouse `task_logs` carries a `stream` field
(`stdout` / `stderr`) so the UI can distinguish the two streams. It carries no
**severity**: a `logger.error(...)` is indistinguishable from a stack trace or
an ordinary `print()` that happens to go to stderr. The UI cannot color by
severity (info / warning / error) because the level was never recorded.

This framework owns the whole task-execution path, so it can install a Python
`logging` handler during task capture and persist each record's level as
per-line metadata.

## Goals

- Record a `level` for every captured log line.
- Capture **true levels** for `logging` records (`logger.info/warning/error/...`).
- Give plain `print()` / raw stream output a sensible default level.
- Require **zero user setup** — capture is automatic for every task run.
- Surface `level` through the existing read path → REST → UI with no extra
  endpoints.

## Non-goals (YAGNI)

- No per-line capture timestamps beyond the existing `created_at`.
- No change to thread-safety of the sink (single-task assumption is unchanged;
  noted as a known limitation).
- Minimal level-based coloring only. The UI gains one CSS class per `LogLevel`
  and keys color on it; an elaborate themed palette, per-level filtering, or a
  log-level selector are out of scope.

## Decisions

| Question | Decision |
|----------|----------|
| What carries a true level | `logging` records only; raw output gets a default |
| Activation | Automatic inside `capture_task_output()` |
| CH schema rollout | Drop & recreate (DDL + expected-columns update) |
| Root-logger handling | Take over root logger for the task window, restore on exit |
| Default level: raw stdout | `INFO` |
| Default level: raw stderr | `ERROR` |
| Default captured root level | `INFO`, overridable via `AAICLICK_LOG_LEVEL` |

## Level vocabulary

Only the closed `Literal` **type** is declared in `aaiclick/view_models.py`
(`logging` provides the level *values* but no typing alias for the closed set):

```python
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
```

We do **not** re-declare `LEVEL_DEBUG = "DEBUG"`-style string constants — the
standard `logging` module already owns level names and numbers
(`logging.DEBUG … logging.CRITICAL` as ints, `logging.getLevelName(...)` for the
names). Custom / numeric levels are bucketed to the nearest standard name by
comparing `record.levelno` against `logging`'s own constants, so the value set
stays closed without a parallel constant table:

```python
import logging

# highest-first so the first threshold a level clears wins; the string
# appears once, mapped to its logging constant, and type-checks as LogLevel.
_LEVEL_THRESHOLDS: tuple[tuple[int, LogLevel], ...] = (
    (logging.CRITICAL, "CRITICAL"),
    (logging.ERROR, "ERROR"),
    (logging.WARNING, "WARNING"),
    (logging.INFO, "INFO"),
    (logging.DEBUG, "DEBUG"),
)

def normalize_level(levelno: int) -> LogLevel:
    for threshold, name in _LEVEL_THRESHOLDS:
        if levelno >= threshold:
            return name
    return "DEBUG"  # below DEBUG (incl. NOTSET)
```

| `record.levelno` | `LogLevel` |
|------------------|------------|
| `< logging.INFO` (incl. `NOTSET`, `DEBUG`) | `DEBUG` |
| `>= logging.INFO`, `< logging.WARNING` | `INFO` |
| `>= logging.WARNING`, `< logging.ERROR` | `WARNING` |
| `>= logging.ERROR`, `< logging.CRITICAL` | `ERROR` |
| `>= logging.CRITICAL` | `CRITICAL` |

Raw stream defaults reuse the same names via the type: `stdout → "INFO"`,
`stderr → "ERROR"` (e.g. a `{stream: LogLevel}` mapping), not new constants.

`LogLine` gains the field (between `stream` and `text`):

```python
class LogLine(BaseModel):
    stream: LogStream
    level: LogLevel
    text: str
```

Because `LogLine` is the unit returned by `read_task_logs` and embedded in
`TaskLogsView`, `level` reaches the REST surface and SPA types with no extra
wiring.

## ClickHouse schema

`aaiclick/oplog/models.py` — add `level String` after `stream` in both the DDL
and the expected-columns map:

```sql
CREATE TABLE IF NOT EXISTS task_logs (
    task_id     UInt64,
    job_id      UInt64,
    run_id      UInt64,
    seq         UInt64,
    stream      String,
    level       String,
    line        String,
    created_at  DateTime64(3)
) ENGINE = MergeTree()
ORDER BY (task_id, run_id, seq)
```

`_validate_schema` already raises a "drop the table and let aaiclick recreate
it" error when an existing table is missing the column, which is the chosen
rollout path. No Alembic migration (CH tables are not Alembic-managed).

## Capture wiring

`aaiclick/orchestration/logging.py`:

### `_ChLogSink`

- `write(stream, data)` tags completed lines with the stream's **default**
  level: `stdout → INFO`, `stderr → ERROR`. Each `LogLine` now carries `level`.
- A new entry point appends pre-leveled lines from the handler, e.g.
  `record(level, text)` — splits a multi-line message/traceback into per-line
  `LogLine`s that share the same `level`, appended in emission order to the same
  `_lines` list so ordering with `print()` output is preserved.

### `_ChLogHandler(logging.Handler)`

- Holds references to the active `sink`, the open `log_file`, and the original
  console stream (`original_stderr`).
- `emit(record)`:
  1. `level = normalize_level(record.levelno)`
  2. `msg = self.format(record)`
  3. Write `msg + "\n"` to `log_file` and `original_stderr` directly and flush —
     for human visibility, **bypassing the tee** so the record is not captured a
     second time as raw stderr text.
  4. `sink.record(level, msg)`.

### `capture_task_output`

In addition to today's tee setup:

1. Create `sink` and `log_file` as today.
2. Build `_ChLogHandler(sink, log_file, original_stderr)` with a formatter.
3. Save the root logger's existing handlers and level; remove the handlers;
   add our handler; set the root level from `AAICLICK_LOG_LEVEL` (default
   `INFO`).
4. `yield log_path`.
5. In `finally`: restore the saved root handlers and level, restore
   `sys.stdout` / `sys.stderr`, close `log_file`, then `flush_task_logs(...)`
   as today.

`AAICLICK_LOG_LEVEL` is a new env var; document it next to `AAICLICK_LOG_DIR`.

### `flush_task_logs` / `read_task_logs`

- `flush_task_logs`: include `line.level` in each row tuple; add `level` to the
  column lists (driven by `TASK_LOGS_EXPECTED_COLUMNS`, so they update
  automatically).
- `read_task_logs`: add `level` to both `SELECT`s and to the `LogLine`
  construction.

## Frontend rendering

The whole point of `level` is UI coloring, so the SPA must consume it. The
change is small and lives in the existing log viewer:

- **Generated types** (`src/api/schema.ts`, `src/api/types.ts`): regenerated from
  the OpenAPI schema — `LogLine` gains `level: "DEBUG" | "INFO" | "WARNING" |
  "ERROR" | "CRITICAL"`. No hand-editing.
- **`src/components/LogViewer.tsx:12-15`**: today each line is
  `className={line.stream === "stderr" ? "log-line log-stderr" : "log-line"}`.
  Change it to also carry a per-level class and a stable test hook, e.g.:

  ```tsx
  <div
    key={i}
    data-testid={`log-line-${line.level}`}
    className={`log-line log-level-${line.level.toLowerCase()}` +
               (line.stream === "stderr" ? " log-stderr" : "")}
  >
    {line.text}
  </div>
  ```

  The existing `log-stderr` class stays (back-compat for raw-stream coloring);
  `log-level-*` is the new severity key, and `data-testid` gives the e2e test a
  selector that does not depend on CSS.
- **`src/styles/globals.css:201-214`**: add one color rule per level alongside
  the existing `.logs .log-stderr` rule — e.g. `.log-level-error` /
  `.log-level-critical` red, `.log-level-warning` amber, `.log-level-info`
  default, `.log-level-debug` muted. Severity color takes precedence over the
  stream color.

## Data flow

```
logger.info("x") ──► _ChLogHandler.emit ──► log_file + console (direct)
                                       └──► sink.record("INFO", "x")
print("y")       ──► sys.stdout (TeeWriter) ──► console + log_file
                                          └──► sink.write("stdout", "y")  [INFO]
print(err, file=sys.stderr) ─► TeeWriter ──► sink.write("stderr", ...)    [ERROR]
                                          (task end)
sink.finalize() ──► flush_task_logs ──► CH task_logs(..., level, line, ...)
                                  read_task_logs ──► LogLine(stream, level, text)
                                                ──► TaskLogsView ──► REST ──► UI
```

## Testing

### Backend / capture

Extend `aaiclick/internal_api/test_tasks_logs.py` (per `python-testing-style`):

- True level captured from `logger.warning(...)` / `logger.error(...)`.
- Numeric/custom level bucketed to the nearest `LogLevel`.
- Raw `print()` to stdout → `INFO`; raw write to stderr → `ERROR`.
- Root logger handlers and level are restored after the task body exits.
- No duplicate rows when the task pre-configures a `basicConfig` stderr handler
  (take-over prevents double capture).

### UI e2e

The existing suite already proves the round trip: `test_smoke.py`'s
`test_task_view_shows_logs` runs the `task_with_output` fixture and asserts the
`div.logs` viewer shows the lines. We extend that pattern for level coloring.

- **Fixture task** — add to `aaiclick/orchestration/fixtures/sample_tasks.py`
  alongside `task_with_output`, e.g. `task_with_log_levels()` that emits one
  record per level through the `logging` module:

  ```python
  def task_with_log_levels():
      """Emit one record at each level so the UI can be checked for coloring."""
      log = logging.getLogger("sample")
      log.info("info line")
      log.warning("warning line")
      log.error("error line")
  ```

- **e2e test** — add `test_task_view_colors_logs_by_level(page, base_url)` to
  `test_e2e/web/test_smoke.py`, reusing the existing helpers
  (`_login_if_needed`, the run-task-then-poll helper) but running the new
  fixture. It then asserts level-keyed rendering using the `data-testid` hooks
  added to `LogViewer.tsx`:

  ```python
  logs = page.locator("div.logs")
  logs.get_by_test_id("log-line-ERROR").get_by_text("error line").wait_for(timeout=15000)
  logs.get_by_test_id("log-line-WARNING").get_by_text("warning line").wait_for(timeout=15000)
  # severity color actually applied (CSS class resolves to a color):
  error_color = logs.locator(".log-level-error").first.evaluate(
      "el => getComputedStyle(el).color"
  )
  assert error_color  # non-empty; exact value asserted against the globals.css rule
  ```

  The `data-testid` assertions prove the right level reached the DOM; the
  `getComputedStyle` check proves the CSS rule is wired so the line is actually
  colored. This runs in CI under the existing `test-ui-e2e-dist` job
  (`.github/workflows/_test-reusable.yaml`) — no new workflow needed, since that
  job already builds the SPA and stands up Postgres + ClickHouse.

## Documentation

- Update the logging doc (the one describing `task_logs` / `capture_task_output`)
  to cover the `level` column, the default-level mapping, and
  `AAICLICK_LOG_LEVEL`. Apply `markdown-style` / `shortify` to the edited doc.

## Known limitations

- The sink is not thread-safe; concurrent logging from task-spawned threads can
  interleave partial lines. This is a plain in-memory race on the sink's buffers
  (before any ClickHouse I/O), so it is **backend-independent** — not a chdb
  artifact. Unchanged from today's behavior. (Distinct from the deliberate
  buffer-and-flush-once design, which *is* chdb-driven: a single end-of-task
  flush avoids contending with the task's own queries on chdb's single session.)
