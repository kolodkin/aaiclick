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
- Stamp each line with a **per-line creation timestamp** (when it was emitted,
  not when the batch was flushed) and expose it through the read path.
- Require **zero user setup** — capture is automatic for every task run.
- Surface `level` and the timestamp through the existing read path → REST → UI
  with no extra endpoints. Timestamp display in the UI is **opt-in (toggle)**.

## Non-goals (YAGNI)

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
| Per-line `created_at` | Stamped at emission (reuses existing column); UI display opt-in via toggle |

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

`LogLine` gains `level` and a per-line `created_at`:

```python
from datetime import datetime

class LogLine(BaseModel):
    stream: LogStream
    level: LogLevel
    text: str
    created_at: datetime  # when the line was emitted (UTC), not flush time
```

Because `LogLine` is the unit returned by `read_task_logs` and embedded in
`TaskLogsView`, both `level` and `created_at` reach the REST surface and SPA
types with no extra wiring (`created_at` serializes as an ISO-8601 string).

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

Only `level` is a new column; `created_at DateTime64(3)` already exists. What
changes is its **meaning**: today every row in a run gets one flush-time value;
now each row carries the line's own emission time. That is a write-path change
(below), not a schema change. `_validate_schema` already raises a "drop the
table and let aaiclick recreate it" error when the `level` column is missing,
which is the chosen rollout path. No Alembic migration (CH tables are not
Alembic-managed).

## Capture wiring

`aaiclick/orchestration/logging.py`:

### `_ChLogSink`

- Every completed line is stamped with `created_at = utc_now()` **at the moment
  it is appended** to `_lines`, so the timestamp reflects emission time, not the
  end-of-task flush. Each `LogLine` now carries `level` and `created_at`.
- `write(stream, data)` tags completed lines with the stream's **default**
  level: `stdout → INFO`, `stderr → ERROR`.
- A new entry point appends pre-leveled lines from the handler, e.g.
  `record(level, text)` — splits a multi-line message/traceback into per-line
  `LogLine`s that share the same `level`, appended in emission order to the same
  `_lines` list so ordering with `print()` output is preserved. (All lines of
  one record share that record's stamp.)

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

- `flush_task_logs`: include `line.level` and `line.created_at` in each row
  tuple; column lists are driven by `TASK_LOGS_EXPECTED_COLUMNS`, so they update
  automatically. Drop the single batch `now = utc_now()` — the timestamp now
  comes from each line.
- `read_task_logs`: add `level` and `created_at` to both `SELECT`s and to the
  `LogLine` construction. Ordering stays by `seq` (the canonical emission order);
  `created_at` is metadata, not the sort key.

## Frontend rendering

The point of `level` is UI coloring and the timestamp is opt-in detail, so the
SPA consumes both. The change is small and lives in the existing log viewer:

- **Generated types** (`src/api/schema.ts`, `src/api/types.ts`): regenerated from
  the OpenAPI schema — `LogLine` gains `level: "DEBUG" | "INFO" | "WARNING" |
  "ERROR" | "CRITICAL"` and `created_at: string`. No hand-editing.
- **`src/components/LogViewer.tsx`**: today each line is
  `className={line.stream === "stderr" ? "log-line log-stderr" : "log-line"}`.
  Two changes:
  1. Per-level class + stable test hook on each line.
  2. A "Show timestamps" toggle (a `useState(false)` checkbox in the viewer
     header). When on, each line is prefixed with a `<span class="log-ts">` of
     the formatted `created_at`; when off, the timestamp is not rendered.

  ```tsx
  <div
    key={i}
    data-testid={`log-line-${line.level}`}
    className={`log-line log-level-${line.level.toLowerCase()}` +
               (line.stream === "stderr" ? " log-stderr" : "")}
  >
    {showTimestamps && <span className="log-ts">{fmtTs(line.created_at)} </span>}
    {line.text}
  </div>
  ```

  The existing `log-stderr` class stays (back-compat for raw-stream coloring);
  `log-level-*` is the new severity key, and `data-testid` gives the e2e test a
  selector that does not depend on CSS. The toggle defaults **off** so the
  default view is unchanged.
- **`src/styles/globals.css:201-214`**: add one color rule per level alongside
  the existing `.logs .log-stderr` rule — e.g. `.log-level-error` /
  `.log-level-critical` red, `.log-level-warning` amber, `.log-level-info`
  default, `.log-level-debug` muted. Severity color takes precedence over the
  stream color. Add a muted `.log-ts` rule for the timestamp prefix.

## Data flow

```
logger.info("x") ──► _ChLogHandler.emit ──► log_file + console (direct)
                                       └──► sink.record("INFO", "x")  [+created_at]
print("y")       ──► sys.stdout (TeeWriter) ──► console + log_file
                                          └──► sink.write("stdout", "y")  [INFO +created_at]
print(err, file=sys.stderr) ─► TeeWriter ──► sink.write("stderr", ...)    [ERROR +created_at]
                                          (task end)
sink.finalize() ──► flush_task_logs ──► CH task_logs(..., level, line, created_at)
                              read_task_logs ──► LogLine(stream, level, text, created_at)
                                            ──► TaskLogsView ──► REST ──► UI (toggle)
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
- Each line carries a `created_at`; lines emitted in sequence have
  non-decreasing timestamps, and they are *not* all equal to the flush time
  (proves per-line stamping, not the old per-batch value).

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

  # timestamp is opt-in: hidden by default, shown after toggling.
  assert logs.locator(".log-ts").count() == 0
  page.get_by_label("Show timestamps").check()
  logs.locator(".log-ts").first.wait_for(timeout=5000)
  ```

  The `data-testid` assertions prove the right level reached the DOM; the
  `getComputedStyle` check proves the CSS rule is wired so the line is actually
  colored; the toggle assertions prove `created_at` is delivered and gated behind
  the opt-in. This runs in CI under the existing `test-ui-e2e-dist` job
  (`.github/workflows/_test-reusable.yaml`) — no new workflow needed, since that
  job already builds the SPA and stands up Postgres + ClickHouse.

## Documentation

- Update the logging doc (the one describing `task_logs` / `capture_task_output`)
  to cover the `level` column, the default-level mapping, `AAICLICK_LOG_LEVEL`,
  the per-line `created_at` semantics (emission time, not flush time), and the
  opt-in timestamp toggle in the viewer. Apply `markdown-style` / `shortify` to
  the edited doc.

## Known limitations

- The sink is not thread-safe; concurrent logging from task-spawned threads can
  interleave partial lines. This is a plain in-memory race on the sink's buffers
  (before any ClickHouse I/O), so it is **backend-independent** — not a chdb
  artifact. Unchanged from today's behavior. (Distinct from the deliberate
  buffer-and-flush-once design, which *is* chdb-driven: a single end-of-task
  flush avoids contending with the task's own queries on chdb's single session.)
