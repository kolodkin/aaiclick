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
- No new colors / UI work in this spec — only the data the UI keys on.

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

A closed `Literal` (project convention: prefer `Literal` over enums for closed
string sets), declared in `aaiclick/view_models.py` alongside `LogStream`:

```python
LEVEL_DEBUG = "DEBUG"
LEVEL_INFO = "INFO"
LEVEL_WARNING = "WARNING"
LEVEL_ERROR = "ERROR"
LEVEL_CRITICAL = "CRITICAL"
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
```

Custom / numeric logging levels are bucketed to the nearest standard name by
`record.levelno` so the value set stays closed:

| `record.levelno` range | `LogLevel` |
|------------------------|------------|
| `< 20` (incl. `NOTSET`, `DEBUG`) | `DEBUG` |
| `20 – 29` | `INFO` |
| `30 – 39` | `WARNING` |
| `40 – 49` | `ERROR` |
| `>= 50` | `CRITICAL` |

A `normalize_level(levelno: int) -> LogLevel` helper lives next to the constants.

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

Extend `aaiclick/internal_api/test_tasks_logs.py` (per `python-testing-style`):

- True level captured from `logger.warning(...)` / `logger.error(...)`.
- Numeric/custom level bucketed to the nearest `LogLevel`.
- Raw `print()` to stdout → `INFO`; raw write to stderr → `ERROR`.
- Root logger handlers and level are restored after the task body exits.
- No duplicate rows when the task pre-configures a `basicConfig` stderr handler
  (take-over prevents double capture).

## Documentation

- Update the logging doc (the one describing `task_logs` / `capture_task_output`)
  to cover the `level` column, the default-level mapping, and
  `AAICLICK_LOG_LEVEL`. Apply `markdown-style` / `shortify` to the edited doc.

## Known limitations

- The sink is not thread-safe; concurrent logging from task-spawned threads can
  interleave partial lines. Unchanged from today's behavior.
