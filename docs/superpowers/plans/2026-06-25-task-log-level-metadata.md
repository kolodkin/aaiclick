# Task Log Level Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Capture a per-line log `level` and a per-line `created_at` for task output into ClickHouse and surface both through the read path so the UI can color lines by severity and optionally show timestamps.

**Architecture:** A `logging.Handler` installed automatically by `capture_task_output()` records true levels for `logging.*` calls (raw `print()` keeps a per-stream default: stdout→INFO, stderr→ERROR). Each captured line is stamped with its emission time. `level` is a new ClickHouse column (`created_at` already exists; its semantics change from per-batch to per-line). The SPA log viewer colors by `level` and gains an opt-in "Show timestamps" toggle.

**Tech Stack:** Python 3.12, pydantic, ClickHouse (chdb locally), pytest (async), React 19 + TypeScript + Vite, Playwright (Python).

**Reference spec:** `docs/superpowers/specs/2026-06-25-task-log-level-metadata-design.md`

---

## File Structure

| File | Change | Responsibility |
|------|--------|----------------|
| `aaiclick/view_models.py` | Modify | `LogLevel` Literal, `normalize_level()`, `LogLine` gains `level` + `created_at` |
| `aaiclick/test_view_models.py` | Create | Unit tests for `normalize_level` + `LogLine` defaults |
| `aaiclick/oplog/models.py` | Modify | Add `level String` to `task_logs` DDL + expected columns |
| `aaiclick/orchestration/logging.py` | Modify | Per-line stamping, default level, `record()`, `_ChLogHandler`, root-logger takeover, `AAICLICK_LOG_LEVEL`, level/created_at in flush+read |
| `aaiclick/orchestration/test_logging.py` | Create | Unit tests for `_ChLogSink` + `normalize`-driven defaults (no CH) |
| `aaiclick/internal_api/test_tasks_logs.py` | Modify | Round-trip tests: level + per-line created_at via flush/read; capture takeover |
| `aaiclick/orchestration/fixtures/sample_tasks.py` | Modify | Add `task_with_log_levels()` fixture |
| `src/api/schema.ts` | Regenerate | `LogLine` gains `level` + `created_at` (via `npm run gen-types`) |
| `src/components/LogViewer.tsx` | Modify | Level class + `data-testid`, timestamp toggle, `fmtTs` |
| `src/styles/globals.css` | Modify | Extend `.lvl-*` to all levels, drop `.log-stderr` |
| `test_e2e/web/test_smoke.py` | Modify | `test_task_view_colors_logs_by_level` |
| `docs/orchestration.md` | Modify | Document level/created_at semantics + `AAICLICK_LOG_LEVEL` |
| `docs/ui.md` | Modify | Document level coloring + timestamp toggle |

---

## Task 1: `LogLevel` type, `normalize_level`, and `LogLine` fields

**Files:**
- Modify: `aaiclick/view_models.py:36-49` (the `LogStream` / `LogLine` block)
- Test: `aaiclick/test_view_models.py` (create)

- [ ] **Step 1: Write the failing test**

Create `aaiclick/test_view_models.py`:

```python
from __future__ import annotations

import logging

from aaiclick.view_models import LogLine, normalize_level


def test_normalize_level_exact_standard_levels():
    assert normalize_level(logging.DEBUG) == "DEBUG"
    assert normalize_level(logging.INFO) == "INFO"
    assert normalize_level(logging.WARNING) == "WARNING"
    assert normalize_level(logging.ERROR) == "ERROR"
    assert normalize_level(logging.CRITICAL) == "CRITICAL"


def test_normalize_level_buckets_custom_levels_down():
    assert normalize_level(25) == "INFO"      # between INFO and WARNING
    assert normalize_level(45) == "ERROR"     # between ERROR and CRITICAL
    assert normalize_level(100) == "CRITICAL" # above CRITICAL


def test_normalize_level_below_debug_is_debug():
    assert normalize_level(0) == "DEBUG"      # NOTSET
    assert normalize_level(5) == "DEBUG"


def test_logline_defaults_level_info_and_stamps_created_at():
    line = LogLine(stream="stdout", text="hi")
    assert line.level == "INFO"
    assert line.created_at is not None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest aaiclick/test_view_models.py -v`
Expected: FAIL — `ImportError: cannot import name 'normalize_level'`.

- [ ] **Step 3: Implement the type, helper, and fields**

In `aaiclick/view_models.py`, add `import logging` to the stdlib import group (top of file, with `from datetime import datetime` already present) and `from .datetime_utils import utc_now` to the current-package import group.

Replace the existing stream/`LogLine` block (currently lines 36-49):

```python
# Captured task output streams. Defined here (not in orchestration.view_models)
# so aaiclick.orchestration.logging can import LogLine without forming a cycle
# through the jobs/execution packages.
STDOUT_STREAM = "stdout"
STDERR_STREAM = "stderr"
LogStream = Literal["stdout", "stderr"]


class LogLine(BaseModel):
    """One captured output line tagged with the stream it came from."""

    stream: LogStream
    text: str
```

with:

```python
# Captured task output streams. Defined here (not in orchestration.view_models)
# so aaiclick.orchestration.logging can import LogLine without forming a cycle
# through the jobs/execution packages.
STDOUT_STREAM = "stdout"
STDERR_STREAM = "stderr"
LogStream = Literal["stdout", "stderr"]

LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# highest-first so the first threshold a level clears wins; each string appears
# once mapped to its logging constant and type-checks as LogLevel. logging owns
# the level names/numbers, so we do not re-declare LEVEL_* string constants.
_LEVEL_THRESHOLDS: tuple[tuple[int, LogLevel], ...] = (
    (logging.CRITICAL, "CRITICAL"),
    (logging.ERROR, "ERROR"),
    (logging.WARNING, "WARNING"),
    (logging.INFO, "INFO"),
    (logging.DEBUG, "DEBUG"),
)


def normalize_level(levelno: int) -> LogLevel:
    """Bucket any logging level number to the nearest standard LogLevel name."""
    for threshold, name in _LEVEL_THRESHOLDS:
        if levelno >= threshold:
            return name
    return "DEBUG"  # below DEBUG (incl. NOTSET)


class LogLine(BaseModel):
    """One captured output line tagged with its stream, level, and emit time."""

    stream: LogStream
    level: LogLevel = "INFO"
    text: str
    created_at: datetime = Field(default_factory=utc_now)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest aaiclick/test_view_models.py -v`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/view_models.py aaiclick/test_view_models.py
git commit -m "feat: add LogLevel type, normalize_level, and LogLine level/created_at"
```

---

## Task 2: ClickHouse schema + flush/read carry level and per-line created_at

**Files:**
- Modify: `aaiclick/oplog/models.py:42-69` (DDL + expected columns)
- Modify: `aaiclick/orchestration/logging.py:115-159` (`flush_task_logs`, `read_task_logs`)
- Test: `aaiclick/internal_api/test_tasks_logs.py` (add tests)

- [ ] **Step 1: Write the failing tests**

Add `from datetime import datetime` to the stdlib import group at the top of `aaiclick/internal_api/test_tasks_logs.py` (CLAUDE.md: all imports at top, including in tests). Then add to the same file (the imports `LogLine`, `STDOUT_STREAM`, `STDERR_STREAM`, `flush_task_logs`, `get_task_logs`, `create_job`, `simple_task`, `get_tasks_for_job`, and the helper `_set_run_ids` already exist):

```python
async def test_logs_preserve_level(orch_ctx):
    job = await create_job("logs_level", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 71
    await flush_task_logs(
        task.id,
        job.id,
        run_id,
        [
            LogLine(stream=STDOUT_STREAM, level="INFO", text="info line"),
            LogLine(stream=STDERR_STREAM, level="ERROR", text="error line"),
        ],
    )
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    assert [(line.level, line.text) for line in result.lines] == [
        ("INFO", "info line"),
        ("ERROR", "error line"),
    ]


async def test_logs_preserve_per_line_created_at(orch_ctx):
    job = await create_job("logs_ts", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 72
    early = datetime(2020, 1, 1, 0, 0, 0)  # uses the top-level `from datetime import datetime`
    late = datetime(2020, 1, 1, 0, 0, 5)
    await flush_task_logs(
        task.id,
        job.id,
        run_id,
        [
            LogLine(stream=STDOUT_STREAM, text="first", created_at=early),
            LogLine(stream=STDOUT_STREAM, text="second", created_at=late),
        ],
    )
    await _set_run_ids(task.id, [run_id])

    result = await get_task_logs(task.id)

    # Each line keeps its own stamp (not one shared flush-time value).
    stamps = [line.created_at for line in result.lines]
    assert stamps[0] != stamps[1]
    assert stamps[0].replace(microsecond=0) == early
    assert stamps[1].replace(microsecond=0) == late
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_tasks_logs.py::test_logs_preserve_level aaiclick/internal_api/test_tasks_logs.py::test_logs_preserve_per_line_created_at -v`
Expected: FAIL — `read_task_logs` does not select `level`/`created_at` yet (constructed `LogLine` has default level/now, so assertions mismatch), and/or the insert column count mismatches once the DDL changes.

- [ ] **Step 3: Add the `level` column to the DDL and expected columns**

In `aaiclick/oplog/models.py`, update `TASK_LOGS_DDL` (lines 42-53) to insert `level String` after `stream`:

```python
TASK_LOGS_DDL = """
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
"""
```

and update `TASK_LOGS_EXPECTED_COLUMNS` (lines 61-69) to match, inserting `level` after `stream`:

```python
TASK_LOGS_EXPECTED_COLUMNS: dict[str, str] = {
    "task_id": "UInt64",
    "job_id": "UInt64",
    "run_id": "UInt64",
    "seq": "UInt64",
    "stream": "String",
    "level": "String",
    "line": "String",
    "created_at": "DateTime64(3)",
}
```

- [ ] **Step 4: Update `flush_task_logs` and `read_task_logs`**

In `aaiclick/orchestration/logging.py`, replace `flush_task_logs` (lines 115-133). The column lists are derived from `TASK_LOGS_EXPECTED_COLUMNS`, so the row tuple just needs to match the new column order, and the timestamp comes from each line:

```python
async def flush_task_logs(task_id: int, job_id: int, run_id: int, lines: list[LogLine]) -> None:
    """Best-effort batch insert of captured log lines into CH ``task_logs``.

    A failed write must not fail the task, so errors are logged and swallowed —
    same contract as oplog row writes.
    """
    if not lines:
        return
    rows = [
        [task_id, job_id, run_id, seq, line.stream, line.level, line.text, line.created_at]
        for seq, line in enumerate(lines)
    ]
    try:
        await get_ch_client().insert(
            "task_logs",
            rows,
            column_names=_TASK_LOG_COLS,
            column_type_names=_TASK_LOG_TYPE_NAMES,
        )
    except Exception:
        logger.error("Failed to write task_logs for task %s run %s", task_id, run_id, exc_info=True)
```

Then replace `read_task_logs` (lines 136-159) so both queries select and construct `level` + `created_at`:

```python
async def read_task_logs(task_id: int, run_id: int, tail: int | None = None) -> list[LogLine]:
    """Return captured log lines for a single task attempt from CH ``task_logs``.

    Each line carries its source ``stream`` (stdout / stderr), its ``level``, and
    the time it was emitted. When ``tail`` is given, returns only the last
    ``tail`` lines (still in emission order) — fetched with a ``seq``-descending
    ``LIMIT`` so the read stays bounded for large logs.
    """
    where = "WHERE task_id = {task_id:UInt64} AND run_id = {run_id:UInt64}"
    parameters: dict[str, int] = {"task_id": task_id, "run_id": run_id}
    cols = "stream, level, line, created_at"
    if tail is not None:
        parameters["tail"] = tail
        result = await get_ch_client().query(
            f"SELECT {cols} FROM task_logs {where} ORDER BY seq DESC LIMIT {{tail:UInt64}}",
            parameters=parameters,
        )
        rows = list(reversed(result.result_rows))
    else:
        result = await get_ch_client().query(
            f"SELECT {cols} FROM task_logs {where} ORDER BY seq",
            parameters=parameters,
        )
        rows = result.result_rows
    return [LogLine(stream=row[0], level=row[1], text=row[2], created_at=row[3]) for row in rows]
```

- [ ] **Step 5: Run the new tests and the full logs suite**

Run: `uv run pytest aaiclick/internal_api/test_tasks_logs.py -v`
Expected: PASS — the two new tests plus all 7 pre-existing tests.

> If a pre-existing test fails with a column-count or schema-validation error, the local `task_logs` table predates the new column. The chosen rollout is drop-and-recreate; the test fixtures recreate CH tables per run, so a clean `uv run pytest` re-creates it. If a stale table persists, drop it and re-run.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/oplog/models.py aaiclick/orchestration/logging.py aaiclick/internal_api/test_tasks_logs.py
git commit -m "feat: persist per-line log level and created_at in task_logs"
```

---

## Task 3: `_ChLogSink` per-line stamping, default level, and `record()`

**Files:**
- Modify: `aaiclick/orchestration/logging.py:61-88` (`_ChLogSink`)
- Test: `aaiclick/orchestration/test_logging.py` (create)

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/orchestration/test_logging.py`:

```python
from __future__ import annotations

from aaiclick.orchestration.logging import _ChLogSink
from aaiclick.view_models import STDERR_STREAM, STDOUT_STREAM


def test_sink_default_levels_per_stream():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "out line\n")
    sink.write(STDERR_STREAM, "err line\n")
    lines = sink.finalize()
    assert [(l.stream, l.level, l.text) for l in lines] == [
        (STDOUT_STREAM, "INFO", "out line"),
        (STDERR_STREAM, "ERROR", "err line"),
    ]


def test_sink_record_applies_level_and_splits_multiline():
    sink = _ChLogSink()
    sink.record("WARNING", "first\nsecond")
    lines = sink.finalize()
    assert [(l.level, l.text) for l in lines] == [
        ("WARNING", "first"),
        ("WARNING", "second"),
    ]
    assert all(l.stream == STDERR_STREAM for l in lines)


def test_sink_stamps_each_line_with_created_at():
    sink = _ChLogSink()
    sink.write(STDOUT_STREAM, "a\nb\n")
    lines = sink.finalize()
    assert all(l.created_at is not None for l in lines)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/orchestration/test_logging.py -v`
Expected: FAIL — `_ChLogSink` has no `record` method, and `write` does not set `level`.

- [ ] **Step 3: Update `_ChLogSink`**

In `aaiclick/orchestration/logging.py`, ensure the imports include `normalize_level` and `utc_now` (both already partially imported — confirm the line `from aaiclick.view_models import ...` includes `LogLevel`, `normalize_level`, and that `from aaiclick.datetime_utils import utc_now` is present; it is). Update the import line to:

```python
from aaiclick.view_models import STDERR_STREAM, STDOUT_STREAM, LogLevel, LogLine, LogStream, normalize_level
```

Replace `_ChLogSink` (lines 61-88) with:

```python
_DEFAULT_STREAM_LEVEL: dict[LogStream, LogLevel] = {STDOUT_STREAM: "INFO", STDERR_STREAM: "ERROR"}


class _ChLogSink:
    """Accumulate captured output as level-tagged lines for a CH batch write.

    ``write`` is sync — it's driven by ``print`` through ``_TeeWriter`` while the
    task runs. Each stream (stdout / stderr) keeps its own partial-line buffer so
    a line is tagged with the stream that emitted it; completed lines are
    appended in emission order, each stamped with its own emit time. ``record``
    is the logging path: it appends already-leveled lines from ``_ChLogHandler``.
    The async flush happens once after the task body completes
    (:func:`capture_task_output`), so the task's own ClickHouse work never races
    the log write on a shared (chdb single-session) client.
    """

    def __init__(self) -> None:
        self._partial: dict[LogStream, str] = {STDOUT_STREAM: "", STDERR_STREAM: ""}
        self._lines: list[LogLine] = []

    def write(self, stream: LogStream, data: str) -> None:
        parts = (self._partial[stream] + data).split("\n")
        self._partial[stream] = parts.pop()
        level = _DEFAULT_STREAM_LEVEL[stream]
        self._lines.extend(
            LogLine(stream=stream, level=level, text=p, created_at=utc_now()) for p in parts
        )

    def record(self, level: LogLevel, text: str) -> None:
        """Append a logging record's message as level-tagged line(s)."""
        now = utc_now()
        self._lines.extend(
            LogLine(stream=STDERR_STREAM, level=level, text=p, created_at=now)
            for p in text.split("\n")
        )

    def finalize(self) -> list[LogLine]:
        """Return all captured lines, flushing any unterminated trailing line."""
        for stream in (STDOUT_STREAM, STDERR_STREAM):
            if self._partial[stream]:
                self._lines.append(
                    LogLine(
                        stream=stream,
                        level=_DEFAULT_STREAM_LEVEL[stream],
                        text=self._partial[stream],
                        created_at=utc_now(),
                    )
                )
                self._partial[stream] = ""
        lines, self._lines = self._lines, []
        return lines
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/orchestration/test_logging.py -v`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/logging.py aaiclick/orchestration/test_logging.py
git commit -m "feat: per-line level + emit-time stamping in _ChLogSink"
```

---

## Task 4: `_ChLogHandler` + root-logger takeover in `capture_task_output`

**Files:**
- Modify: `aaiclick/orchestration/logging.py:91-205` (`_TeeWriter` unchanged; add `_ChLogHandler`; extend `capture_task_output`)
- Test: `aaiclick/internal_api/test_tasks_logs.py` (add capture tests)

- [ ] **Step 1: Write the failing tests**

Add to `aaiclick/internal_api/test_tasks_logs.py`. Add `import logging` to the stdlib import group at the top, and change the existing `from aaiclick.orchestration.logging import flush_task_logs` line to `from aaiclick.orchestration.logging import capture_task_output, flush_task_logs, read_task_logs`:

```python
async def test_capture_records_true_level_and_restores_root(orch_ctx):
    job = await create_job("cap_levels", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 81

    root = logging.getLogger()
    before_handlers = list(root.handlers)
    before_level = root.level

    async with capture_task_output(task.id, job.id, run_id):
        logging.getLogger("sample").warning("a warning")
        logging.getLogger("sample").error("an error")
        print("plain stdout")

    # Root logger is restored exactly.
    assert list(root.handlers) == before_handlers
    assert root.level == before_level

    lines = await read_task_logs(task.id, run_id)
    by_text = {l.text: l.level for l in lines}
    assert by_text["a warning"] == "WARNING"
    assert by_text["an error"] == "ERROR"
    assert by_text["plain stdout"] == "INFO"


async def test_capture_no_duplicate_rows_with_preexisting_handler(orch_ctx):
    job = await create_job("cap_dedup", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    run_id = 82

    # Simulate a task that configured its own stderr handler.
    noisy = logging.getLogger()
    extra = logging.StreamHandler()
    noisy.addHandler(extra)
    try:
        async with capture_task_output(task.id, job.id, run_id):
            logging.getLogger("sample").error("once only")
    finally:
        noisy.removeHandler(extra)

    lines = await read_task_logs(task.id, run_id)
    assert [l.text for l in lines].count("once only") == 1
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/internal_api/test_tasks_logs.py::test_capture_records_true_level_and_restores_root aaiclick/internal_api/test_tasks_logs.py::test_capture_no_duplicate_rows_with_preexisting_handler -v`
Expected: FAIL — `capture_task_output` does not install a level handler yet, so `WARNING`/`ERROR` are missing or duplicated.

- [ ] **Step 3: Add `_ChLogHandler` and extend `capture_task_output`**

In `aaiclick/orchestration/logging.py`, add `_ChLogHandler` immediately after `_TeeWriter` (after line 112):

```python
class _ChLogHandler(logging.Handler):
    """Route ``logging`` records into the active CH sink with their true level.

    Echoes the formatted message to the on-host log file and the original stderr
    for visibility, bypassing the tee so the record is not captured a second time
    as raw stderr text.
    """

    def __init__(self, sink: _ChLogSink, log_file: TextIO, console: TextIO):
        super().__init__()
        self._sink = sink
        self._log_file = log_file
        self._console = console
        self.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = normalize_level(record.levelno)
            msg = self.format(record)
            for stream in (self._log_file, self._console):
                stream.write(msg + "\n")
                stream.flush()
            self._sink.record(level, msg)
        except Exception:  # never let logging crash the task
            self.handleError(record)
```

Then extend `capture_task_output` (lines 162-205). Replace its body so it takes over the root logger. The new env var resolves the captured level:

```python
@asynccontextmanager
async def capture_task_output(task_id: int, job_id: int, run_id: int):
    """
    Context manager to capture stdout, stderr, and ``logging`` for one task run.

    Output is teed to the original streams, an on-host log file, and a ClickHouse
    sink. ``logging`` records are routed through :class:`_ChLogHandler` so each
    carries its true level; for the duration of the run the root logger's
    handlers are replaced with ours (restored on exit) so records are captured
    exactly once. The sink is flushed to ``task_logs`` once the body exits (on
    success or failure), giving every runner a host-independent log source.

    Log files are organized as: {base}/{job_id}/{task_id}/{run_id}.log

    Args:
        task_id: Task ID used to generate log file path.
        job_id: Job ID for the directory hierarchy.
        run_id: Per-attempt snowflake ID — each retry gets its own log file.

    Yields:
        str: Path to the log file
    """
    log_dir = os.path.join(get_logs_dir(), str(job_id), str(task_id))
    Path(log_dir).mkdir(parents=True, exist_ok=True)
    log_path = os.path.join(log_dir, f"{run_id}.log")

    original_stdout = sys.stdout
    original_stderr = sys.stderr
    sink = _ChLogSink()
    log_file = open(log_path, "w")

    sys.stdout = _TeeWriter(original_stdout, log_file, sink=sink, source=STDOUT_STREAM)
    sys.stderr = _TeeWriter(original_stderr, log_file, sink=sink, source=STDERR_STREAM)

    root = logging.getLogger()
    saved_handlers = root.handlers[:]
    saved_level = root.level
    ch_handler = _ChLogHandler(sink, log_file, original_stderr)
    root.handlers = [ch_handler]
    root.setLevel(os.getenv("AAICLICK_LOG_LEVEL", "INFO"))

    try:
        yield log_path
    finally:
        root.handlers = saved_handlers
        root.setLevel(saved_level)
        sys.stdout = original_stdout
        sys.stderr = original_stderr
        log_file.close()
        await flush_task_logs(task_id, job_id, run_id, sink.finalize())
```

- [ ] **Step 4: Run the capture tests and the full logs suite**

Run: `uv run pytest aaiclick/internal_api/test_tasks_logs.py -v`
Expected: PASS — both new capture tests plus all earlier tests.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/logging.py aaiclick/internal_api/test_tasks_logs.py
git commit -m "feat: capture logging levels via root-logger takeover in capture_task_output"
```

---

## Task 5: Add the `task_with_log_levels` fixture

**Files:**
- Modify: `aaiclick/orchestration/fixtures/sample_tasks.py:1-35`

- [ ] **Step 1: Add the fixture task**

In `aaiclick/orchestration/fixtures/sample_tasks.py`, add `import logging` to the import block at the top (it currently imports `sys` and `from pathlib import Path`), and add this function after `task_with_output` (after line 35):

```python
def task_with_log_levels():
    """Emit one logging record at each level so the UI can be checked for coloring."""
    log = logging.getLogger("sample")
    log.info("info line")
    log.warning("warning line")
    log.error("error line")
```

- [ ] **Step 2: Verify it imports and is callable**

Run: `uv run python -c "from aaiclick.orchestration.fixtures.sample_tasks import task_with_log_levels; task_with_log_levels()"`
Expected: prints the three log lines to stderr (default logging output), exits 0.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/fixtures/sample_tasks.py
git commit -m "test: add task_with_log_levels fixture"
```

---

## Task 6: Frontend — regenerate types, color by level, timestamp toggle

**Files:**
- Regenerate: `src/api/schema.ts`
- Modify: `src/components/LogViewer.tsx`
- Modify: `src/styles/globals.css:209-214`

- [ ] **Step 1: Regenerate the OpenAPI types**

Run: `npm run gen-types`
Expected: `src/api/schema.ts` `LogLine` block now includes `level` (enum of the five names) and `created_at: string`. Verify:

Run: `grep -n -A8 "LogLine: {" src/api/schema.ts`
Expected output contains `level:` with the five-name enum and `created_at`.

- [ ] **Step 2: Update the CSS to cover every level and drop the stderr modifier**

In `src/styles/globals.css`, replace the block at lines 210-214:

```css
.logs .lvl-INFO { color: #79c0ff; }
.logs .lvl-WARN { color: #e3b341; }
.logs .lvl-ERROR { color: #ff7b72; }
.logs .log-line { white-space: pre-wrap; }
.logs .log-stderr { color: #ff7b72; }
```

with (rename `WARN`→`WARNING`, add `DEBUG`/`CRITICAL`, drop the now-unused `.log-stderr`):

```css
.logs .lvl-DEBUG { color: #6b7480; }
.logs .lvl-INFO { color: #79c0ff; }
.logs .lvl-WARNING { color: #e3b341; }
.logs .lvl-ERROR { color: #ff7b72; }
.logs .lvl-CRITICAL { color: #ff7b72; font-weight: 600; }
.logs .log-line { white-space: pre-wrap; }
```

(`.logs .ts` at line 209 stays — it already styles the timestamp prefix muted.)

- [ ] **Step 3: Update `LogViewer.tsx`**

Replace the entire contents of `src/components/LogViewer.tsx` with:

```tsx
import { memo, useState } from "react";
import type { LogLine } from "../api/types";
import { useTaskLogs } from "../api/hooks";

// Render a captured created_at (ISO string) as HH:MM:SS.mmm for the inline
// timestamp prefix. Kept tiny and dependency-free; the value is informational.
function fmtTs(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  return d.toISOString().slice(11, 23);
}

// `lines` typically grows by appending; memoising on the array identity (plus
// the timestamp flag) skips the per-line VDOM rebuild when a poll returns the
// same payload. Each line carries a per-level class so the viewer colors by
// severity; raw stdout/stderr arrive as INFO/ERROR so they color too.
const LogLines = memo(function LogLines({
  lines,
  showTimestamps,
}: {
  lines: readonly LogLine[];
  showTimestamps: boolean;
}) {
  return (
    <>
      {lines.map((line, i) => (
        <div key={i} data-testid={`log-line-${line.level}`} className={`log-line lvl-${line.level}`}>
          {showTimestamps && <span className="ts">{fmtTs(line.created_at)} </span>}
          {line.text}
        </div>
      ))}
    </>
  );
});

export function LogViewer({ taskId }: { taskId: string }) {
  const { data, isLoading, isError } = useTaskLogs(taskId);
  const [showTimestamps, setShowTimestamps] = useState(false);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  const lines = data?.lines ?? [];
  if (!data || !data.available || lines.length === 0) {
    return <div className="logs">(no logs captured for this task)</div>;
  }
  return (
    <div className="logs">
      <label className="logs-toolbar">
        <input
          type="checkbox"
          checked={showTimestamps}
          onChange={(e) => setShowTimestamps(e.target.checked)}
        />
        Show timestamps
      </label>
      <LogLines lines={lines} showTimestamps={showTimestamps} />
    </div>
  );
}
```

> The `<label>` wraps the checkbox and the text "Show timestamps", so Playwright's `get_by_label("Show timestamps")` resolves to the checkbox.

- [ ] **Step 4: Type-check the SPA**

Run: `npm run check`
Expected: no TypeScript errors.

- [ ] **Step 5: Build the SPA (needed for e2e)**

Run: `npm run build`
Expected: build succeeds; `aaiclick/server/static/index.html` is produced.

- [ ] **Step 6: Commit**

```bash
git add src/api/schema.ts src/components/LogViewer.tsx src/styles/globals.css
git commit -m "feat: color task logs by level and add opt-in timestamp toggle"
```

---

## Task 7: UI e2e test for level coloring + timestamp toggle

**Files:**
- Modify: `test_e2e/web/test_smoke.py` (add a helper + a test)

- [ ] **Step 1: Write the test**

In `test_e2e/web/test_smoke.py`, add a run helper (mirrors `_run_logging_task` but runs the new fixture) and the test. Add after `test_task_view_shows_logs` (end of file):

```python
def _run_log_levels_task(page, base_url: str) -> str:
    """Run task_with_log_levels and return the completed task id."""
    api = f"{base_url}/api/v0"
    resp = page.request.post(
        f"{api}/jobs:run",
        data={"name": "aaiclick.orchestration.fixtures.sample_tasks.task_with_log_levels"},
    )
    assert resp.ok, resp.text()
    job_id = resp.json()["id"]

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        detail = page.request.get(f"{api}/jobs/{job_id}").json()
        tasks = detail.get("tasks") or []
        if tasks and tasks[0]["status"] == "COMPLETED":
            return tasks[0]["id"]
        time.sleep(0.5)
    raise AssertionError("task did not reach COMPLETED within 30 s")


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
@pytest.mark.skipif(
    not is_local(),
    reason="needs auth-off + an in-process worker (local_runtime), both local-mode only; "
    "the distributed e2e job enforces auth and runs no worker",
)
def test_task_view_colors_logs_by_level(page, base_url: str) -> None:
    """The task view colors lines by level and shows timestamps only when toggled."""
    task_id = _run_log_levels_task(page, base_url)

    page.goto(f"{base_url}/?p=@task {task_id}")
    page.wait_for_selector("#root")
    _login_if_needed(page)

    logs = page.locator("div.logs")
    logs.get_by_test_id("log-line-ERROR").get_by_text("error line").wait_for(timeout=15000)
    logs.get_by_test_id("log-line-WARNING").get_by_text("warning line").wait_for(timeout=15000)

    # Severity color is actually applied (CSS class resolves to a non-empty color).
    error_color = logs.locator(".lvl-ERROR").first.evaluate("el => getComputedStyle(el).color")
    assert error_color

    # Timestamp is opt-in: hidden by default, shown after toggling.
    assert logs.locator(".ts").count() == 0
    page.get_by_label("Show timestamps").check()
    logs.locator(".ts").first.wait_for(timeout=5000)
```

- [ ] **Step 2: Run the e2e test (local mode)**

Run: `uv run pytest test_e2e/web/test_smoke.py::test_task_view_colors_logs_by_level -v -p no:cov`
Expected: PASS (requires the SPA build from Task 6 Step 5 and Playwright installed; otherwise it SKIPs with a recorded reason).

- [ ] **Step 3: Commit**

```bash
git add test_e2e/web/test_smoke.py
git commit -m "test: e2e for log level coloring and timestamp toggle"
```

---

## Task 8: Documentation

**Files:**
- Modify: `docs/orchestration.md:372-380`
- Modify: `docs/ui.md:113`

- [ ] **Step 1: Update the orchestration doc**

In `docs/orchestration.md`, replace the "Cross-host logs" paragraph (lines 372-380) so it covers level and per-line timestamp:

```markdown
**Cross-host logs**: `capture_task_output` tees task stdout/stderr to the local
file *and* streams it into the ClickHouse `task_logs` table from inside the task
process. It also installs a `logging` handler (taking over the root logger for
the task) so each `logging.*` record is captured with its true `level`; raw
`print()` output defaults to `INFO` (stdout) / `ERROR` (stderr), and
`AAICLICK_LOG_LEVEL` sets the captured root level (default `INFO`). Every row is
tagged with its `stream` (`stdout`/`stderr`), its `level`, and a per-line
`created_at` (emit time, not flush time) so the UI can color by severity and
optionally show timestamps. Because every runner (subprocess, docker,
kubernetes) shares that path, `get_task_logs` reads one host-independent source
regardless of where the task ran — `aaiclick/orchestration/logging.py`,
`aaiclick/oplog/models.py`. The rows are job-scoped: the background worker's
`_delete_job_data` drops a job's `task_logs` alongside its `operation_log` on TTL
expiry, so logs share the job's retention lifecycle.
```

- [ ] **Step 2: Update the UI doc**

In `docs/ui.md`, replace the "Main section" log-viewer sentence (line 113) end so it mentions coloring + toggle. Change the existing line to add, after "…captured no output.":

```markdown
Lines are colored by `level` (`lvl-*` classes) and an opt-in "Show timestamps" toggle reveals each line's `created_at`.
```

- [ ] **Step 3: Apply doc style skills**

Invoke the `markdown-style` skill, then the `shortify` skill, on the two edited docs. Apply any fixes they surface.

- [ ] **Step 4: Commit**

```bash
git add docs/orchestration.md docs/ui.md
git commit -m "docs: document task log level + per-line timestamp"
```

---

## Task 9: Full verification

- [ ] **Step 1: Run the backend test suites touched by this change**

Run: `uv run pytest aaiclick/test_view_models.py aaiclick/orchestration/test_logging.py aaiclick/internal_api/test_tasks_logs.py -v`
Expected: all PASS.

- [ ] **Step 2: Run the SPA type-check**

Run: `npm run check`
Expected: no errors.

- [ ] **Step 3: Run the e2e smoke suite (local)**

Run: `uv run pytest test_e2e/web/test_smoke.py -v -p no:cov`
Expected: PASS or SKIP (never FAIL).

- [ ] **Step 4: Verify GitHub Actions**

Push the branch and use the `check-pr` skill to confirm the `test-ui-e2e-dist` job and the unit-test jobs are green. Fix any failures.
