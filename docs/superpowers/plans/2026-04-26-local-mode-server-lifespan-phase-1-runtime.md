# Phase 1 — `local_runtime()` helper

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this phase task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a self-contained `local_runtime()` async context manager that starts and stops the BackgroundWorker + execution `worker_main_loop` for the duration of its block. This phase introduces the helper and a single mode-guard test. End-to-end behaviour is covered in Phase 2 via the lifespan smoke test (where the existing `app_client` fixture provides the right infrastructure).

**Spec:** `docs/superpowers/specs/2026-04-26-local-mode-server-lifespan-design.md` — §"`local_runtime()` semantics".

**Branch:** `claude/fastapi-lifespan-worker-X8KZm` (existing).

**Parent plan:** `2026-04-26-local-mode-server-lifespan.md`.

---

## Task 1: Create the `local_runtime()` module

**Files:**
- Create: `aaiclick/orchestration/local_runtime.py`

- [ ] **Step 1: Write the module**

Create `aaiclick/orchestration/local_runtime.py` with the following content:

```python
"""Local-mode worker startup/shutdown helper.

Used by the FastAPI lifespan in ``aaiclick.server.app`` to start the
``BackgroundWorker`` and the execution ``worker_main_loop`` for the
duration of a single local-mode (chdb + sqlite) server process.

The helper is strict: it raises ``RuntimeError`` if called outside
local mode. Distributed-mode callers run ``worker start`` and
``background start`` as separate processes instead.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from aaiclick.backend import is_local
from aaiclick.cli_renderers import render_setup_result
from aaiclick.internal_api.setup import is_setup_done, setup

from .background import BackgroundWorker
from .execution import worker_main_loop
from .orch_context import orch_context


@asynccontextmanager
async def local_runtime() -> AsyncIterator[None]:
    """Run BackgroundWorker + execution worker for the duration of the block.

    Local mode only — raises ``RuntimeError`` if ``is_local()`` is False.
    Auto-runs ``setup()`` on first use. The execution worker runs as a
    background ``asyncio.Task`` and is cancelled on shutdown.
    """
    if not is_local():
        raise RuntimeError(
            "local_runtime() requires local mode (chdb + sqlite). "
            "In distributed mode, run `worker start` and `background start` "
            "as separate processes."
        )
    if not is_setup_done():
        render_setup_result(setup())

    background = BackgroundWorker()
    await background.start()
    try:
        async with orch_context(with_ch=True):
            worker_task = asyncio.create_task(
                worker_main_loop(install_signal_handlers=False)
            )
            try:
                yield
            finally:
                worker_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await worker_task
    finally:
        await background.stop()
```

- [ ] **Step 2: Verify imports resolve**

Run: `uv run --extra server --extra test python -c "from aaiclick.orchestration.local_runtime import local_runtime; print(local_runtime)"`
Expected: prints the function object — no `ImportError`.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/local_runtime.py
git commit -m "$(cat <<'EOF'
feature: add local_runtime() async context manager

Wraps BackgroundWorker.start/stop and a foreground asyncio task running
worker_main_loop, scoped to a single async context. Raises if called
outside local mode. Auto-runs setup() on first use.

Unused at runtime in this phase — Phase 2 wires it into the FastAPI
lifespan; Phase 2's lifespan smoke test exercises the helper end to end.
EOF
)"
```

---

## Task 2: Mode-guard test

**Files:**
- Create: `aaiclick/orchestration/test_local_runtime.py`

End-to-end coverage (worker registration, job completion, in-flight cancellation) lives in Phase 2 — the `aaiclick/server/test_app.py` `app_client` fixture is the right infrastructure for it. This phase only verifies the cheap, infra-free guard.

- [ ] **Step 1: Write the failing test**

Create `aaiclick/orchestration/test_local_runtime.py`:

```python
"""Tests for local_runtime() — the lifespan-shared worker helper.

End-to-end coverage (worker startup, job completion, shutdown
cancellation) lives in aaiclick/server/test_app.py — that file's
app_client fixture already enters the FastAPI lifespan, which is the
helper's only production caller.
"""

from __future__ import annotations

import pytest

from .local_runtime import local_runtime


async def test_local_runtime_rejects_distributed_mode(monkeypatch):
    """Outside local mode the helper raises before touching any resource."""
    monkeypatch.setattr(
        "aaiclick.orchestration.local_runtime.is_local",
        lambda: False,
    )
    with pytest.raises(RuntimeError, match="requires local mode"):
        async with local_runtime():
            pytest.fail("local_runtime() should have raised before yielding")
```

- [ ] **Step 2: Run the test**

Run: `uv run --extra server --extra test pytest aaiclick/orchestration/test_local_runtime.py -v`
Expected: PASS. The mode guard short-circuits before any I/O, so this test does not need an outer `orch_ctx` and runs identically in local and distributed CI legs.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/test_local_runtime.py
git commit -m "test: local_runtime() rejects distributed mode"
```

---

## Task 3: Full suite, lint, push, CI, simplify

- [ ] **Step 1: Run the orchestration suite (local mode)**

Run: `AAICLICK_SQL_URL='' AAICLICK_CH_URL='' uv run --extra server --extra test pytest aaiclick/orchestration/ -v`
Expected: all tests pass. The new module is unused at runtime, so existing tests should be unaffected.

- [ ] **Step 2: Lint and type-check**

Run: `uv run --extra server --extra test ruff check aaiclick/orchestration/local_runtime.py aaiclick/orchestration/test_local_runtime.py`
Run: `uv run --extra server --extra test pyright aaiclick/orchestration/local_runtime.py aaiclick/orchestration/test_local_runtime.py`
Expected: no errors.

- [ ] **Step 3: Push the branch**

```bash
git push origin claude/fastapi-lifespan-worker-X8KZm
```

- [ ] **Step 4: Run `/check-pr`**

```
/check-pr 262
```

Wait for CI to complete. If failures appear, read the failed run's logs (`gh run view <RUN_ID> --log-failed`), fix locally, commit, push, and re-run `/check-pr` until CI is green across all matrix legs.

- [ ] **Step 5: Run `/simplify`**

```
/simplify aaiclick/orchestration/local_runtime.py aaiclick/orchestration/test_local_runtime.py
```

Apply suggestions that improve quality without expanding scope. Commit any changes with `cleanup: <what>` and push. Re-run `/check-pr`.

- [ ] **Step 6: Confirm Phase 1 complete**

Phase 1 is complete when:
- `aaiclick/orchestration/local_runtime.py` exists and exports `local_runtime`.
- `aaiclick/orchestration/test_local_runtime.py` covers the mode-guard path.
- CI is green on PR #262.
- `/simplify` has produced no further actionable suggestions (or they have been applied).

Move on to Phase 2: `2026-04-26-local-mode-server-lifespan-phase-2-lifespan.md`.
