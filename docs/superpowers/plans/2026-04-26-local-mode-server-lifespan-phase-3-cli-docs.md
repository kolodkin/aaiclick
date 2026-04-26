# Phase 3 — CLI rewrite + docs updates

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this phase task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rewrite `aaiclick/orchestration/cli.py::start_local()` to launch uvicorn against the combined ASGI app, drop the `--max-tasks` flag from `local start`, and update docs to reflect the new behaviour. After this phase, `python -m aaiclick local start` runs the combined REST + MCP server with workers — all in one process, all in local mode.

**Spec:** `docs/superpowers/specs/2026-04-26-local-mode-server-lifespan-design.md` — §"CLI changes".

**Branch:** `claude/fastapi-lifespan-worker-X8KZm` (existing).

**Parent plan:** `2026-04-26-local-mode-server-lifespan.md`.

**Prerequisite:** Phase 2 complete (`uvicorn aaiclick.server.app:app` works in local mode).

---

## Task 1: Rewrite `start_local()`

**Files:**
- Modify: `aaiclick/orchestration/cli.py`

- [ ] **Step 1: Replace `start_local()`**

Open `aaiclick/orchestration/cli.py`. Locate the existing `start_local()` definition (around line 49). Replace the entire function with:

```python
async def start_local(host: str = "127.0.0.1", port: int = 8000) -> None:
    """Run the combined REST + MCP server with workers in a single local-mode process.

    Local mode only — chdb's file lock requires a single process. The
    server's lifespan starts the BackgroundWorker and the execution
    worker via local_runtime(). uvicorn handles SIGTERM / SIGINT.
    """
    if not is_local():
        raise RuntimeError(
            "'local start' requires local mode (chdb + SQLite). "
            "Use `worker start` + `background start` + "
            "`uvicorn aaiclick.server.app:app` in distributed mode."
        )

    try:
        from aaiclick.server.app import app
    except ImportError as exc:
        raise ImportError(
            "`local start` requires the [server] extra. Install it with "
            "`pip install 'aaiclick[server]'` (or `uv add 'aaiclick[server]'`)."
        ) from exc

    import uvicorn

    config = uvicorn.Config(app, host=host, port=port, log_level="info")
    await uvicorn.Server(config).serve()
```

Keep the rest of the file untouched: `start_worker()` and `start_background()` are unchanged. The top-of-file imports `is_setup_done`, `setup`, `render_setup_result`, `BackgroundWorker`, `mp_worker_main_loop`, and `worker_main_loop` are still needed by `start_worker()`; do not remove them.

- [ ] **Step 2: Verify the module imports**

Run: `uv run --extra server --extra test python -c "from aaiclick.orchestration.cli import start_local; print(start_local)"`
Expected: prints the function object.

- [ ] **Step 3: Commit**

```bash
git add aaiclick/orchestration/cli.py
git commit -m "$(cat <<'EOF'
feature: start_local launches uvicorn against combined REST + MCP app

Replaces the previous worker-only loop with a uvicorn launcher
against `aaiclick.server.app:app`. The server's lifespan (Phase 2)
runs the workers via local_runtime(); this CLI verb is now a thin
wrapper that drives the process lifecycle.

Setup auto-runs inside local_runtime(); the lazy import of
aaiclick.server.app emits a friendly error if the [server] extra is
missing.
EOF
)"
```

---

## Task 2: Drop `--max-tasks` from `local start` argparse

**Files:**
- Modify: `aaiclick/__main__.py`

- [ ] **Step 1: Update the argparse subcommand**

Open `aaiclick/__main__.py`. Locate the `local start` subcommand (around lines 330-340). Replace the block:

```python
    # local start
    local_start_parser = local_subparsers.add_parser(
        "start",
        help="Start worker + background in a single process",
    )
    local_start_parser.add_argument(
        "--max-tasks",
        type=int,
        default=None,
        help="Maximum tasks to execute (default: unlimited)",
    )
```

With:

```python
    # local start
    local_start_parser = local_subparsers.add_parser(
        "start",
        help="Start the combined REST + MCP server with workers (local mode)",
    )
    local_start_parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Host to bind (default: 127.0.0.1)",
    )
    local_start_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind (default: 8000)",
    )
```

- [ ] **Step 2: Update the dispatch site**

Locate the dispatch around line 685:

```python
    elif args.command == "local":
        if args.local_command == "start":
            from aaiclick.orchestration.cli import start_local

            asyncio.run(start_local(max_tasks=args.max_tasks))
```

Replace with:

```python
    elif args.command == "local":
        if args.local_command == "start":
            from aaiclick.orchestration.cli import start_local

            asyncio.run(start_local(host=args.host, port=args.port))
```

- [ ] **Step 3: Update the module docstring usage block**

Open the `python -m aaiclick local start` line in the docstring at the top of `aaiclick/__main__.py` (around line 8). Replace:

```
    python -m aaiclick local start              # Start worker + background (local mode)
```

With:

```
    python -m aaiclick local start              # Start REST + MCP server with workers (local mode)
```

- [ ] **Step 4: Smoke-test the CLI parser**

Run: `uv run --extra server python -m aaiclick local start --help`
Expected: shows the new help text including `--host` and `--port`. Does not include `--max-tasks`.

Run: `uv run --extra server python -m aaiclick local start --max-tasks 5 2>&1 | head -2`
Expected: argparse error mentioning unrecognized argument.

- [ ] **Step 5: Commit**

```bash
git add aaiclick/__main__.py
git commit -m "$(cat <<'EOF'
cleanup: drop --max-tasks from `local start`, expose --host / --port

`local start` now drives uvicorn rather than a bounded worker loop.
The previous --max-tasks flag was a CLI-only test affordance; with
uvicorn owning process lifecycle the flag no longer maps to a real
parameter. --host / --port replace it with the controls a server
operator actually needs.
EOF
)"
```

---

## Task 3: Update `docs/api_server.md`

**Files:**
- Modify: `docs/api_server.md`

- [ ] **Step 1: Inspect the current "Running the server" section**

Run: `grep -n "^# Running the server\|^# Configuration\|^## Spawning workers" docs/api_server.md`

- [ ] **Step 2: Update the "Running the server" section**

Replace the "Running the server" section (the `bash` block plus surrounding paragraphs) with:

```markdown
# Running the server

The app is exposed as a module-level `app = FastAPI(...)` in
`aaiclick/server/app.py`. Run it directly with uvicorn:

```bash
pip install 'aaiclick[server]'
uvicorn aaiclick.server.app:app
# dev:
uvicorn aaiclick.server.app:app --reload
```

In **local mode** (`chdb` + `sqlite`), the lifespan automatically runs
the BackgroundWorker and the execution worker alongside the HTTP
server — submitting a job via the REST or MCP surface picks it up and
runs it in the same process. There is no separate worker process to
launch.

For convenience, the CLI exposes the same flow:

```bash
python -m aaiclick local start            # workers + REST + MCP on 127.0.0.1:8000
python -m aaiclick local start --port 9000
```

In **distributed mode** (PostgreSQL + ClickHouse), the lifespan is a
no-op and the worker / background processes run separately:

```bash
uvicorn aaiclick.server.app:app           # serves REST + MCP
python -m aaiclick worker start           # one or more worker processes
python -m aaiclick background start       # one cleanup process
```

Host, port, workers, reload, TLS, etc. are uvicorn's standard flags
and env vars (`UVICORN_HOST`, `UVICORN_PORT`, …); aaiclick does not
invent a parallel `AAICLICK_SERVER_*` namespace.
```

- [ ] **Step 3: Search for stale references to `local start`'s old behaviour**

Run: `grep -nE "local start.*worker|local start.*max-tasks|max-tasks.*local" docs/`
Expected: no matches. If matches appear, update them to reflect the new behaviour.

- [ ] **Step 4: Run the docs builder**

Run: `uv run --extra docs mkdocs build --strict 2>&1 | tail -20`
Expected: build succeeds. Fix any broken links or missing references.

- [ ] **Step 5: Commit**

```bash
git add docs/api_server.md
git commit -m "docs: api_server — describe new lifespan behaviour and `local start`"
```

---

## Task 4: Hand-test the full flow

- [ ] **Step 1: Boot the server in local mode**

In a scratch terminal, run:

```bash
AAICLICK_LOCAL_ROOT=$(mktemp -d) uv run --extra server python -m aaiclick local start --port 18000 &
SERVER_PID=$!
sleep 5
```

Expected: server logs include `Uvicorn running on http://127.0.0.1:18000`. Worker registration log lines should appear (`Worker <id> registered ...`).

- [ ] **Step 2: Hit the health endpoint and list workers**

```bash
curl -s http://127.0.0.1:18000/health
curl -s http://127.0.0.1:18000/api/v0/workers | head -c 400
```

Expected: `{"status":"ok"}` then a JSON page including at least one ACTIVE worker.

- [ ] **Step 3: Submit a sample job and watch it complete**

```bash
curl -s -X POST http://127.0.0.1:18000/api/v0/jobs:run \
  -H 'Content-Type: application/json' \
  -d '{"name":"hand_test_job","kwargs":{},"preservation_mode":null}'
sleep 3
curl -s http://127.0.0.1:18000/api/v0/jobs | head -c 400
```

Expected: the new job transitions to `COMPLETED` (or whatever terminal status the sample task produces) within a couple of seconds, observable in the listing.

If the entrypoint is not registered, swap in a registered name from `aaiclick.orchestration.fixtures.sample_tasks` (e.g. `simple_task`) using the appropriate `register-job` flow first.

- [ ] **Step 4: Tear down**

```bash
kill $SERVER_PID
wait $SERVER_PID 2>/dev/null
```

Expected: clean shutdown logs (`Worker <id> stopped`, FastMCP teardown, uvicorn exits).

- [ ] **Step 5: Capture observations**

If anything looked off (slow shutdown, stuck task, unexpected log noise), open a follow-up issue and reference it from the PR description. Do not block the merge on cosmetic log-noise.

---

## Task 5: Push, CI, simplify, finalise

- [ ] **Step 1: Run the full local test suite**

Run: `AAICLICK_SQL_URL='' AAICLICK_CH_URL='' uv run --extra server --extra test pytest aaiclick/ -v`
Expected: all tests pass.

- [ ] **Step 2: Run lint**

Run: `uv run --extra server --extra test ruff check aaiclick/ docs/`
Run: `uv run --extra server --extra test pyright aaiclick/orchestration/cli.py aaiclick/__main__.py`
Expected: no errors.

- [ ] **Step 3: Push the branch**

```bash
git push origin claude/fastapi-lifespan-worker-X8KZm
```

- [ ] **Step 4: Run `/check-pr`**

```
/check-pr 262
```

Wait for CI. Address any failures by editing locally, committing, and re-pushing.

- [ ] **Step 5: Run `/simplify`**

```
/simplify aaiclick/orchestration/cli.py aaiclick/__main__.py docs/api_server.md
```

Apply useful suggestions that stay within scope. Commit and push.

- [ ] **Step 6: Update the PR description**

Edit PR #262 description to:
- Strike through the spec-only summary; add a brief implementation summary.
- Tick the checkboxes in the test plan that this implementation now satisfies.

- [ ] **Step 7: Confirm Phase 3 complete**

Phase 3 is complete when:
- `python -m aaiclick local start` boots the combined server.
- The PR description reflects the implemented behaviour.
- CI is green on PR #262.
- `/simplify` has produced no further actionable suggestions.

The implementation is complete. The PR is ready for review and merge.
