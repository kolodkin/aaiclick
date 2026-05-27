# Operator UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the prompt-driven operator dashboard as a React SPA wired to the live REST API, served by FastAPI, with REST-polling refresh and the full mockup feature set.

**Architecture:** Three small backend additions (task-logs endpoint, `JobView` task counts, SPA static mount) plus a React 19 + Vite + Tailwind SPA at the repo root. The prompt string is the only app state and syncs to a URL query param; TanStack Query owns server state with `refetchInterval` polling. The visual design ports `docs/ui/index.html` 1:1.

**Tech Stack:** Backend — FastAPI, SQLModel, pytest + httpx. Frontend — React 19, TypeScript, Vite 6, TailwindCSS 4, TanStack Query 5; `tsc --noEmit` is the type gate, Playwright (Python) for one e2e smoke.

**Spec:** `docs/superpowers/specs/2026-05-27-operator-ui-design.md`. UX: `docs/ui.md`. Frontend architecture: `docs/frontend.md`. Visual reference: `docs/ui/index.html`.

**Frontend "test" convention:** Vitest is deferred (see spec). For frontend tasks the verification step is `npm run check` (`tsc --noEmit`) passing, plus the Playwright smoke test in the final task. Backend tasks use real pytest TDD.

---

# Phase 1 — Backend additions

## Task 1: `TaskLogsView` model

**Files:**
- Modify: `aaiclick/orchestration/view_models.py` (add `TaskLogsView`)
- Test: `aaiclick/orchestration/test_view_models.py` (create if absent)

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/orchestration/test_view_models.py` (create the file with this content if it does not exist):

```python
from __future__ import annotations

from aaiclick.orchestration.view_models import TaskLogsView


def test_task_logs_view_defaults():
    view = TaskLogsView(available=False, log_path=None)
    assert view.lines == []
    assert view.available is False
    assert view.log_path is None


def test_task_logs_view_with_lines():
    view = TaskLogsView(available=True, log_path="/tmp/x.log", lines=["a", "b"])
    assert view.lines == ["a", "b"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/orchestration/test_view_models.py -v -p no:cov`
Expected: FAIL — `ImportError: cannot import name 'TaskLogsView'`.

- [ ] **Step 3: Add the model**

In `aaiclick/orchestration/view_models.py`, after the `TaskDetail` class, add:

```python
class TaskLogsView(BaseModel):
    """Captured log lines for a task, served by ``GET /tasks/{id}/logs``."""

    available: bool
    log_path: str | None = None
    lines: list[str] = Field(default_factory=list)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/orchestration/test_view_models.py -v -p no:cov`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/orchestration/view_models.py aaiclick/orchestration/test_view_models.py
git commit -m "feat(server): add TaskLogsView model"
```

## Task 2: `get_task_logs` internal API

**Files:**
- Modify: `aaiclick/internal_api/tasks.py`
- Test: `aaiclick/internal_api/test_tasks_logs.py` (create)

- [ ] **Step 1: Write the failing test**

Create `aaiclick/internal_api/test_tasks_logs.py`:

```python
from __future__ import annotations

import pytest
from sqlmodel import select

from aaiclick.internal_api.errors import NotFound
from aaiclick.internal_api.tasks import get_task_logs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import Task
from aaiclick.orchestration.orch_context import get_sql_session


async def _set_log_path(task_id: int, path: str | None) -> None:
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.id == task_id))).scalar_one()
        task.log_path = path
        s.add(task)
        await s.commit()


async def test_logs_unavailable_when_log_path_none(orch_ctx):
    job = await create_job("logs_none", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_read_from_file(orch_ctx, tmp_path):
    job = await create_job("logs_file", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    log_file = tmp_path / "task.log"
    log_file.write_text("line one\nline two\n")
    await _set_log_path(task.id, str(log_file))

    result = await get_task_logs(task.id)

    assert result.available is True
    assert result.log_path == str(log_file)
    assert result.lines == ["line one", "line two"]


async def test_logs_unavailable_when_file_missing(orch_ctx, tmp_path):
    job = await create_job("logs_missing", simple_task)
    task = (await get_tasks_for_job(job.id))[0]
    await _set_log_path(task.id, str(tmp_path / "does_not_exist.log"))

    result = await get_task_logs(task.id)

    assert result.available is False
    assert result.lines == []


async def test_logs_not_found_raises(orch_ctx):
    with pytest.raises(NotFound):
        await get_task_logs(999999999)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/internal_api/test_tasks_logs.py -v -p no:cov`
Expected: FAIL — `ImportError: cannot import name 'get_task_logs'`.

- [ ] **Step 3: Implement `get_task_logs`**

Replace the contents of `aaiclick/internal_api/tasks.py` with:

```python
"""Internal API for task commands.

Each function runs inside an active ``orch_context()`` and reads the SQL
session via the contextvar getter. Returns pydantic view models.
"""

from __future__ import annotations

import os

from aaiclick.orchestration.jobs.queries import get_task as _get_task_impl
from aaiclick.orchestration.view_models import TaskDetail, TaskLogsView, task_to_detail

from .errors import NotFound


async def get_task(task_id: int) -> TaskDetail:
    """Return full task detail by numeric ID.

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _get_task_impl(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")
    return task_to_detail(task)


async def get_task_logs(task_id: int) -> TaskLogsView:
    """Return captured log lines for a task.

    Reads the file at ``task.log_path``. Returns ``available=False`` when the
    task has no log path or the file is not present on this process's
    filesystem (distributed / docker runs).

    Raises ``NotFound`` if no task matches ``task_id``.
    """
    task = await _get_task_impl(task_id)
    if task is None:
        raise NotFound(f"Task not found: {task_id}")

    log_path = task.log_path
    if not log_path or not os.path.isfile(log_path):
        return TaskLogsView(available=False, log_path=log_path)

    with open(log_path, encoding="utf-8", errors="replace") as f:
        lines = f.read().splitlines()
    return TaskLogsView(available=True, log_path=log_path, lines=lines)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/internal_api/test_tasks_logs.py -v -p no:cov`
Expected: PASS (4 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/internal_api/tasks.py aaiclick/internal_api/test_tasks_logs.py
git commit -m "feat(server): add get_task_logs internal API"
```

## Task 3: `GET /tasks/{id}/logs` route

**Files:**
- Modify: `aaiclick/server/routers/tasks.py`
- Test: `aaiclick/server/routers/test_tasks.py`

- [ ] **Step 1: Write the failing test**

Append to `aaiclick/server/routers/test_tasks.py`:

```python
from aaiclick.orchestration.view_models import TaskLogsView


async def test_get_task_logs(orch_ctx, app_client):
    job = await create_job("logs_route_job", simple_task)
    task = (await get_tasks_for_job(job.id))[0]

    response = await app_client.get(f"{API_PREFIX}/tasks/{task.id}/logs")

    assert response.status_code == 200
    logs = TaskLogsView.model_validate(response.json())
    assert logs.available is False


async def test_get_task_logs_not_found_returns_404(orch_ctx, app_client):
    response = await app_client.get(f"{API_PREFIX}/tasks/999999999/logs")

    assert response.status_code == 404
    problem = Problem.model_validate(response.json())
    assert problem.code is ProblemCode.NOT_FOUND
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/server/routers/test_tasks.py -v -p no:cov`
Expected: FAIL — 404 on the logs route is actually `404 Not Found` from FastAPI routing (route undefined), so `test_get_task_logs` fails at `status_code == 200`.

- [ ] **Step 3: Add the route**

Replace `aaiclick/server/routers/tasks.py` with:

```python
from __future__ import annotations

from fastapi import APIRouter, Depends

from aaiclick.internal_api import tasks as tasks_api
from aaiclick.orchestration.view_models import TaskDetail, TaskLogsView

from ..deps import orch_scope
from ..errors import problem_responses

router = APIRouter(prefix="/tasks", tags=["tasks"], dependencies=[Depends(orch_scope)])


@router.get("/{task_id}", response_model=TaskDetail, responses=problem_responses(404))
async def get_task(task_id: int) -> TaskDetail:
    return await tasks_api.get_task(task_id)


@router.get("/{task_id}/logs", response_model=TaskLogsView, responses=problem_responses(404))
async def get_task_logs(task_id: int) -> TaskLogsView:
    return await tasks_api.get_task_logs(task_id)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/server/routers/test_tasks.py -v -p no:cov`
Expected: PASS (all tests in file).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/routers/tasks.py aaiclick/server/routers/test_tasks.py
git commit -m "feat(server): add GET /tasks/{id}/logs route"
```

## Task 4: Task counts on `JobView`

**Files:**
- Modify: `aaiclick/orchestration/view_models.py` (`JobView`, `job_to_view`, `job_to_detail`)
- Modify: `aaiclick/internal_api/jobs.py` (`list_jobs`)
- Test: `aaiclick/internal_api/test_jobs_counts.py` (create)

- [ ] **Step 1: Write the failing test**

Create `aaiclick/internal_api/test_jobs_counts.py`:

```python
from __future__ import annotations

from sqlmodel import select

from aaiclick.internal_api.jobs import list_jobs
from aaiclick.orchestration.factories import create_job
from aaiclick.orchestration.fixtures.sample_tasks import simple_task
from aaiclick.orchestration.jobs.queries import get_tasks_for_job
from aaiclick.orchestration.models import TASK_COMPLETED, Task
from aaiclick.orchestration.orch_context import get_sql_session


async def _mark_completed(task_id: int) -> None:
    async with get_sql_session() as s:
        task = (await s.execute(select(Task).where(Task.id == task_id))).scalar_one()
        task.status = TASK_COMPLETED
        s.add(task)
        await s.commit()


async def test_list_jobs_reports_task_counts(orch_ctx):
    job = await create_job("counts_job", simple_task)
    tasks = await get_tasks_for_job(job.id)
    await _mark_completed(tasks[0].id)

    page = await list_jobs()
    view = next(j for j in page.items if j.id == job.id)

    assert view.total_tasks == len(tasks)
    assert view.completed_tasks == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/internal_api/test_jobs_counts.py -v -p no:cov`
Expected: FAIL — `AttributeError: 'JobView' object has no attribute 'total_tasks'` (or validation default mismatch).

- [ ] **Step 3a: Add fields to `JobView`**

In `aaiclick/orchestration/view_models.py`, add two fields at the end of the `JobView` class body:

```python
    total_tasks: int = 0
    completed_tasks: int = 0
```

- [ ] **Step 3b: Accept counts in the adapters**

In `aaiclick/orchestration/view_models.py`, change `job_to_view` to accept optional counts:

```python
def job_to_view(job: Job, *, total_tasks: int = 0, completed_tasks: int = 0) -> JobView:
    return JobView(
        id=job.id,
        name=job.name,
        status=job.status,
        run_type=job.run_type,
        preservation_mode=job.preservation_mode,
        registered_job_id=job.registered_job_id,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        total_tasks=total_tasks,
        completed_tasks=completed_tasks,
    )
```

And update `job_to_detail` to populate counts from the tasks it already has. `TaskStatus` is a `Literal`, so `task.status` is a plain string — compare against `"COMPLETED"` directly. Replace the function with:

```python
def job_to_detail(job: Job, tasks: list[Task]) -> JobDetail:
    completed = sum(1 for t in tasks if t.status == "COMPLETED")
    return JobDetail(
        id=job.id,
        name=job.name,
        status=job.status,
        run_type=job.run_type,
        preservation_mode=job.preservation_mode,
        registered_job_id=job.registered_job_id,
        created_at=job.created_at,
        started_at=job.started_at,
        completed_at=job.completed_at,
        error=job.error,
        tasks=[task_to_view(t) for t in tasks],
        duration_ms=_ms_between(job.started_at, job.completed_at),
        total_tasks=len(tasks),
        completed_tasks=completed,
    )
```

- [ ] **Step 3c: Populate counts in `list_jobs`**

In `aaiclick/internal_api/jobs.py`, add this import to the external-packages group (alongside the existing `from sqlalchemy.ext.asyncio import AsyncSession`):

```python
from sqlalchemy import Integer, func as sa_func
```

And add `TASK_COMPLETED` to the existing models import so the line reads:

```python
from aaiclick.orchestration.models import Job, Task, TASK_COMPLETED
```

Then replace the body of `list_jobs` from the `page = await paginate(...)` call to the end of the function with:

```python
    page = await paginate(
        Job,
        where=predicates,
        order_by=col(Job.created_at).desc(),
        limit=filter.limit,
        offset=filter.offset,
    )

    job_ids = [j.id for j in page.rows]
    totals: dict[int, int] = {}
    completed: dict[int, int] = {}
    if job_ids:
        async with get_sql_session() as session:
            rows = (
                await session.execute(
                    select(
                        Task.job_id,
                        sa_func.count().label("total"),
                        sa_func.sum(sa_func.cast(Task.status == TASK_COMPLETED, Integer)).label("done"),
                    )
                    .where(col(Task.job_id).in_(job_ids))
                    .group_by(Task.job_id)
                )
            ).all()
        for job_id, total, done in rows:
            totals[job_id] = int(total)
            completed[job_id] = int(done or 0)

    return Page[JobView](
        items=[
            job_to_view(j, total_tasks=totals.get(j.id, 0), completed_tasks=completed.get(j.id, 0))
            for j in page.rows
        ],
        total=page.total,
    )
```

> The `cast(boolean, Integer)` sum is portable across SQLite and Postgres — avoid a backend-specific `CASE` or `FILTER`.

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/internal_api/test_jobs_counts.py aaiclick/orchestration/test_view_models.py -v -p no:cov`
Expected: PASS.

- [ ] **Step 5: Run the broader job/task suites for regressions**

Run: `pytest aaiclick/internal_api/ aaiclick/server/ -q -p no:cov`
Expected: PASS (no regressions from the `job_to_view` signature change).

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/view_models.py aaiclick/internal_api/jobs.py aaiclick/internal_api/test_jobs_counts.py
git commit -m "feat(server): report task counts on JobView list items"
```

## Task 5: Serve the SPA from FastAPI

**Files:**
- Modify: `aaiclick/server/app.py`
- Test: `aaiclick/server/test_static_mount.py` (create)

- [ ] **Step 1: Write the failing test**

Create `aaiclick/server/test_static_mount.py`:

```python
from __future__ import annotations

from aaiclick.server.app import STATIC_DIR, app


def test_static_dir_constant_points_into_server_package():
    assert STATIC_DIR.name == "static"
    assert STATIC_DIR.parent.name == "server"


def test_api_routes_still_registered():
    paths = {r.path for r in app.routes if hasattr(r, "path")}
    assert "/health" in paths
    assert any(p.startswith("/api/v0/jobs") for p in paths)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest aaiclick/server/test_static_mount.py -v -p no:cov`
Expected: FAIL — `ImportError: cannot import name 'STATIC_DIR'`.

- [ ] **Step 3: Mount the SPA**

In `aaiclick/server/app.py`, add imports near the top:

```python
from pathlib import Path

from fastapi.staticfiles import StaticFiles
```

Add the constant after `MCP_PATH`:

```python
STATIC_DIR = Path(__file__).parent / "static"
```

At the very end of the file (after the `health` endpoint), mount the SPA only when a build exists, so dev/test without a `npm run build` keep working:

```python
# SPA: serve the Vite build (gitignored, produced by `npm run build`). Mounted
# last so the API routers, /mcp, and /health keep priority. `html=True` serves
# index.html at "/" and resolves hashed asset paths under /assets/*.
if STATIC_DIR.is_dir():
    app.mount("/", StaticFiles(directory=STATIC_DIR, html=True), name="spa")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest aaiclick/server/test_static_mount.py -v -p no:cov`
Expected: PASS (2 passed).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/server/app.py aaiclick/server/test_static_mount.py
git commit -m "feat(server): mount the SPA build when present"
```

---

# Phase 2 — Frontend scaffolding

> All Phase 2+ files live at the repo root or under `src/`. After each task, run `npm run check` and verify it passes before committing.

## Task 6: Node project + tooling config

**Files:**
- Create: `package.json`, `vite.config.ts`, `tsconfig.json`, `tsconfig.node.json`, `index.html`
- Modify: `.gitignore`, `pyrightconfig.json`, `pyproject.toml`

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "aaiclick-ui",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "tsc -b && vite build",
    "check": "tsc --noEmit",
    "preview": "vite preview"
  },
  "dependencies": {
    "@tanstack/react-query": "^5.62.0",
    "react": "^19.0.0",
    "react-dom": "^19.0.0"
  },
  "devDependencies": {
    "@tailwindcss/vite": "^4.0.0",
    "@types/react": "^19.0.0",
    "@types/react-dom": "^19.0.0",
    "@vitejs/plugin-react": "^4.3.4",
    "tailwindcss": "^4.0.0",
    "typescript": "^5.7.2",
    "vite": "^6.0.0"
  }
}
```

- [ ] **Step 2: Create `vite.config.ts`**

```ts
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  build: {
    outDir: "aaiclick/server/static",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://localhost:8000",
      "/mcp": "http://localhost:8000",
    },
  },
});
```

- [ ] **Step 3: Create `tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "useDefineForClassFields": true,
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "noEmit": true,
    "jsx": "react-jsx",
    "strict": true,
    "noUnusedLocals": true,
    "noUnusedParameters": true,
    "noFallthroughCasesInSwitch": true
  },
  "include": ["src"],
  "references": [{ "path": "./tsconfig.node.json" }]
}
```

- [ ] **Step 4: Create `tsconfig.node.json`**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "lib": ["ES2023"],
    "module": "ESNext",
    "skipLibCheck": true,
    "moduleResolution": "bundler",
    "allowImportingTsExtensions": true,
    "isolatedModules": true,
    "noEmit": true
  },
  "include": ["vite.config.ts"]
}
```

- [ ] **Step 5: Create `index.html` (repo root)**

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>aaiclick</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 6: Update `.gitignore`**

Ensure these lines are present (append any that are missing):

```
node_modules/
aaiclick/server/static/
```

- [ ] **Step 7: Exclude frontend dirs from Python tooling**

In `pyrightconfig.json`, add `"src"` and `"node_modules"` to its `exclude` array (create the array if absent). In `pyproject.toml`, under `[tool.ruff]`, add:

```toml
extend-exclude = ["src", "node_modules"]
```

- [ ] **Step 8: Install and verify**

Run: `npm install && npm run check`
Expected: install succeeds; `tsc --noEmit` exits 0 (no `src/` files yet → no errors).

- [ ] **Step 9: Commit**

```bash
git add package.json package-lock.json vite.config.ts tsconfig.json tsconfig.node.json index.html .gitignore pyrightconfig.json pyproject.toml
git commit -m "chore(ui): scaffold Vite + React + Tailwind project"
```

## Task 7: Global theme stylesheet (ported from the mockup)

**Files:**
- Create: `src/styles/globals.css`

- [ ] **Step 1: Create `src/styles/globals.css`**

Start with the Tailwind import, then port the mockup's CSS verbatim. Copy the entire contents of the `<style>` block in `docs/ui/index.html` (the `:root` variables and every selector through `.frame-body .logs`) below the Tailwind import. The class names (`.badge`, `.b-RUNNING`, `.btn`, `.cmd`, `.logs`, `.toggle`, `.progress`, `.meta`, `.panel`, `.chip`, `.frame`, etc.) are reused verbatim by the React components, so the port must be 1:1.

```css
@import "tailwindcss";

/* ---- ported from docs/ui/index.html <style> ---- */
:root {
  --bg: #0a0e1a;
  --text: #e8eef6;
  /* ...copy the remaining :root variables from docs/ui/index.html... */
}
/* ...copy every rule from `* { box-sizing... }` through `.frame-body .logs { ... }` ... */
```

> Keep `html, body { height: 100%; margin: 0; }` and the `body { ... }` gradient/flex rules — the React `#root` must also stretch. Add this rule after the body rule so the flex column fills the viewport:
>
> ```css
> #root { display: flex; flex-direction: column; min-height: 100%; flex: 1; }
> ```

- [ ] **Step 2: Verify**

Run: `npm run check`
Expected: exits 0 (CSS is not type-checked; this confirms nothing broke).

- [ ] **Step 3: Commit**

```bash
git add src/styles/globals.css
git commit -m "feat(ui): port mockup theme into globals.css"
```

---

# Phase 3 — API layer

## Task 8: TypeScript API types

**Files:**
- Create: `src/api/types.ts`

- [ ] **Step 1: Create `src/api/types.ts`**

Types mirror the pydantic view models in `aaiclick/orchestration/view_models.py` and `aaiclick/view_models.py`.

```ts
export type JobStatus = "PENDING" | "RUNNING" | "COMPLETED" | "FAILED" | "CANCELLED";
export type TaskStatus =
  | "PENDING"
  | "CLAIMED"
  | "RUNNING"
  | "COMPLETED"
  | "FAILED"
  | "CANCELLED"
  | "PENDING_CLEANUP"
  | "UPSTREAM_FAILED";

export interface Page<T> {
  items: T[];
  total: number | null;
  next_cursor: string | null;
}

export interface JobView {
  id: number;
  name: string;
  status: JobStatus;
  run_type: string;
  preservation_mode: string;
  registered_job_id: number | null;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  total_tasks: number;
  completed_tasks: number;
}

export interface TaskView {
  id: number;
  job_id: number;
  entrypoint: string;
  name: string;
  status: TaskStatus;
  attempt: number;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
}

export interface JobDetail extends JobView {
  tasks: TaskView[];
  duration_ms: number | null;
}

export interface TaskDetail extends TaskView {
  kwargs: Record<string, unknown>;
  result: Record<string, unknown> | null;
  log_path: string | null;
  worker_id: number | null;
  error: string | null;
  max_retries: number;
}

export interface TaskLogs {
  available: boolean;
  log_path: string | null;
  lines: string[];
}

export interface RegisteredJobView {
  id: number;
  name: string;
  entrypoint: string;
  enabled: boolean;
  schedule: string | null;
  default_kwargs: Record<string, unknown> | null;
  preservation_mode: string | null;
  next_run_at: string | null;
  created_at: string;
  updated_at: string;
}

export interface Problem {
  title: string;
  status: number;
  detail: string | null;
  code: string | null;
}

export interface RunJobRequest {
  name: string;
  kwargs?: Record<string, unknown>;
  preservation_mode?: string | null;
}

export interface RegisterJobRequest {
  name?: string;
  entrypoint: string;
  schedule?: string | null;
  default_kwargs?: Record<string, unknown> | null;
  enabled?: boolean;
  preservation_mode?: string | null;
}
```

- [ ] **Step 2: Verify**

Run: `npm run check`
Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/api/types.ts
git commit -m "feat(ui): add API TypeScript types"
```

## Task 9: REST client

**Files:**
- Create: `src/api/client.ts`

- [ ] **Step 1: Create `src/api/client.ts`**

```ts
import type { Problem } from "./types";

export const API = "/api/v0";

export class ApiError extends Error {
  status: number;
  problem: Problem | null;
  constructor(status: number, problem: Problem | null, message: string) {
    super(message);
    this.status = status;
    this.problem = problem;
  }
}

async function parseError(res: Response): Promise<ApiError> {
  let problem: Problem | null = null;
  try {
    problem = (await res.json()) as Problem;
  } catch {
    problem = null;
  }
  const detail = problem?.detail ?? problem?.title ?? res.statusText;
  return new ApiError(res.status, problem, detail);
}

export async function fetchJSON<T>(path: string): Promise<T> {
  const res = await fetch(`${API}${path}`);
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}

export async function postJSON<T>(path: string, body?: unknown): Promise<T> {
  const res = await fetch(`${API}${path}`, {
    method: "POST",
    headers: body === undefined ? {} : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  if (!res.ok) throw await parseError(res);
  return (await res.json()) as T;
}
```

- [ ] **Step 2: Verify**

Run: `npm run check`
Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/api/client.ts
git commit -m "feat(ui): add REST client"
```

## Task 10: React Query hooks

**Files:**
- Create: `src/api/hooks.ts`

- [ ] **Step 1: Create `src/api/hooks.ts`**

```ts
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { fetchJSON, postJSON } from "./client";
import type {
  JobDetail,
  JobView,
  Page,
  RegisteredJobView,
  RegisterJobRequest,
  RunJobRequest,
  TaskDetail,
  TaskLogs,
} from "./types";

const POLL = 2000;

export function useJobs() {
  return useQuery({
    queryKey: ["jobs"],
    queryFn: () => fetchJSON<Page<JobView>>("/jobs"),
    refetchInterval: POLL,
  });
}

export function useJob(ref: string) {
  return useQuery({
    queryKey: ["job", ref],
    queryFn: () => fetchJSON<JobDetail>(`/jobs/${encodeURIComponent(ref)}`),
    refetchInterval: POLL,
    enabled: ref.length > 0,
  });
}

export function useTask(id: number) {
  return useQuery({
    queryKey: ["task", id],
    queryFn: () => fetchJSON<TaskDetail>(`/tasks/${id}`),
    refetchInterval: POLL,
    enabled: Number.isFinite(id),
  });
}

export function useTaskLogs(id: number) {
  return useQuery({
    queryKey: ["task-logs", id],
    queryFn: () => fetchJSON<TaskLogs>(`/tasks/${id}/logs`),
    refetchInterval: POLL,
    enabled: Number.isFinite(id),
  });
}

export function useRegisteredJobs() {
  return useQuery({
    queryKey: ["registered-jobs"],
    queryFn: () => fetchJSON<Page<RegisteredJobView>>("/registered-jobs"),
    refetchInterval: POLL,
  });
}

export function useRunJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: RunJobRequest) => postJSON<JobView>("/jobs:run", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["jobs"] }),
  });
}

export function useCancelJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (ref: string) => postJSON<JobView>(`/jobs/${encodeURIComponent(ref)}/cancel`),
    onSuccess: (_data, ref) => {
      qc.invalidateQueries({ queryKey: ["jobs"] });
      qc.invalidateQueries({ queryKey: ["job", ref] });
    },
  });
}

export function useRegisterJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (req: RegisterJobRequest) => postJSON<RegisteredJobView>("/registered-jobs", req),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["registered-jobs"] }),
  });
}

export function useToggleRegisteredJob() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ name, enabled }: { name: string; enabled: boolean }) =>
      postJSON<RegisteredJobView>(`/registered-jobs/${encodeURIComponent(name)}/${enabled ? "enable" : "disable"}`),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["registered-jobs"] }),
  });
}
```

- [ ] **Step 2: Verify**

Run: `npm run check`
Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add src/api/hooks.ts
git commit -m "feat(ui): add React Query hooks"
```

---

# Phase 4 — Prompt router, formatting, app shell

## Task 11: Prompt parser + URL sync

**Files:**
- Create: `src/prompt.ts`

- [ ] **Step 1: Create `src/prompt.ts`**

```ts
export type Route =
  | { kind: "home" }
  | { kind: "all" }
  | { kind: "jobs" }
  | { kind: "registered" }
  | { kind: "register"; name: string }
  | { kind: "job"; name: string }
  | { kind: "task"; id: number }
  | { kind: "run-confirm"; name: string }
  | { kind: "run-form"; name: string }
  | { kind: "cancel-confirm"; ref: string }
  | { kind: "unknown"; raw: string };

export function parsePrompt(raw: string): Route {
  const p = raw.trim();
  if (p === "") return { kind: "home" };
  if (p === "@all") return { kind: "all" };
  if (p === "@jobs") return { kind: "jobs" };
  if (p === "@registered") return { kind: "registered" };
  if (p === "register") return { kind: "register", name: "" };
  if (p.startsWith("register ")) return { kind: "register", name: p.slice(9).trim() };
  if (p.startsWith("@job ")) return { kind: "job", name: p.slice(5).trim() };
  if (p.startsWith("@task ")) return { kind: "task", id: parseInt(p.slice(6).trim(), 10) };
  if (p.startsWith("run ")) {
    const rest = p.slice(4).trim();
    if (rest.endsWith("?")) return { kind: "run-form", name: rest.slice(0, -1).trim() };
    return { kind: "run-confirm", name: rest };
  }
  if (p.startsWith("cancel ")) return { kind: "cancel-confirm", ref: p.slice(7).trim() };
  return { kind: "unknown", raw: p };
}

const PARAM = "p";

export function promptFromUrl(): string {
  return new URLSearchParams(window.location.search).get(PARAM) ?? "";
}

export function pushPromptToUrl(prompt: string): void {
  const url = new URL(window.location.href);
  if (prompt) url.searchParams.set(PARAM, prompt);
  else url.searchParams.delete(PARAM);
  window.history.pushState({}, "", url);
}
```

- [ ] **Step 2: Verify** — `npm run check` exits 0.
- [ ] **Step 3: Commit**

```bash
git add src/prompt.ts
git commit -m "feat(ui): add prompt parser and URL sync"
```

## Task 12: Time/duration formatting

**Files:**
- Create: `src/lib/format.ts`

- [ ] **Step 1: Create `src/lib/format.ts`**

```ts
export function relativeTime(iso: string | null): string {
  if (!iso) return "—";
  const then = new Date(iso).getTime();
  const secs = Math.max(0, Math.round((Date.now() - then) / 1000));
  if (secs < 60) return `${secs}s ago`;
  const mins = Math.round(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.round(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
}

export function durationBetween(start: string | null, end: string | null): string {
  if (!start) return "—";
  const from = new Date(start).getTime();
  const to = end ? new Date(end).getTime() : Date.now();
  let secs = Math.max(0, Math.round((to - from) / 1000));
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  secs = secs % 60;
  if (mins < 60) return `${mins}m ${String(secs).padStart(2, "0")}s`;
  const hours = Math.floor(mins / 60);
  return `${hours}h ${String(mins % 60).padStart(2, "0")}m`;
}

export function durationMs(ms: number | null): string {
  if (ms == null) return "—";
  return durationBetween(new Date(Date.now() - ms).toISOString(), new Date().toISOString());
}
```

- [ ] **Step 2: Verify** — `npm run check` exits 0.
- [ ] **Step 3: Commit**

```bash
git add src/lib/format.ts
git commit -m "feat(ui): add time/duration formatting helpers"
```

## Task 13: Toast provider

**Files:**
- Create: `src/components/Toast.tsx`

- [ ] **Step 1: Create `src/components/Toast.tsx`**

```tsx
import { createContext, useCallback, useContext, useState, type ReactNode } from "react";

interface ToastItem {
  id: number;
  message: string;
}

const ToastContext = createContext<(message: string) => void>(() => {});

export function useToast() {
  return useContext(ToastContext);
}

export function ToastProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<ToastItem[]>([]);

  const push = useCallback((message: string) => {
    const id = Date.now() + Math.random();
    setToasts((t) => [...t, { id, message }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 2600);
  }, []);

  return (
    <ToastContext.Provider value={push}>
      {children}
      <div id="toast-wrap">
        {toasts.map((t) => (
          <div key={t.id} className="toast">
            {t.message}
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}
```

- [ ] **Step 2: Verify** — `npm run check` exits 0.
- [ ] **Step 3: Commit**

```bash
git add src/components/Toast.tsx
git commit -m "feat(ui): add toast provider"
```

## Task 14: App shell (Header + router + providers)

**Files:**
- Create: `src/main.tsx`, `src/App.tsx`, `src/components/Header.tsx`
- Note: views are stubbed in this task and filled in Phase 5. Create temporary stubs so `tsc` passes, then replace them per Phase 5.

- [ ] **Step 1: Create `src/components/Header.tsx`**

```tsx
interface HeaderProps {
  prompt: string;
  onPrompt: (value: string) => void;
}

export function Header({ prompt, onPrompt }: HeaderProps) {
  return (
    <header>
      <div className="logo">
        aai<span>click</span>
      </div>
      <div className="prompt-wrap">
        <input
          id="prompt"
          value={prompt}
          onChange={(e) => onPrompt(e.target.value)}
          placeholder="Type a command…  try @jobs, @registered, or run nyc_taxi_pipeline"
          autoComplete="off"
          spellCheck={false}
        />
      </div>
      <div className="hint">prompt drives the view ↑</div>
    </header>
  );
}
```

- [ ] **Step 2: Create view stubs**

Create `src/views/index.tsx` with stub exports so the router type-checks before Phase 5 fills them in:

```tsx
export function Home({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Home</h2>;
}
export function Jobs({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Jobs</h2>;
}
export function JobDetail({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Job {name}</h2>;
}
export function TaskDetail({ id, onPrompt }: { id: number; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Task {id}</h2>;
}
export function Registered({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Registered</h2>;
}
export function RunConfirm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Run {name}?</h2>;
}
export function RunForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Run {name}</h2>;
}
export function RegisterForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Register {name}</h2>;
}
export function CancelConfirm({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>Cancel {refId}?</h2>;
}
export function AllGallery({ onPrompt }: { onPrompt: (v: string) => void }) {
  void onPrompt;
  return <h2>All</h2>;
}
```

- [ ] **Step 3: Create `src/App.tsx`**

```tsx
import { useEffect, useState } from "react";
import { Header } from "./components/Header";
import { parsePrompt, promptFromUrl, pushPromptToUrl } from "./prompt";
import {
  AllGallery,
  CancelConfirm,
  Home,
  JobDetail,
  Jobs,
  Registered,
  RegisterForm,
  RunConfirm,
  RunForm,
  TaskDetail,
} from "./views";

function renderRoute(prompt: string, onPrompt: (v: string) => void) {
  const route = parsePrompt(prompt);
  switch (route.kind) {
    case "home":
      return <Home onPrompt={onPrompt} />;
    case "all":
      return <AllGallery onPrompt={onPrompt} />;
    case "jobs":
      return <Jobs onPrompt={onPrompt} />;
    case "registered":
      return <Registered onPrompt={onPrompt} />;
    case "register":
      return <RegisterForm name={route.name} onPrompt={onPrompt} />;
    case "job":
      return <JobDetail name={route.name} onPrompt={onPrompt} />;
    case "task":
      return <TaskDetail id={route.id} onPrompt={onPrompt} />;
    case "run-confirm":
      return <RunConfirm name={route.name} onPrompt={onPrompt} />;
    case "run-form":
      return <RunForm name={route.name} onPrompt={onPrompt} />;
    case "cancel-confirm":
      return <CancelConfirm refId={route.ref} onPrompt={onPrompt} />;
    case "unknown":
      return (
        <>
          <h2>Unknown command</h2>
          <p className="sub mono">{route.raw}</p>
          <div className="chips">
            <span className="chip" onClick={() => onPrompt("")}>
              help
            </span>
            <span className="chip" onClick={() => onPrompt("@jobs")}>
              @jobs
            </span>
          </div>
        </>
      );
  }
}

export function App() {
  const [prompt, setPrompt] = useState(promptFromUrl);

  const onPrompt = (value: string) => {
    setPrompt(value);
    pushPromptToUrl(value);
  };

  useEffect(() => {
    const onPop = () => setPrompt(promptFromUrl());
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  return (
    <>
      <Header prompt={prompt} onPrompt={onPrompt} />
      <main>
        <div className="content" id="content">
          {renderRoute(prompt, onPrompt)}
        </div>
      </main>
    </>
  );
}
```

- [ ] **Step 4: Create `src/main.tsx`**

```tsx
import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App } from "./App";
import { ToastProvider } from "./components/Toast";
import "./styles/globals.css";

const queryClient = new QueryClient();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      <ToastProvider>
        <App />
      </ToastProvider>
    </QueryClientProvider>
  </StrictMode>,
);
```

- [ ] **Step 5: Verify** — `npm run check` exits 0.

- [ ] **Step 6: Manual smoke**

Run: `npm run dev`, open the printed URL. Expected: header + prompt render; typing `@jobs` shows the "Jobs" stub; the URL gains `?p=@jobs`; browser back restores the previous prompt. Stop the dev server.

- [ ] **Step 7: Commit**

```bash
git add src/main.tsx src/App.tsx src/components/Header.tsx src/views/index.tsx
git commit -m "feat(ui): add app shell, header, and prompt router"
```

---

# Phase 5 — Components and views

> Each view's markup mirrors the corresponding `view*()` function in `docs/ui/index.html`; reuse the exact class names. Replace the stubs in `src/views/index.tsx` by creating one file per view and re-exporting them from `src/views/index.tsx`.

## Task 15: Shared presentational components

**Files:**
- Create: `src/components/StatusBadge.tsx`, `ProgressBar.tsx`, `Chips.tsx`, `MetaGrid.tsx`, `Panel.tsx`

- [ ] **Step 1: `src/components/StatusBadge.tsx`**

```tsx
export function StatusBadge({ status }: { status: string }) {
  return <span className={`badge b-${status}`}>{status}</span>;
}
```

- [ ] **Step 2: `src/components/ProgressBar.tsx`**

```tsx
export function ProgressBar({ done, total }: { done: number; total: number }) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0;
  return (
    <div className="progress">
      <div className="bar">
        <span style={{ width: `${pct}%` }} />
      </div>
      <span className="mono">
        {done}/{total}
      </span>
    </div>
  );
}
```

- [ ] **Step 3: `src/components/Chips.tsx`**

```tsx
interface Chip {
  label: string;
  cmd: string;
}

export function Chips({ chips, onPrompt, children }: { chips: Chip[]; onPrompt: (v: string) => void; children?: React.ReactNode }) {
  return (
    <div className="chips">
      {chips.map((c) => (
        <span key={c.label} className="chip" onClick={() => onPrompt(c.cmd)}>
          {c.label}
        </span>
      ))}
      {children}
    </div>
  );
}
```

- [ ] **Step 4: `src/components/MetaGrid.tsx`**

```tsx
export interface MetaItem {
  k: string;
  v: React.ReactNode;
  mono?: boolean;
}

export function MetaGrid({ items }: { items: MetaItem[] }) {
  return (
    <div className="meta">
      {items.map((it) => (
        <div key={it.k}>
          <span className="k">{it.k}</span>
          {it.mono ? <span className="mono">{it.v}</span> : it.v}
        </div>
      ))}
    </div>
  );
}
```

- [ ] **Step 5: `src/components/Panel.tsx`**

```tsx
export function Panel({ className = "", children }: { className?: string; children: React.ReactNode }) {
  return <div className={`panel ${className}`}>{children}</div>;
}
```

- [ ] **Step 6: Verify** — `npm run check` exits 0.
- [ ] **Step 7: Commit**

```bash
git add src/components/StatusBadge.tsx src/components/ProgressBar.tsx src/components/Chips.tsx src/components/MetaGrid.tsx src/components/Panel.tsx
git commit -m "feat(ui): add shared presentational components"
```

## Task 16: `LogViewer` and `EnabledToggle`

**Files:**
- Create: `src/components/LogViewer.tsx`, `src/components/EnabledToggle.tsx`

- [ ] **Step 1: `src/components/LogViewer.tsx`**

```tsx
import { useTaskLogs } from "../api/hooks";

export function LogViewer({ taskId }: { taskId: number }) {
  const { data, isLoading, isError } = useTaskLogs(taskId);

  if (isLoading) return <div className="logs">loading logs…</div>;
  if (isError) return <div className="logs">failed to load logs</div>;
  if (!data || !data.available || data.lines.length === 0) {
    return <div className="logs">(no logs captured for this task)</div>;
  }
  return (
    <div className="logs">
      {data.lines.map((line, i) => (
        <div key={i}>{line}</div>
      ))}
    </div>
  );
}
```

- [ ] **Step 2: `src/components/EnabledToggle.tsx`**

```tsx
import { useToggleRegisteredJob } from "../api/hooks";
import { useToast } from "./Toast";

export function EnabledToggle({ name, enabled }: { name: string; enabled: boolean }) {
  const toggle = useToggleRegisteredJob();
  const toast = useToast();
  const onClick = () => {
    const next = !enabled;
    toggle.mutate(
      { name, enabled: next },
      { onSuccess: () => toast(`${next ? "Enabled" : "Disabled"} ${name}`) },
    );
  };
  return (
    <button className={`toggle ${enabled ? "on" : "off"}`} onClick={onClick}>
      <span className="switch" />
      {enabled ? "enabled" : "disabled"}
    </button>
  );
}
```

- [ ] **Step 3: Verify** — `npm run check` exits 0.
- [ ] **Step 4: Commit**

```bash
git add src/components/LogViewer.tsx src/components/EnabledToggle.tsx
git commit -m "feat(ui): add LogViewer and EnabledToggle"
```

## Task 17: Home view

**Files:**
- Create: `src/views/Home.tsx`
- Modify: `src/views/index.tsx` (re-export real Home)

- [ ] **Step 1: `src/views/Home.tsx`**

Port `viewHome()` from `docs/ui/index.html` to JSX. Use the same `.cmd-list` / `.cmd` / `.group-label` / `.note` markup; each `.cmd` calls `onPrompt(cmd)` on click.

```tsx
interface Cmd {
  code: string;
  desc: string;
  cmd: string;
}

const NAVIGATE: Cmd[] = [
  { code: "@jobs", desc: "List all jobs, newest first.", cmd: "@jobs" },
  { code: "@registered", desc: "Registered jobs — run, register, enable/disable.", cmd: "@registered" },
  { code: "@job <name>", desc: "Job detail — header + tasks table.", cmd: "@job nyc_taxi_pipeline" },
  { code: "@task <id>", desc: "Task detail — status bar + live logs.", cmd: "@task 1" },
  { code: "@all", desc: "Every screen on one page.", cmd: "@all" },
];

const ACTIONS: Cmd[] = [
  { code: "run <name>", desc: "Run a registered job with defaults. Add ? to edit params.", cmd: "run nyc_taxi_pipeline" },
  { code: "cancel <name|id>", desc: "Cancel a pending/running job.", cmd: "cancel nyc_taxi_pipeline" },
  { code: "register", desc: "Register a new job (entrypoint, schedule, kwargs).", cmd: "register" },
  { code: "enable / disable <name>", desc: "Toggle a registered job on/off via @registered.", cmd: "@registered" },
];

function CmdList({ items, onPrompt }: { items: Cmd[]; onPrompt: (v: string) => void }) {
  return (
    <div className="cmd-list">
      {items.map((c) => (
        <div key={c.code} className="cmd" onClick={() => onPrompt(c.cmd)}>
          <code>{c.code}</code>
          <span className="desc">{c.desc}</span>
        </div>
      ))}
    </div>
  );
}

export function Home({ onPrompt }: { onPrompt: (v: string) => void }) {
  return (
    <>
      <h2>aaiclick</h2>
      <p className="sub">Prompt-driven operator dashboard. Type a command above, or click one below.</p>
      <div className="group-label">Navigate</div>
      <CmdList items={NAVIGATE} onPrompt={onPrompt} />
      <div className="group-label">Actions</div>
      <CmdList items={ACTIONS} onPrompt={onPrompt} />
    </>
  );
}
```

- [ ] **Step 2: Re-export**

In `src/views/index.tsx`, remove the stub `Home` and add at the top: `export { Home } from "./Home";`

- [ ] **Step 3: Verify** — `npm run check` exits 0.
- [ ] **Step 4: Commit**

```bash
git add src/views/Home.tsx src/views/index.tsx
git commit -m "feat(ui): implement Home view"
```

## Task 18: Jobs list view

**Files:**
- Create: `src/components/JobsTable.tsx`, `src/views/Jobs.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/components/JobsTable.tsx`**

```tsx
import type { JobView } from "../api/types";
import { durationBetween, relativeTime } from "../lib/format";
import { ProgressBar } from "./ProgressBar";
import { StatusBadge } from "./StatusBadge";

export function JobsTable({ jobs, onPrompt }: { jobs: JobView[]; onPrompt: (v: string) => void }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Status</th>
          <th>Progress</th>
          <th>Created</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>
        {jobs.map((j) => (
          <tr key={j.id} className="clickable" onClick={() => onPrompt(`@job ${j.name}`)}>
            <td>
              <span className="name-link mono">{j.name}</span>
            </td>
            <td>
              <StatusBadge status={j.status} />
            </td>
            <td>
              <ProgressBar done={j.completed_tasks} total={j.total_tasks} />
            </td>
            <td>{relativeTime(j.created_at)}</td>
            <td className="mono">{durationBetween(j.started_at, j.completed_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 2: `src/views/Jobs.tsx`**

```tsx
import { useJobs } from "../api/hooks";
import { Chips } from "../components/Chips";
import { JobsTable } from "../components/JobsTable";

export function Jobs({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useJobs();
  return (
    <>
      <h2>Jobs</h2>
      <p className="sub">Sorted by created_at, newest first · auto-refreshes</p>
      <Chips
        chips={[
          { label: "← home", cmd: "" },
          { label: "@registered", cmd: "@registered" },
        ]}
        onPrompt={onPrompt}
      />
      {isLoading && <p className="sub">loading…</p>}
      {isError && <p className="err">failed to load jobs</p>}
      {data && <JobsTable jobs={data.items} onPrompt={onPrompt} />}
    </>
  );
}
```

- [ ] **Step 3: Re-export** — in `src/views/index.tsx` replace stub `Jobs` with `export { Jobs } from "./Jobs";`
- [ ] **Step 4: Verify** — `npm run check` exits 0.
- [ ] **Step 5: Commit**

```bash
git add src/components/JobsTable.tsx src/views/Jobs.tsx src/views/index.tsx
git commit -m "feat(ui): implement Jobs list view"
```

## Task 19: Job detail view

**Files:**
- Create: `src/components/TasksTable.tsx`, `src/views/JobDetail.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/components/TasksTable.tsx`**

```tsx
import type { TaskView } from "../api/types";
import { durationBetween, relativeTime } from "../lib/format";
import { StatusBadge } from "./StatusBadge";

export function TasksTable({ tasks, onPrompt }: { tasks: TaskView[]; onPrompt: (v: string) => void }) {
  return (
    <table>
      <thead>
        <tr>
          <th>Name</th>
          <th>Status</th>
          <th>Entrypoint</th>
          <th>Attempt</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>
        {tasks.map((t) => (
          <tr key={t.id} className="clickable" onClick={() => onPrompt(`@task ${t.id}`)}>
            <td>
              <span className="name-link mono">{t.name}</span>
            </td>
            <td>
              <StatusBadge status={t.status} />
            </td>
            <td className="mono">{t.entrypoint}</td>
            <td className="mono">{t.attempt}</td>
            <td>{relativeTime(t.started_at)}</td>
            <td className="mono">{durationBetween(t.started_at, t.completed_at)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 2: `src/views/JobDetail.tsx`**

```tsx
import { useCancelJob, useJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { MetaGrid } from "../components/MetaGrid";
import { StatusBadge } from "../components/StatusBadge";
import { TasksTable } from "../components/TasksTable";
import { useToast } from "../components/Toast";
import { durationMs, relativeTime } from "../lib/format";

export function JobDetail({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const { data: job, isLoading, isError } = useJob(name);
  const cancel = useCancelJob();
  const toast = useToast();

  if (isLoading) return <p className="sub">loading…</p>;
  if (isError || !job)
    return (
      <>
        <h2>Job not found</h2>
        <p className="sub mono">{name}</p>
        <Chips chips={[{ label: "@jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      </>
    );

  const cancellable = job.status === "RUNNING" || job.status === "PENDING";
  const onCancel = () => onPrompt(`cancel ${job.name}`);
  void cancel;
  void toast;

  return (
    <>
      <Chips chips={[{ label: "← @jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      <div className="detail-head">
        <div className="title-row">
          <h2>
            <span className="mono">{job.name}</span> <StatusBadge status={job.status} />
          </h2>
          <div className="spacer" />
          {cancellable && (
            <button className="btn btn-danger btn-sm" onClick={onCancel}>
              Cancel job
            </button>
          )}
        </div>
        <MetaGrid
          items={[
            { k: "Created", v: relativeTime(job.created_at) },
            { k: "Started", v: relativeTime(job.started_at) },
            { k: "Completed", v: relativeTime(job.completed_at) },
            { k: "Duration", v: durationMs(job.duration_ms) },
            { k: "Progress", v: `${job.completed_tasks}/${job.total_tasks} tasks` },
          ]}
        />
        {job.error && <div className="err">{job.error}</div>}
      </div>
      <TasksTable tasks={job.tasks} onPrompt={onPrompt} />
    </>
  );
}
```

- [ ] **Step 3: Re-export** — replace stub `JobDetail` with `export { JobDetail } from "./JobDetail";`
- [ ] **Step 4: Verify** — `npm run check` exits 0.
- [ ] **Step 5: Commit**

```bash
git add src/components/TasksTable.tsx src/views/JobDetail.tsx src/views/index.tsx
git commit -m "feat(ui): implement Job detail view"
```

## Task 20: Task detail view

**Files:**
- Create: `src/views/TaskDetail.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/views/TaskDetail.tsx`**

```tsx
import { useTask } from "../api/hooks";
import { Chips } from "../components/Chips";
import { LogViewer } from "../components/LogViewer";
import { MetaGrid } from "../components/MetaGrid";
import { StatusBadge } from "../components/StatusBadge";
import { durationBetween, relativeTime } from "../lib/format";

export function TaskDetail({ id, onPrompt }: { id: number; onPrompt: (v: string) => void }) {
  const { data: task, isLoading, isError } = useTask(id);

  if (isLoading) return <p className="sub">loading…</p>;
  if (isError || !task)
    return (
      <>
        <h2>Task not found</h2>
        <p className="sub mono">#{id}</p>
        <Chips chips={[{ label: "@jobs", cmd: "@jobs" }]} onPrompt={onPrompt} />
      </>
    );

  return (
    <>
      <Chips
        chips={[
          { label: "@jobs", cmd: "@jobs" },
          { label: `← @job ${task.job_id}`, cmd: `@job ${task.job_id}` },
        ]}
        onPrompt={onPrompt}
      />
      <div className="detail-head">
        <h2>
          <span className="mono">{task.name}</span> <StatusBadge status={task.status} />
        </h2>
        <MetaGrid
          items={[
            { k: "Task ID", v: `#${task.id}`, mono: true },
            { k: "Job", v: String(task.job_id), mono: true },
            { k: "Entrypoint", v: task.entrypoint, mono: true },
            { k: "Attempt", v: `${task.attempt}/${task.max_retries}`, mono: true },
            { k: "Worker", v: task.worker_id == null ? "—" : String(task.worker_id), mono: true },
            { k: "Started", v: relativeTime(task.started_at) },
            { k: "Duration", v: durationBetween(task.started_at, task.completed_at) },
          ]}
        />
        {task.error && <div className="err">{task.error}</div>}
      </div>
      <LogViewer taskId={task.id} />
    </>
  );
}
```

- [ ] **Step 2: Re-export** — replace stub `TaskDetail` with `export { TaskDetail } from "./TaskDetail";`
- [ ] **Step 3: Verify** — `npm run check` exits 0.
- [ ] **Step 4: Commit**

```bash
git add src/views/TaskDetail.tsx src/views/index.tsx
git commit -m "feat(ui): implement Task detail view"
```

## Task 21: Registered jobs view

**Files:**
- Create: `src/views/Registered.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/views/Registered.tsx`**

```tsx
import { useRegisteredJobs } from "../api/hooks";
import { Chips } from "../components/Chips";
import { EnabledToggle } from "../components/EnabledToggle";

export function Registered({ onPrompt }: { onPrompt: (v: string) => void }) {
  const { data, isLoading, isError } = useRegisteredJobs();
  return (
    <>
      <Chips chips={[{ label: "← @jobs", cmd: "@jobs" }]} onPrompt={onPrompt}>
        <div className="spacer" />
        <button className="btn btn-primary btn-sm" onClick={() => onPrompt("register")}>
          + Register new job
        </button>
      </Chips>
      <h2>Registered jobs</h2>
      <p className="sub">Run on demand, or on a cron schedule via the background scheduler.</p>
      {isLoading && <p className="sub">loading…</p>}
      {isError && <p className="err">failed to load registered jobs</p>}
      {data && (
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Entrypoint</th>
              <th>Enabled</th>
              <th>Schedule</th>
              <th>Next run</th>
              <th>Actions</th>
            </tr>
          </thead>
          <tbody>
            {data.items.map((r) => (
              <tr key={r.id}>
                <td>
                  <span className="name-link mono">{r.name}</span>
                </td>
                <td className="mono">{r.entrypoint}</td>
                <td>
                  <EnabledToggle name={r.name} enabled={r.enabled} />
                </td>
                <td className="mono">{r.schedule ?? "manual"}</td>
                <td>{r.next_run_at ?? "—"}</td>
                <td>
                  <div className="row-actions">
                    <button className="btn btn-primary btn-sm" onClick={() => onPrompt(`run ${r.name}`)}>
                      Run
                    </button>
                    <button className="btn btn-sm" onClick={() => onPrompt(`run ${r.name} ?`)}>
                      Run…
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </>
  );
}
```

- [ ] **Step 2: Re-export** — replace stub `Registered` with `export { Registered } from "./Registered";`
- [ ] **Step 3: Verify** — `npm run check` exits 0.
- [ ] **Step 4: Commit**

```bash
git add src/views/Registered.tsx src/views/index.tsx
git commit -m "feat(ui): implement Registered jobs view"
```

## Task 22: Run confirm + run form views

**Files:**
- Create: `src/views/RunConfirm.tsx`, `src/views/RunForm.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/views/RunConfirm.tsx`**

```tsx
import { useRunJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function RunConfirm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const run = useRunJob();
  const toast = useToast();
  const onRun = () =>
    run.mutate(
      { name },
      {
        onSuccess: (job) => {
          toast(`Started ${name} — job #${job.id}`);
          onPrompt(`@job ${name}`);
        },
        onError: (e) => toast(`Run failed: ${e.message}`),
      },
    );

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel className="confirm info">
        <h2>
          Run <span className="mono">{name}</span>?
        </h2>
        <p className="sub">Starts a new job with the registered default parameters.</p>
        <div className="form-actions">
          <button className="btn btn-primary" disabled={run.isPending} onClick={onRun}>
            Run job
          </button>
          <button className="btn" onClick={() => onPrompt(`run ${name} ?`)}>
            Edit parameters…
          </button>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Cancel
          </button>
        </div>
      </Panel>
    </>
  );
}
```

- [ ] **Step 2: `src/views/RunForm.tsx`**

```tsx
import { useState } from "react";
import { useRunJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function RunForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const run = useRunJob();
  const toast = useToast();
  const [kwargs, setKwargs] = useState("{}");
  const [preservation, setPreservation] = useState("");

  const onRun = () => {
    let parsed: Record<string, unknown>;
    try {
      parsed = JSON.parse(kwargs || "{}");
    } catch {
      toast("kwargs is not valid JSON");
      return;
    }
    run.mutate(
      { name, kwargs: parsed, preservation_mode: preservation || null },
      {
        onSuccess: (job) => {
          toast(`Started ${name} — job #${job.id}`);
          onPrompt(`@job ${name}`);
        },
        onError: (e) => toast(`Run failed: ${e.message}`),
      },
    );
  };

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel>
        <h2>
          Run <span className="mono">{name}</span>
        </h2>
        <p className="sub">POST /api/v0/jobs:run — edit parameters before launching.</p>
        <div className="field">
          <label>
            kwargs <span className="help">— JSON passed to the job entrypoint</span>
          </label>
          <textarea rows={5} value={kwargs} onChange={(e) => setKwargs(e.target.value)} />
        </div>
        <div className="field">
          <label>Preservation mode</label>
          <select value={preservation} onChange={(e) => setPreservation(e.target.value)}>
            <option value="">(registered default)</option>
            <option value="NONE">NONE</option>
            <option value="TEMP_NAMED">TEMP_NAMED</option>
          </select>
        </div>
        <div className="form-actions">
          <button className="btn btn-primary" disabled={run.isPending} onClick={onRun}>
            Run job
          </button>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Cancel
          </button>
        </div>
      </Panel>
    </>
  );
}
```

- [ ] **Step 3: Re-export** — replace stubs `RunConfirm`, `RunForm` with real re-exports.
- [ ] **Step 4: Verify** — `npm run check` exits 0.
- [ ] **Step 5: Commit**

```bash
git add src/views/RunConfirm.tsx src/views/RunForm.tsx src/views/index.tsx
git commit -m "feat(ui): implement run confirm and run form views"
```

## Task 23: Register form + cancel confirm views

**Files:**
- Create: `src/views/RegisterForm.tsx`, `src/views/CancelConfirm.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/views/RegisterForm.tsx`**

```tsx
import { useState } from "react";
import { useRegisterJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function RegisterForm({ name, onPrompt }: { name: string; onPrompt: (v: string) => void }) {
  const register = useRegisterJob();
  const toast = useToast();
  const [entrypoint, setEntrypoint] = useState("");
  const [jobName, setJobName] = useState(name);
  const [schedule, setSchedule] = useState("");
  const [kwargs, setKwargs] = useState("");
  const [enabled, setEnabled] = useState(true);

  const onRegister = () => {
    let parsed: Record<string, unknown> | null = null;
    if (kwargs.trim()) {
      try {
        parsed = JSON.parse(kwargs);
      } catch {
        toast("default_kwargs is not valid JSON");
        return;
      }
    }
    register.mutate(
      {
        entrypoint,
        name: jobName || undefined,
        schedule: schedule || null,
        default_kwargs: parsed,
        enabled,
      },
      {
        onSuccess: (rj) => {
          toast(`Registered ${rj.name}`);
          onPrompt("@registered");
        },
        onError: (e) => toast(`Register failed: ${e.message}`),
      },
    );
  };

  return (
    <>
      <Chips chips={[{ label: "← @registered", cmd: "@registered" }]} onPrompt={onPrompt} />
      <Panel>
        <h2>Register a job</h2>
        <p className="sub">POST /api/v0/registered-jobs — the entrypoint must exist in the deployed code.</p>
        <div className="field">
          <label>
            Entrypoint <span className="help">— dotted path, e.g. tasks.report.build</span>
          </label>
          <input type="text" value={entrypoint} onChange={(e) => setEntrypoint(e.target.value)} placeholder="package.module.callable" />
        </div>
        <div className="field">
          <label>
            Name <span className="help">— defaults to the last segment of the entrypoint</span>
          </label>
          <input type="text" value={jobName} onChange={(e) => setJobName(e.target.value)} placeholder="(optional)" />
        </div>
        <div className="field">
          <label>
            Schedule <span className="help">— cron, e.g. 0 2 * * * · leave blank for manual</span>
          </label>
          <input type="text" value={schedule} onChange={(e) => setSchedule(e.target.value)} placeholder="(optional cron expression)" />
        </div>
        <div className="field">
          <label>
            default_kwargs <span className="help">— JSON</span>
          </label>
          <textarea rows={3} value={kwargs} onChange={(e) => setKwargs(e.target.value)} placeholder="{}" />
        </div>
        <div className="field inline">
          <input type="checkbox" id="reg-enabled" checked={enabled} onChange={(e) => setEnabled(e.target.checked)} />
          <label htmlFor="reg-enabled">Enabled</label>
        </div>
        <div className="form-actions">
          <button className="btn btn-primary" disabled={register.isPending || !entrypoint} onClick={onRegister}>
            Register
          </button>
          <button className="btn" onClick={() => onPrompt("@registered")}>
            Cancel
          </button>
        </div>
      </Panel>
    </>
  );
}
```

- [ ] **Step 2: `src/views/CancelConfirm.tsx`**

```tsx
import { useCancelJob } from "../api/hooks";
import { Chips } from "../components/Chips";
import { Panel } from "../components/Panel";
import { useToast } from "../components/Toast";

export function CancelConfirm({ refId, onPrompt }: { refId: string; onPrompt: (v: string) => void }) {
  const cancel = useCancelJob();
  const toast = useToast();
  const onCancel = () =>
    cancel.mutate(refId, {
      onSuccess: () => {
        toast(`Cancelling ${refId}…`);
        onPrompt(`@job ${refId}`);
      },
      onError: (e) => toast(`Cancel failed: ${e.message}`),
    });

  return (
    <>
      <Chips chips={[{ label: `← @job ${refId}`, cmd: `@job ${refId}` }]} onPrompt={onPrompt} />
      <Panel className="confirm">
        <h2>
          Cancel <span className="mono">{refId}</span>?
        </h2>
        <p className="sub">
          POST /api/v0/jobs/{refId}/cancel — pending tasks are cancelled and any running task is signalled to abort.
        </p>
        <div className="form-actions">
          <button className="btn btn-danger" disabled={cancel.isPending} onClick={onCancel}>
            Cancel job
          </button>
          <button className="btn" onClick={() => onPrompt(`@job ${refId}`)}>
            Keep running
          </button>
        </div>
      </Panel>
    </>
  );
}
```

- [ ] **Step 3: Re-export** — replace stubs `RegisterForm`, `CancelConfirm` with real re-exports.
- [ ] **Step 4: Verify** — `npm run check` exits 0.
- [ ] **Step 5: Commit**

```bash
git add src/views/RegisterForm.tsx src/views/CancelConfirm.tsx src/views/index.tsx
git commit -m "feat(ui): implement register form and cancel confirm views"
```

## Task 24: AllGallery view

**Files:**
- Create: `src/views/AllGallery.tsx`
- Modify: `src/views/index.tsx`

- [ ] **Step 1: `src/views/AllGallery.tsx`**

The mockup's `@all` shows every screen on one page using static sample data. For the live app, render a lightweight index that links to each real screen rather than re-mounting every data-fetching view (which would fire many queries). Keep the `.chips` + intro and a `.cmd-list` of links.

```tsx
import { Chips } from "../components/Chips";

const SCREENS: { label: string; cmd: string }[] = [
  { label: "Home", cmd: "" },
  { label: "Jobs list", cmd: "@jobs" },
  { label: "Registered jobs", cmd: "@registered" },
  { label: "Register a job", cmd: "register" },
];

export function AllGallery({ onPrompt }: { onPrompt: (v: string) => void }) {
  return (
    <>
      <Chips chips={[{ label: "← home", cmd: "" }]} onPrompt={onPrompt} />
      <h2>All screens</h2>
      <p className="sub">Jump to any live screen.</p>
      <div className="cmd-list">
        {SCREENS.map((s) => (
          <div key={s.label} className="cmd" onClick={() => onPrompt(s.cmd)}>
            <code>{s.cmd || "(home)"}</code>
            <span className="desc">{s.label}</span>
          </div>
        ))}
      </div>
    </>
  );
}
```

- [ ] **Step 2: Re-export** — replace stub `AllGallery` with `export { AllGallery } from "./AllGallery";`. The final `src/views/index.tsx` should contain only re-export lines, no stubs.
- [ ] **Step 3: Verify** — `npm run check` exits 0.
- [ ] **Step 4: Commit**

```bash
git add src/views/AllGallery.tsx src/views/index.tsx
git commit -m "feat(ui): implement AllGallery view"
```

---

# Phase 6 — Build, manual verification, e2e

## Task 25: Production build + manual end-to-end check

**Files:** none (verification task)

- [ ] **Step 1: Build the SPA**

Run: `npm run build`
Expected: `tsc -b` passes; Vite writes assets into `aaiclick/server/static/` (`index.html` + `assets/`).

- [ ] **Step 2: Run the server with the build**

Run: `python -m uvicorn aaiclick.server.app:app --port 8000` (or the documented launch in `docs/api_server.md`).
In a browser open `http://localhost:8000/`.

- [ ] **Step 3: Drive the golden path**

Verify, against a database with at least one job:
- `@jobs` lists jobs with status badges, progress, relative created time.
- Clicking a job row navigates to `@job <name>` showing the tasks table.
- Clicking a task row navigates to `@task <id>` showing metadata + logs (or the "(no logs captured)" placeholder).
- `@registered` lists registered jobs; the enable/disable toggle flips and a toast appears.
- `run <name>` confirm → Run job → toast + navigation to the job.
- Browser refresh on a deep link (`/?p=@jobs`) restores the same view.

If no jobs exist, run one first via the CLI or the `run` action, then re-check.

- [ ] **Step 4: Commit (only if any fixes were needed)**

```bash
git add -A
git commit -m "fix(ui): address issues found during manual verification"
```

## Task 26: Playwright smoke e2e

**Files:**
- Create: `test_e2e/web/test_smoke.py`
- Create: `test_e2e/web/conftest.py` (only if no shared server fixture is reusable)

- [ ] **Step 1: Inspect the existing e2e harness**

Read `test_e2e/docker/` for the established pattern (how the server/orchestrator are started, what pytest fixtures exist). Reuse fixtures where possible; the web smoke must boot the FastAPI app with the built `static/` and drive a browser.

- [ ] **Step 2: Write the smoke test**

Create `test_e2e/web/test_smoke.py`. It must: start the server (reuse the docker-suite server fixture or launch uvicorn against the default chdb+SQLite backend), ensure the SPA build exists (skip with a clear message if `aaiclick/server/static/index.html` is absent), then drive Playwright:

```python
from __future__ import annotations

from pathlib import Path

import pytest

STATIC = Path(__file__).resolve().parents[2] / "aaiclick" / "server" / "static" / "index.html"


@pytest.mark.skipif(not STATIC.is_file(), reason="SPA build missing; run `npm run build`")
async def test_jobs_to_task_golden_path(page, base_url):
    # `page` and `base_url` come from the web-suite fixtures (Step 1).
    await page.goto(f"{base_url}/?p=@jobs")
    await page.wait_for_selector("table")
    # Click the first job row, then the first task row, asserting navigation.
    await page.click("tbody tr.clickable")
    await page.wait_for_selector(".detail-head")
    await page.click("tbody tr.clickable")
    await page.wait_for_selector(".logs")
```

> Finalize fixture names (`page`, `base_url`) to match what Step 1 reveals. If the docker suite has no reusable browser fixture, add a `conftest.py` in `test_e2e/web/` that launches uvicorn on a free port against the default backend, seeds one job, and yields a Playwright `page`. Keep the test under the `test_e2e/` tree, which the default `pytest` run excludes (per `CLAUDE.md`).

- [ ] **Step 3: Run the smoke test**

Run: `npm run build && pytest test_e2e/web/test_smoke.py -v -p no:cov`
Expected: PASS (or SKIP only if the build is intentionally absent).

- [ ] **Step 4: Commit**

```bash
git add test_e2e/web/
git commit -m "test(ui): add Playwright golden-path smoke e2e"
```

## Task 27: Docs — record implementation + deferred work

**Files:**
- Modify: `docs/frontend.md`, `docs/ui.md`, `docs/future.md`

- [ ] **Step 1: Add implementation references**

In `docs/frontend.md` and `docs/ui.md`, mark the implemented pieces and point at the code by name (not line number), e.g. `**Implementation**: aaiclick/server/routers/tasks.py — see get_task_logs`. Note that real-time is REST polling for v0.

- [ ] **Step 2: Record deferred work in `docs/future.md`**

Add entries (if not already present): SSE `/events` endpoint + LISTEN/NOTIFY fanout, cross-host/docker log access, Vitest unit tests, OpenAPI codegen, auth, wheel packaging of `aaiclick/server/static/`.

- [ ] **Step 3: Apply doc skills**

Use the `markdown-style` and `shortify` skills on the edited docs (they live under `docs/`).

- [ ] **Step 4: Commit**

```bash
git add docs/frontend.md docs/ui.md docs/future.md
git commit -m "docs: record operator UI implementation and deferred work"
```

---

# Self-review notes

- **Spec coverage:** Phase 1 covers the three backend additions (logs endpoint → Tasks 1–3, JobView counts → Task 4, SPA mount → Task 5). Phases 2–5 cover the frontend stack, prompt-as-URL state, API hook layer, all views/components, and theme. Phase 6 covers the `tsc` gate (every frontend task), the Playwright smoke e2e (Task 26), and docs (Task 27). All mockup screens map to a view task.
- **Polling:** every live query sets `refetchInterval` (Task 10) — satisfies the REST-polling decision.
- **Type consistency:** hook names (`useJobs`, `useJob`, `useTask`, `useTaskLogs`, `useRegisteredJobs`, `useRunJob`, `useCancelJob`, `useRegisterJob`, `useToggleRegisteredJob`) are defined in Task 10 and used unchanged in Tasks 16–24. `Route` kinds in Task 11 match the `switch` in Task 14. `JobView.total_tasks`/`completed_tasks` (Task 4) match the TS `JobView` (Task 8) and `ProgressBar`/`JobsTable` usage (Tasks 15, 18).
- **Known follow-up:** `@task` back-chip links to `@job <job_id>` (numeric) since `TaskDetail` carries `job_id`, not the job name; `_resolve_job` accepts numeric refs, so this resolves correctly.
