# Creation Funnel Consolidation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** One task-construction path and one task-commit path. `TaskFactory.__call__` and `JobFactory._create_job` stop hand-building `Task` rows and call `factories.create_task`; `create_job` and `create_built_job` stop hand-rolling session/injection/registry logic and delegate to `commit_tasks` (which already owns image validation, build-task injection, and registry cleanup).

**Architecture:** `commit_tasks` (orch_context.py) is the single commit funnel: it stamps `job_id`, validates image sources against the job's runner mode, injects build tasks, and pops the registry. The job row is committed first in its own transaction (the pattern `JobFactory._create_job` already uses) so `commit_tasks` can fetch it for validation. `Task.model_post_init` already registers every task in the ContextVar registry, so `create_task`'s explicit registration is dead code (the survey-reported "TaskFactory doesn't register" divergence does not exist).

**Explicitly out of scope:** a `JobSpec` dataclass rewrite of `registered_jobs.run_job` — its ~15 parameters are documented user-facing API; a spec object would relocate them, not remove them. Only the duplicated fetch-or-raise in `enable_job`/`disable_job` is extracted.

**Tech Stack:** Python 3.12, chdb + SQLite local backend, `uv run pytest`.

**Spec:** This plan is its own spec. Accepted behavior deltas: (a) `create_job`/`create_built_job` commit the job row and the tasks in two transactions instead of one (already `JobFactory`'s pattern; a crash in between leaves a task-less PENDING job, handled by background job cleanup); (b) a `Task` with an `image_source` passed to `create_job` now gets validation + build-task injection via `commit_tasks` (previously silently skipped).

## Global Constraints

- All imports at top of file; no `__all__`; no history comments (CLAUDE.md).
- Do not change `commit_tasks` itself, the decorator public API, or any function signatures in `factories.py` / `registered_jobs.py`.
- Run tests with `uv run pytest <paths> -q --no-cov -p no:cacheprovider`.

---

### Task 1: `create_task` — drop redundant registry write; `create_job`/`create_built_job` → `commit_tasks`

**Files:**
- Modify: `aaiclick/orchestration/factories.py`

- [ ] **Step 1:** In `create_task`, delete the trailing
```python
    registry = get_task_registry()
    if registry is not None:
        registry[task_id] = task
```
  (keep `return task`) — `Task.model_post_init` already calls `register_task(self.id, self)`. Drop the now-unused `from .task_registry import get_task_registry` import if nothing else in the file uses it after Step 2/3.

- [ ] **Step 2:** Rewrite `create_job`'s body after the entry-task resolution:

```python
    job = new_job_row(
        name,
        run_type=run_type,
        registered_job_id=registered_job_id,
        preservation_mode=preservation_mode,
        registered=registered,
    )

    task = entry if isinstance(entry, Task) else create_task(entry)

    async with get_sql_session() as session:
        session.add(job)
        await session.commit()

    await commit_tasks(task, job.id)
    return job
```

  `commit_tasks` sets `task.job_id`, commits, and pops the registry — delete the manual session block and registry pop. Add `commit_tasks` to the `from .orch_context import` line.

- [ ] **Step 3:** Rewrite `create_built_job`'s body after `entry_task.image_source = dump_image_source(image_source)`:

```python
    async with get_sql_session() as session:
        session.add(job)
        await session.commit()

    await commit_tasks(entry_task, job.id)
    return job
```

  `commit_tasks` fetches the committed job, validates image sources against `job.runner_mode`, injects the build task with its `build >> entry` edge, and pops the registry — delete the manual `inject_build_tasks` session block and pop loop. Remove the `inject_build_tasks` import if now unused in this file. Keep the docstring, noting injection now happens in `commit_tasks`.

- [ ] **Step 4:** Run: `uv run pytest aaiclick/orchestration -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 5: Commit** — `refactor: create_job and create_built_job commit tasks through commit_tasks`

### Task 2: decorators build tasks via `create_task`

**Files:**
- Modify: `aaiclick/orchestration/decorators.py`

- [ ] **Step 1:** In `TaskFactory.__call__`, replace the inline `Task(...)` construction (`task_id = get_snowflake_id()` through the `Task(...)` literal) with:

```python
        task = create_task(
            self.entrypoint,
            serialized_kwargs,
            name=self.name,
            max_retries=self.max_retries,
        )
```

  Keep the upstream collection and `upstream >> task` wiring exactly as-is.
- [ ] **Step 2:** In `JobFactory._create_job`, replace the inline entry-task `Task(...)` construction with:

```python
        entry_task = create_task(self.entrypoint, serialized_kwargs, name=self.name)
```

- [ ] **Step 3:** Update imports: `from .factories import _callable_to_string, create_task, new_job_row`; drop `get_snowflake_id`, `utc_now`, `TASK_PENDING` from the import lines if now unused (check each — `utc_now`/`get_snowflake_id` may have no other users in this module).
- [ ] **Step 4:** Run: `uv run pytest aaiclick/orchestration -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 5: Commit** — `refactor: decorators create tasks through factories.create_task`

### Task 3: registered_jobs fetch-or-raise helper

**Files:**
- Modify: `aaiclick/orchestration/registered_jobs.py`

- [ ] **Step 1:** Add above `enable_job`:

```python
async def _get_registered_or_raise(session, name: str) -> RegisteredJob:
    """Fetch a registration by name or raise RegisteredJobNotFound."""
    result = await session.execute(select(RegisteredJob).where(RegisteredJob.name == name))
    job = result.scalar_one_or_none()
    if job is None:
        raise RegisteredJobNotFound(f"Registered job '{name}' not found")
    return job
```

  and use it in `enable_job` / `disable_job` in place of their duplicated fetch blocks.
- [ ] **Step 2:** Run: `uv run pytest aaiclick/orchestration/test_registered_jobs.py -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 3:** Full suite: `uv run pytest aaiclick -q --no-cov -p no:cacheprovider` — all pass.
- [ ] **Step 4: Commit and push** — `refactor: dedupe registered-job lookup`

## Self-Review

- One construction path (`create_task`), one commit path (`commit_tasks`); no signature changes; registry semantics proven equivalent via `Task.model_post_init`.
- Placeholders: none. Types: `commit_tasks(task, job.id)` accepts a single `Task` (its `TasksType` union) — verified in orch_context.py.
