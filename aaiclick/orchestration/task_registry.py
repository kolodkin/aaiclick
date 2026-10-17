"""Task registry ContextVar for tracking in-memory Task/Group objects.

A fresh dict is activated by orch_context (for tests and top-level callers)
and by task_scope (for per-task worker execution).
When no context is active the registry is None, and commit_tasks falls
back to committing only the explicitly passed items.

A plain dict (not WeakValueDictionary) is used intentionally: task objects
created inside a @task function are locals that go out of scope when the
function returns, before commit_tasks can walk the registry. A plain dict
holds strong references so tasks survive until after commit_tasks runs.
Only in-memory tasks awaiting commit belong here. orch_context's registry
lives as long as the worker loop, so tasks loaded from the DB (e.g. claimed
tasks) must be ORM-loaded, which skips model_post_init, never built with
Task(**row) — otherwise every claimed task leaks into it.
"""

from contextvars import ContextVar

_task_registry_var: ContextVar[dict | None] = ContextVar("task_registry", default=None)


def get_task_registry() -> dict | None:
    """Return the active task registry, or None if no context is active."""
    return _task_registry_var.get()


def register_task(task_id: int, task) -> None:
    """Register a task in the active registry, if one is active."""
    registry = _task_registry_var.get()
    if registry is not None:
        registry[task_id] = task
