"""Task registry ContextVar for tracking in-memory Task/Group objects.

A fresh WeakValueDictionary is activated by orch_context (for tests and
top-level callers) and by task_scope (for per-task worker execution).
When no context is active the registry is None, and commit_tasks falls
back to committing only the explicitly passed items.
"""

from contextvars import ContextVar
from weakref import WeakValueDictionary

_task_registry_var: ContextVar[WeakValueDictionary | None] = ContextVar(
    "task_registry", default=None
)


def get_task_registry() -> WeakValueDictionary | None:
    """Return the active task registry, or None if no context is active."""
    return _task_registry_var.get()
