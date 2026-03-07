"""Handle types for orchestration graph construction.

Provides MapHandle, a job-definition-time placeholder for dynamic map
operations. MapHandle is used in @job function bodies to wire dependencies
between tasks and groups — workers never see MapHandle objects.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Union

from .models import Group, Task


@dataclass
class MapHandle:
    """Represents a pending map operation.

    Holds the expander task (creates child tasks at runtime) and the output
    group (contains all dynamically created partition tasks). Used as input
    to reduce() and for dependency wiring.
    """

    expander: Task
    group: Group

    def depends_on(self, other: Union[Task, Group]) -> MapHandle:
        """Declare that this map operation depends on a task or group."""
        self.expander.depends_on(other)
        return self

    def __rshift__(self, other):
        """MapHandle >> B: B depends on all map tasks (via group)."""
        if isinstance(other, list):
            for item in other:
                item.depends_on(self.group)
            return other
        else:
            other.depends_on(self.group)
            return other

    def __rrshift__(self, other):
        """[A, B] >> MapHandle: expander depends on A and B."""
        if isinstance(other, list):
            for item in other:
                self.expander.depends_on(item)
        else:
            self.expander.depends_on(other)
        return self
