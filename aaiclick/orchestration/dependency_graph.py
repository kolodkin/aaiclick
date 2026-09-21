"""Successor resolution over the dependency graph.

The scheduler understands four dependency edge shapes: task→task, task→group,
group→task and group→group. A grouped task inherits the edges leaving its
group, and a group target stands for every one of its member tasks. This
module is the one place that rule is written in Python; ``DEPENDENCY_WHERE``
in ``execution/db_handler.py`` encodes the same rule in the predecessor
direction, inside the atomic claim statement.
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from .sql_utils import in_clause


async def successor_task_ids(session: AsyncSession, task_ids: set[int]) -> set[int]:
    """Task ids one dependency hop downstream of ``task_ids``.

    Follows edges leaving the tasks and the groups they belong to, expanding
    group targets to their members. The result never contains a task from
    ``task_ids``: parallel siblings are not downstream of one another.
    """
    if not task_ids:
        return set()
    ph, params = in_clause(sorted(task_ids), "t")
    edges = await session.execute(
        text(
            f"SELECT next_id, next_type FROM dependencies "
            f"WHERE (previous_type = 'task' AND previous_id IN ({ph})) "
            f"OR (previous_type = 'group' AND previous_id IN "
            f"(SELECT group_id FROM tasks WHERE id IN ({ph}) AND group_id IS NOT NULL))"
        ),
        params,
    )
    successors: set[int] = set()
    group_ids: set[int] = set()
    for next_id, next_type in edges:
        (successors if next_type == "task" else group_ids).add(next_id)
    if group_ids:
        gph, gparams = in_clause(sorted(group_ids), "g")
        members = await session.execute(text(f"SELECT id FROM tasks WHERE group_id IN ({gph})"), gparams)
        successors.update(row[0] for row in members)
    return successors - task_ids
