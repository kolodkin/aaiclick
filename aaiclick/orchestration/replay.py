"""
aaiclick.orchestration.replay - Task-graph replay with sampling strategies.

Clones a completed job's task graph, skips input tasks (whose persistent
outputs are reused in place), rewrites child task kwargs to point at
those persistent tables directly, and submits the clone as a new job
with ``preservation_mode=STRATEGY``. The replayed job inherits the
original's name — it's just another STRATEGY-mode run of the same
pipeline, distinguished from the original only by its fresh snowflake
id.

This is the third and final step of the lineage three-phase debugging
plan — see ``docs/lineage_3_phases.md`` for the big picture and
``docs/lineage_3_phases_implementation_plan.md`` for phase details.
"""

from __future__ import annotations

from typing import Any, NamedTuple

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlmodel import col, select

from aaiclick.data.object.refs import (
    JOB_ID,
    TASK_ID,
    is_upstream_ref,
    upstream_ref,
)
from aaiclick.oplog.sampling import SamplingStrategy
from aaiclick.snowflake_id import get_snowflake_id

from .factories import resolve_job_config
from .jobs.queries import get_job, get_tasks_for_job
from .lineage import is_input_task
from .models import (
    DEPENDENCY_TASK,
    Dependency,
    Job,
    PreservationMode,
    RunType,
    Task,
)
from .orch_context import get_sql_session


class _RewriteCtx(NamedTuple):
    """Lookup tables consumed by ``_rewrite_value`` during kwarg rewriting.

    - ``task_id_map`` — original compute task id → cloned compute task id.
    - ``input_task_refs`` — input task id → persistent Object ref to inline.
    - ``wiring_targets`` — wiring task id → that task's stored result,
      which is transitively resolved on encounter.
    """

    task_id_map: dict[int, int]
    input_task_refs: dict[int, dict[str, Any]]
    wiring_targets: dict[int, Any]


def _is_wiring_task(task: Task, *, dynamic_parent_ids: set[int]) -> bool:
    """Return ``True`` when a task is a runtime child-spawning wiring task.

    A wiring task satisfies *both* conditions:

    1. Its result carries no data — either ``None`` (the
       ``register_returned_tasks`` collapse shape) or an upstream ref
       (the rare "task returned a bare Task" shape).
    2. It has at least one downstream child that was committed *after*
       the task started running. A legitimate compute task that just
       happens to return ``None`` has no such dynamic children, so it
       stays classified as a compute task and gets cloned properly.

    ``dynamic_parent_ids`` is precomputed in one query by
    ``_load_dynamic_parent_ids`` so this predicate stays pure.
    """
    if task.id not in dynamic_parent_ids:
        return False
    if task.result is None:
        return True
    return is_upstream_ref(task.result)


def _rewrite_value(value: Any, ctx: _RewriteCtx) -> Any:
    """Recursively rewrite a serialized kwarg value for the replayed graph.

    Upstream refs are mapped per the tables below; every other shape
    (native values, Object/View refs, pydantic refs, nested lists/dicts)
    is preserved as-is.

    | Original ref target | Rewrite                               |
    |---------------------|---------------------------------------|
    | Compute task        | upstream ref with the cloned task id  |
    | Input task          | inlined persistent Object ref         |
    | Wiring task         | whatever the wiring task resolved to  |
    """
    if isinstance(value, list):
        return [_rewrite_value(v, ctx) for v in value]
    if not isinstance(value, dict):
        return value

    if is_upstream_ref(value):
        old_id = value[TASK_ID]
        if old_id in ctx.input_task_refs:
            return dict(ctx.input_task_refs[old_id])
        if old_id in ctx.wiring_targets:
            return _rewrite_value(ctx.wiring_targets[old_id], ctx)
        if old_id in ctx.task_id_map:
            return upstream_ref(ctx.task_id_map[old_id])
        raise ValueError(
            f"Upstream task {old_id} referenced by replayed task is neither "
            "an input task, a wiring task, nor a cloned compute task"
        )

    return {k: _rewrite_value(v, ctx) for k, v in value.items()}


class ReplayedJob(NamedTuple):
    """Return value of :func:`replay_job`.

    Carries both the newly committed ``Job`` row and the
    original→clone task-id mapping, so callers can find a specific
    cloned task without re-querying by entrypoint.
    """

    job: Job
    task_id_map: Dict[int, int]


async def replay_job(
    original_job_id: int,
    sampling_strategy: SamplingStrategy,
    *,
    name: Optional[str] = None,
) -> ReplayedJob:
    """Clone a completed job's task graph and re-run it under a strategy.

    Input tasks (those whose result is a persistent Object — see
    ``is_input_task``) are skipped: their persistent tables survive job
    cleanup, so the replayed children can reference them directly.
    Compute tasks are cloned with fresh snowflake IDs and their kwargs
    are rewritten so any upstream ref pointing at an input task becomes
    an inline persistent Object ref.

    The resulting Job runs under ``PreservationMode.STRATEGY`` with the
    supplied ``sampling_strategy`` and inherits the original's ``name``.
    Registered-job defaults are never consulted — replay always supplies
    both params explicitly.

    Args:
        original_job_id: Job to replay. Must exist.
        sampling_strategy: WHERE clauses that drive strategy-mode oplog
            sampling. Must be non-empty — replay without a strategy is
            pointless (it would just re-run the job with no lineage
            recording beyond NONE mode).
        name: Optional override for the new job's name. Defaults to
            the original job's name.

    Returns:
        A ``ReplayedJob`` with the newly created ``Job`` (status
        ``PENDING``, tasks + dependencies already committed) and the
        ``task_id_map`` mapping each original compute task id to its
        cloned id.

    Raises:
        ValueError: If the original job is missing, if the strategy is
            empty, or if the cloned graph would produce no compute tasks.
    """
    if not sampling_strategy:
        raise ValueError(
            "replay_job() requires a non-empty sampling_strategy — replay "
            "without a strategy carries no lineage information"
        )

    original = await get_job(original_job_id)
    if original is None:
        raise ValueError(f"Job {original_job_id} not found")

    task_rows = await get_tasks_for_job(original_job_id)

    async with get_sql_session() as session:
        dynamic_parent_ids = await _load_dynamic_parent_ids(session, original_job_id)
        dep_rows = await _load_task_dependencies(session, [t.id for t in task_rows])

    input_task_refs: dict[int, dict[str, Any]] = {}
    wiring_targets: dict[int, Any] = {}
    compute_tasks: list[Task] = []

    for task in task_rows:
        if is_input_task(task):
            # ``get_job_result`` injects job_id on read; strip it so the
            # replayed tasks don't carry the original job's id.
            ref = dict(task.result) if task.result is not None else {}
            ref.pop(JOB_ID, None)
            input_task_refs[task.id] = ref
        elif _is_wiring_task(task, dynamic_parent_ids=dynamic_parent_ids):
            wiring_targets[task.id] = task.result
        else:
            compute_tasks.append(task)

    if not compute_tasks:
        raise ValueError(
            f"Job {original_job_id} has no compute tasks to replay — "
            "every task is either an input task or a wiring task"
        )

    config = resolve_job_config(
        PreservationMode.STRATEGY, sampling_strategy, registered=None
    )

    task_id_map: dict[int, int] = {
        task.id: get_snowflake_id() for task in compute_tasks
    }
    rewrite_ctx = _RewriteCtx(
        task_id_map=task_id_map,
        input_task_refs=input_task_refs,
        wiring_targets=wiring_targets,
    )

    new_job_id = get_snowflake_id()
    new_job = Job(
        id=new_job_id,
        name=name or original.name,
        run_type=RunType.MANUAL,
        preservation_mode=config.preservation_mode,
        sampling_strategy=config.sampling_strategy,
    )

    cloned_tasks: list[Task] = [
        Task(
            id=task_id_map[original_task.id],
            job_id=new_job_id,
            entrypoint=original_task.entrypoint,
            name=original_task.name,
            kwargs=_rewrite_value(original_task.kwargs or {}, rewrite_ctx),
            max_retries=original_task.max_retries,
        )
        for original_task in compute_tasks
    ]

    cloned_deps = _clone_dependencies(dep_rows, task_id_map=task_id_map)

    async with get_sql_session() as session:
        session.add(new_job)
        session.add_all(cloned_tasks)
        session.add_all(cloned_deps)
        await session.commit()

    return ReplayedJob(job=new_job, task_id_map=task_id_map)


async def _load_dynamic_parent_ids(session: AsyncSession, job_id: int) -> set[int]:
    """Return the ids of tasks that spawned child tasks at runtime.

    A child was committed at runtime iff its ``created_at`` is on or
    after the parent's ``started_at`` — static children created by the
    job submitter always predate the parent starting to run. This join
    lets the wiring-task classifier stay a pure in-memory predicate.
    """
    parent = aliased(Task)
    child = aliased(Task)
    result = await session.execute(
        select(parent.id)
        .distinct()
        .join(Dependency, col(Dependency.previous_id) == parent.id)
        .join(child, col(Dependency.next_id) == child.id)
        .where(
            parent.job_id == job_id,
            Dependency.previous_type == DEPENDENCY_TASK,
            Dependency.next_type == DEPENDENCY_TASK,
            col(parent.started_at).isnot(None),
            col(child.created_at) >= col(parent.started_at),
        )
    )
    return {row[0] for row in result.all()}


async def _load_task_dependencies(session, task_ids: list[int]) -> list[Dependency]:
    """Fetch every task→task dependency where both endpoints are in ``task_ids``.

    Group dependencies are out of scope — replay currently operates on
    flat task graphs only. If a group-backed job is ever passed in, its
    group dependencies are silently dropped, which is fine because the
    cloned compute tasks won't reference the groups anyway.
    """
    if not task_ids:
        return []
    result = await session.execute(
        select(Dependency).where(
            Dependency.previous_type == DEPENDENCY_TASK,
            Dependency.next_type == DEPENDENCY_TASK,
            col(Dependency.previous_id).in_(task_ids),
            col(Dependency.next_id).in_(task_ids),
        )
    )
    return list(result.scalars().all())


def _clone_dependencies(
    deps: list[Dependency],
    *,
    task_id_map: dict[int, int],
) -> list[Dependency]:
    """Remap dependency endpoints onto the cloned task graph.

    Edges with either endpoint outside ``task_id_map`` are dropped —
    those endpoints belonged to an input task (whose output is already
    inlined into child kwargs) or a wiring task (whose scheduling role
    is captured by the rewritten upstream refs).
    """
    return [
        Dependency(
            previous_id=task_id_map[dep.previous_id],
            previous_type=DEPENDENCY_TASK,
            next_id=task_id_map[dep.next_id],
            next_type=DEPENDENCY_TASK,
        )
        for dep in deps
        if dep.previous_id in task_id_map and dep.next_id in task_id_map
    ]
