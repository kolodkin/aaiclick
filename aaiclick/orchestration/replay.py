"""
aaiclick.orchestration.replay - Task-graph replay with sampling strategies.

Clones a completed job's task graph, skips input tasks (whose persistent
outputs are reused in place), rewrites child task kwargs to point at
those persistent tables directly, and submits the clone as a new job
with ``preservation_mode=STRATEGY`` and ``replay_of`` pointing at the
original job.

This is the third and final step of the lineage three-phase debugging
plan — see ``docs/lineage_3_phases.md`` for the big picture and
``docs/lineage_3_phases_implementation_plan.md`` for phase details.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Optional

from sqlmodel import select

from aaiclick.oplog.sampling import SamplingStrategy
from aaiclick.snowflake_id import get_snowflake_id

from .factories import resolve_job_config
from .lineage import is_input_task
from .models import Dependency, Job, JobStatus, PreservationMode, RunType, Task, TaskStatus
from .orch_context import get_sql_session


def _is_wiring_task(task: Task) -> bool:
    """Return ``True`` when a task produced no data of its own.

    Wiring tasks are job entry points that spawn child tasks at runtime
    (``@job`` / ``TaskResult(tasks=[...])``). ``register_returned_tasks``
    commits the children and collapses the parent's stored result to
    ``None``. Less commonly, a task may also return a bare ``Task``
    reference, which serializes to an upstream ref.

    Either shape is a wiring task: it does no data work itself, so
    cloning + re-executing would re-spawn children with fresh IDs that
    would clash with the replay's cloned compute tasks.
    """
    if task.result is None:
        return True
    return (
        isinstance(task.result, dict)
        and task.result.get("ref_type") == "upstream"
    )


def _persistent_ref_from_input_task(task: Task) -> Dict[str, Any]:
    """Extract a persistent Object ref from an input task's stored result.

    The caller has already verified ``is_input_task(task)`` so we know
    ``task.result`` is a dict shaped like::

        {"object_type": "object", "table": "p_...", "persistent": true, ...}

    Returns a fresh dict (no ``job_id`` — replay deserialization looks up
    the current job).
    """
    result = dict(task.result)
    result.pop("job_id", None)
    return result


def _rewrite_value(
    value: Any,
    *,
    task_id_map: Dict[int, int],
    input_task_refs: Dict[int, Dict[str, Any]],
    wiring_targets: Dict[int, Any],
) -> Any:
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
        return [
            _rewrite_value(
                v,
                task_id_map=task_id_map,
                input_task_refs=input_task_refs,
                wiring_targets=wiring_targets,
            )
            for v in value
        ]
    if not isinstance(value, dict):
        return value

    if value.get("ref_type") == "upstream":
        old_id = value["task_id"]
        if old_id in input_task_refs:
            return _persistent_ref_from_input_task_dict(input_task_refs[old_id])
        if old_id in wiring_targets:
            return _rewrite_value(
                wiring_targets[old_id],
                task_id_map=task_id_map,
                input_task_refs=input_task_refs,
                wiring_targets=wiring_targets,
            )
        if old_id in task_id_map:
            return {"ref_type": "upstream", "task_id": task_id_map[old_id]}
        raise ValueError(
            f"Upstream task {old_id} referenced by replayed task is neither "
            "an input task, a wiring task, nor a cloned compute task"
        )

    return {
        k: _rewrite_value(
            v,
            task_id_map=task_id_map,
            input_task_refs=input_task_refs,
            wiring_targets=wiring_targets,
        )
        for k, v in value.items()
    }


def _persistent_ref_from_input_task_dict(ref: Dict[str, Any]) -> Dict[str, Any]:
    """Shallow copy of an already-extracted persistent ref.

    Kept separate from ``_persistent_ref_from_input_task`` so the per-task
    extraction only runs once (in ``replay_job``) while per-reference
    rewrites in ``_rewrite_value`` get fresh dicts.
    """
    return dict(ref)


async def replay_job(
    original_job_id: int,
    sampling_strategy: SamplingStrategy,
    *,
    name: Optional[str] = None,
) -> Job:
    """Clone a completed job's task graph and re-run it under a strategy.

    Input tasks (those whose result is a persistent Object — see
    ``is_input_task``) are skipped: their persistent tables survive job
    cleanup, so the replayed children can reference them directly.
    Compute tasks are cloned with fresh snowflake IDs and their kwargs
    are rewritten so any upstream ref pointing at an input task becomes
    an inline persistent Object ref.

    The resulting Job runs under ``PreservationMode.STRATEGY`` with the
    supplied ``sampling_strategy``, and its ``replay_of`` column points
    at ``original_job_id``. Registered-job defaults are never consulted —
    replay always supplies both params explicitly.

    Args:
        original_job_id: Job to replay. Must exist.
        sampling_strategy: WHERE clauses that drive strategy-mode oplog
            sampling. Must be non-empty — replay without a strategy is
            pointless (it would just re-run the job with no lineage
            recording beyond NONE mode).
        name: Optional override for the new job's name. Defaults to
            ``"replay_of_{original_job_id}"``.

    Returns:
        The newly created ``Job`` row with status ``PENDING``. Tasks +
        dependencies have already been committed to the database.

    Raises:
        ValueError: If the original job is missing, if the strategy is
            empty, or if the cloned graph would produce no compute tasks.
    """
    if not sampling_strategy:
        raise ValueError(
            "replay_job() requires a non-empty sampling_strategy — replay "
            "without a strategy carries no lineage information"
        )

    async with get_sql_session() as session:
        original = (
            await session.execute(select(Job).where(Job.id == original_job_id))
        ).scalar_one_or_none()
        if original is None:
            raise ValueError(f"Job {original_job_id} not found")

        task_rows = (
            await session.execute(select(Task).where(Task.job_id == original_job_id))
        ).scalars().all()
        dep_rows = await _load_task_dependencies(session, [t.id for t in task_rows])

    input_task_refs: Dict[int, Dict[str, Any]] = {}
    wiring_targets: Dict[int, Any] = {}
    compute_tasks: list[Task] = []

    for task in task_rows:
        if is_input_task(task):
            input_task_refs[task.id] = _persistent_ref_from_input_task(task)
        elif _is_wiring_task(task):
            wiring_targets[task.id] = task.result
        else:
            compute_tasks.append(task)

    if not compute_tasks:
        raise ValueError(
            f"Job {original_job_id} has no compute tasks to replay — "
            "every task is either an input task or a wiring task"
        )

    # Validate resolved config up front so we fail before inserting anything.
    config = resolve_job_config(
        PreservationMode.STRATEGY, sampling_strategy, registered=None
    )

    task_id_map: Dict[int, int] = {
        task.id: get_snowflake_id() for task in compute_tasks
    }

    new_job_id = get_snowflake_id()
    new_job = Job(
        id=new_job_id,
        name=name or f"replay_of_{original_job_id}",
        status=JobStatus.PENDING,
        run_type=RunType.MANUAL,
        registered_job_id=None,
        preservation_mode=config.preservation_mode,
        sampling_strategy=config.sampling_strategy,
        replay_of=original_job_id,
        created_at=datetime.utcnow(),
    )

    cloned_tasks: list[Task] = []
    for original_task in compute_tasks:
        new_kwargs = _rewrite_value(
            original_task.kwargs or {},
            task_id_map=task_id_map,
            input_task_refs=input_task_refs,
            wiring_targets=wiring_targets,
        )
        cloned_tasks.append(
            Task(
                id=task_id_map[original_task.id],
                job_id=new_job_id,
                entrypoint=original_task.entrypoint,
                name=original_task.name,
                kwargs=new_kwargs,
                status=TaskStatus.PENDING,
                created_at=datetime.utcnow(),
                max_retries=original_task.max_retries,
            )
        )

    cloned_deps = _clone_dependencies(
        dep_rows,
        task_id_map=task_id_map,
        input_task_ids=set(input_task_refs.keys()),
        wiring_task_ids=set(wiring_targets.keys()),
    )

    async with get_sql_session() as session:
        session.add(new_job)
        for task in cloned_tasks:
            session.add(task)
        for dep in cloned_deps:
            session.add(dep)
        await session.commit()

    return new_job


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
            Dependency.previous_type == "task",
            Dependency.next_type == "task",
            Dependency.previous_id.in_(task_ids),
            Dependency.next_id.in_(task_ids),
        )
    )
    return list(result.scalars().all())


def _clone_dependencies(
    deps: list[Dependency],
    *,
    task_id_map: Dict[int, int],
    input_task_ids: set[int],
    wiring_task_ids: set[int],
) -> list[Dependency]:
    """Remap dependency endpoints onto the cloned task graph.

    Edges terminating on (or originating from) an input task are dropped
    because the input task's output has already been inlined into the
    child's kwargs via ``_rewrite_value``. Edges touching a wiring task
    are likewise dropped — the wiring task is skipped and its scheduling
    effect is captured directly by the rewritten upstream refs.
    """
    cloned: list[Dependency] = []
    skip = input_task_ids | wiring_task_ids
    for dep in deps:
        if dep.previous_id in skip or dep.next_id in skip:
            continue
        if dep.previous_id not in task_id_map or dep.next_id not in task_id_map:
            continue
        cloned.append(
            Dependency(
                previous_id=task_id_map[dep.previous_id],
                previous_type="task",
                next_id=task_id_map[dep.next_id],
                next_type="task",
            )
        )
    return cloned
