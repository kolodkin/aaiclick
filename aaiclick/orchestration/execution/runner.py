"""Task execution utilities for orchestration backend."""

from __future__ import annotations

import asyncio
import importlib
import inspect
import logging
import math
import os
from collections.abc import Callable
from contextlib import suppress
from typing import Any, NamedTuple

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from aaiclick.data.data_context import (
    get_ch_client,
    get_data_lifecycle,
    register_object,
)
from aaiclick.data.models import Schema
from aaiclick.data.object import LazyOperator, Object, View
from aaiclick.data.object.ingest import _get_table_schema
from aaiclick.data.object.refs import (
    CALLABLE,
    GROUP_RESULTS,
    NATIVE_VALUE,
    OBJECT,
    OBJECT_TYPE,
    PYDANTIC_DATA,
    PYDANTIC_TYPE,
    REF_TYPE,
    TASK_ID,
    UPSTREAM,
    VIEW,
    ObjectRef,
    ViewRef,
    native_value_ref,
    upstream_ref,
)
from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM, LogStream
from aaiclick.oplog.models import init_oplog_tables
from aaiclick.snowflake import get_snowflake_id

from ...datetime_utils import utc_now
from ..decorators import JobFactory, TaskFactory
from ..logging import _ChLogSink, _SinkFlusher, capture_task_output
from ..models import (
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_RUNNING,
    TASK_COMPLETED,
    TASK_FAILED,
    TASK_PENDING,
    TASK_RUNNING,
    Dependency,
    Group,
    Job,
    Task,
)
from ..orch_context import commit_tasks, get_sql_session, task_scope
from ..result import TaskResult
from ..runner_config import ENTRY_SHELL
from .db_handler import DEPENDENCY_WHERE
from .execution_worker_context import set_current_task_info

logger = logging.getLogger(__name__)


def import_callback(entrypoint: str) -> Callable:
    """
    Import a callback function from an entrypoint string.

    If the imported attribute is a TaskFactory (from @task decorator),
    unwraps it to get the original function.

    Args:
        entrypoint: Dot-separated module path and function name
                   (e.g., "mymodule.submodule.my_function")

    Returns:
        Callable: The imported function

    Raises:
        ImportError: If module cannot be imported
        AttributeError: If function not found in module
        TypeError: If the resolved attribute is not callable
    """
    parts = entrypoint.rsplit(".", 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid entrypoint format: {entrypoint}. Expected 'module.function'")

    module_path, function_name = parts
    module = importlib.import_module(module_path)
    attr = getattr(module, function_name)

    if isinstance(attr, TaskFactory):
        return attr.func

    if isinstance(attr, JobFactory):
        return attr.func

    if not callable(attr):
        raise TypeError(f"entrypoint '{entrypoint}' does not resolve to a callable")

    return attr


def _import_class(type_path: str) -> type:
    """Import a class from a dotted module path (e.g. 'mymodule.MyClass')."""
    module_path, class_name = type_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, class_name)


async def _resolve_upstream_ref(ref: dict, session: AsyncSession) -> Any:
    """Resolve an upstream task reference to its result.

    Args:
        ref: Dict with ref_type="upstream" and task_id
        session: Database session for querying task result

    Returns:
        The upstream task's result (Object, View, or native value)

    Raises:
        ValueError: If upstream task not found or not completed
    """
    task_id = ref[TASK_ID]
    db_result = await session.execute(select(Task.result, Task.status).where(Task.id == task_id))
    row = db_result.one_or_none()

    if row is None:
        raise ValueError(f"Upstream task {task_id} not found")

    result, status = row
    if status != TASK_COMPLETED:
        raise ValueError(f"Upstream task {task_id} is not completed (status: {status})")

    if result is None:
        return None

    return result


async def _deserialize_value(value: Any, session: AsyncSession) -> Any:
    """Recursively deserialize a value from JSON format.

    Handles:
    - Upstream references (ref_type="upstream"): Resolves to task result
    - Object references (object_type="object"): Reconstructs Object
    - View references (object_type="view"): Reconstructs View
    - Lists/dicts: Recursively deserializes contents
    - Native Python types: Passed through unchanged

    Args:
        value: Serialized value from task kwargs
        session: Database session for resolving upstream refs

    Returns:
        Deserialized value ready for function call
    """
    if not isinstance(value, dict):
        if isinstance(value, list):
            return [await _deserialize_value(v, session) for v in value]
        return value

    if value.get(REF_TYPE) == UPSTREAM:
        upstream_result = await _resolve_upstream_ref(value, session)
        return await _deserialize_value(upstream_result, session)

    if value.get(REF_TYPE) == CALLABLE:
        return import_callback(value["entrypoint"])

    # from reduce() collecting map results
    if value.get(REF_TYPE) == GROUP_RESULTS:
        group_id = value["group_id"]
        result = await session.execute(
            select(Task.result)
            .where(
                Task.group_id == group_id,
                Task.status == TASK_COMPLETED,
            )
            .order_by(col(Task.id))
        )
        rows = result.all()
        deserialized = []
        for (task_result,) in rows:
            deserialized.append(await _deserialize_value(task_result, session))
        return deserialized

    if NATIVE_VALUE in value and len(value) == 1:
        return value[NATIVE_VALUE]

    if PYDANTIC_TYPE in value:
        cls = _import_class(value[PYDANTIC_TYPE])
        return cls.model_validate(value[PYDANTIC_DATA])

    if OBJECT_TYPE in value:
        obj_type = value[OBJECT_TYPE]

        if obj_type == OBJECT:
            ref = ObjectRef.model_validate(value)
            fieldtype, columns = await _get_table_schema(ref.table, get_ch_client())
            schema = Schema(fieldtype=fieldtype, columns=columns)
            obj = Object(table=ref.table, schema=schema)
            if not ref.persistent:
                obj._register()  # enqueues INCREF
            register_object(obj)
            if not ref.persistent:
                lifecycle = get_data_lifecycle()
                if lifecycle is not None:
                    lifecycle.unpin(ref.table)  # FIFO: after INCREF
            return obj

        elif obj_type == VIEW:
            ref = ViewRef.model_validate(value)
            fieldtype, columns = await _get_table_schema(ref.table, get_ch_client())
            schema = Schema(fieldtype=fieldtype, columns=columns)
            source = Object(table=ref.table, schema=schema)
            source._register()  # enqueues INCREF
            register_object(source)
            view = View(
                source=source,
                where=ref.where,
                limit=ref.limit,
                offset=ref.offset,
                order_by=ref.order_by,
                selected_fields=ref.selected_fields,
                renamed_columns=ref.renamed_columns,
            )
            register_object(view)
            lifecycle = get_data_lifecycle()
            if lifecycle is not None:
                lifecycle.unpin(ref.table)  # FIFO: after INCREF
            return view

        else:
            raise ValueError(f"Unknown object_type: {obj_type}")

    # Regular dict - recursively deserialize values
    return {k: await _deserialize_value(v, session) for k, v in value.items()}


async def deserialize_task_params(serialized_params: dict) -> dict:
    """
    Deserialize task parameters from JSON format.

    Supports:
    - Upstream references: Resolved to completed task results
    - Object/View references: Reconstructed and registered with DataContext
    - Native Python values: Passed through unchanged
    - Nested structures: Lists and dicts are recursively processed

    Args:
        serialized_params: Task kwargs from database (JSON-deserialized)

    Returns:
        dict: Deserialized kwargs ready for function call

    Raises:
        ValueError: Unknown object_type or upstream task not completed
    """
    if not serialized_params:
        return {}

    async with get_sql_session() as session:
        return {k: await _deserialize_value(v, session) for k, v in serialized_params.items()}


async def register_run(task_id: int) -> int:
    """Mint a per-attempt snowflake run_id and record the attempt on the Task.

    Appends the new run_id to ``run_ids`` and ``TASK_RUNNING`` to
    ``run_statuses``. Shared by ``execute_task`` (module tasks, in the task
    process) and the shell vehicles (host side), so every attempt — module or
    shell — keys its ClickHouse ``task_logs`` rows by a registered run_id.
    """
    run_id = get_snowflake_id()
    async with get_sql_session() as session:
        result = await session.execute(select(Task).where(Task.id == task_id))
        db_task = result.scalar_one_or_none()
        if db_task is not None:
            db_task.run_ids = [*db_task.run_ids, run_id]
            db_task.run_statuses = [*db_task.run_statuses, TASK_RUNNING]
            session.add(db_task)
            await session.commit()
    return run_id


async def execute_task(task: Task, shell_spec: ShellSpec | None = None) -> Any:
    """
    Execute a single task inside orch_context.

    Generates a per-attempt run_id (snowflake) and appends it to the Task's
    run_ids/run_statuses arrays; the attempt's output streams to CH
    ``task_logs`` under that run_id.

    Returns:
        The task result. Caller is responsible for updating run_statuses to
        the final status.

    Shell tasks (``entry_type="shell"``) delegate to
    :func:`execute_shell_task` — every in-process caller gets the module/shell
    routing from this one place. ``shell_spec`` is the dispatch-resolved
    launch command for shell tasks; ``None`` runs the task's own argv.
    """
    if task.entry_type == ENTRY_SHELL:
        return await execute_shell_task(task, spec=shell_spec)

    func = import_callback(task.entrypoint)
    run_id = await register_run(task.id)

    set_current_task_info(task_id=task.id, job_id=task.job_id, image_source=task.image_source)

    async with capture_task_output(task.id, task.job_id, run_id):
        async with task_scope(
            task_id=task.id,
            job_id=task.job_id,
            run_id=run_id,
        ):
            kwargs = await deserialize_task_params(task.kwargs)
            if inspect.iscoroutinefunction(func):
                result = await func(**kwargs)
            else:
                result = func(**kwargs)

            # A task may return a LazyOperator (e.g. ``return a + b`` without an
            # explicit await). Materialize before serialization — orch's
            # ``_serialize_ref`` is sync and needs a real ``.table``.
            result = await _materialize_lazies(result)

            # Pin result table for all downstream consumer tasks.
            # At this point we know the table_name (from the Object) but
            # not which tasks will consume it — that's only known via the
            # dependencies table. The PIN handler fans out: queries
            # downstream task_ids and inserts one pin_ref per consumer.
            pin_target = result.data if isinstance(result, TaskResult) else result
            if isinstance(pin_target, (Object, View)) and not pin_target.persistent:
                lifecycle = get_data_lifecycle()
                if lifecycle is not None:
                    lifecycle.pin(pin_target.table)

            # Register returned tasks INSIDE task_scope so the registry
            # (ContextVar) that was populated during func() is still active.
            data_result = await register_returned_tasks(result, task.id, task.job_id)

    return data_result


async def start_shell_process(
    command: list[str] | None, command_env: dict[str, str] | None
) -> asyncio.subprocess.Process:
    """Spawn a shell task's argv with ``command_env`` overlaid on the worker env.

    stdout and stderr are separate pipes so captured lines keep their source
    stream (the docker / kubectl wrappers preserve the split for non-TTY
    runs). :func:`execute_shell_task` pumps both.
    """
    env = {**os.environ, **command_env} if command_env else None
    return await asyncio.create_subprocess_exec(
        *(command or []),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=env,
    )


class ShellSpec(NamedTuple):
    """A shell task's fully-resolved launch command.

    Built host-side (``dispatch.build_shell_spec``) so runner-specific
    wrapping (``docker run`` / ``kubectl run``) stays out of the executing
    process. ``cleanup_argv`` is run after the process ends however it ends —
    killing the wrapper CLI alone would leave its container/pod running."""

    argv: list[str]
    env: dict[str, str] | None
    cleanup_argv: list[str] | None = None


async def _run_cleanup_argv(cleanup_argv: list[str]) -> None:
    """Best-effort teardown command (``docker kill`` / ``kubectl delete``).

    Failure is expected on the happy path (the ``--rm`` container is already
    gone) — output and exit code are deliberately ignored."""
    proc = await asyncio.create_subprocess_exec(
        *cleanup_argv,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    await proc.wait()


async def _pump_stream(stream: asyncio.StreamReader, sink: _ChLogSink, source: LogStream) -> None:
    """Feed one output pipe into the sink chunk by chunk until EOF."""
    while True:
        chunk = await stream.read(65536)
        if not chunk:
            return
        sink.write(source, chunk.decode(errors="replace"))


async def execute_shell_task(task: Task, spec: ShellSpec | None = None) -> None:
    """Run a shell task's argv, streaming its output to CH ``task_logs``.

    One code path for every shell runner: the in-process worker and
    ``job_test`` call it directly (``spec=None`` → the task's own argv), and
    the mp worker's task child calls it with a dispatch-built ``ShellSpec``
    (host argv, or ``docker run`` / ``kubectl run`` wrapped). Output is
    drained to ``task_logs`` every ``LOG_FLUSH_INTERVAL`` seconds and finally
    at exit, so long-running commands are tailed live.

    Returns None (shell tasks have no result). Raises ``RuntimeError`` on
    nonzero exit, matching the ``"exit <code>"`` error the workers report.
    """
    if spec is None:
        spec = ShellSpec(task.command or [], task.command_env)
    run_id = await register_run(task.id)
    # A shell-only job on a fresh DB may not have run task_scope's
    # init_oplog_tables yet; bring the schema up before streaming.
    try:
        await init_oplog_tables(get_ch_client())
    except Exception:
        logger.error("Failed to ensure task_logs for task %s run %s", task.id, run_id, exc_info=True)

    proc = await start_shell_process(spec.argv, spec.env)
    sink = _ChLogSink()
    flusher = _SinkFlusher(sink, task.id, task.job_id, run_id)
    flusher_task = asyncio.create_task(flusher.run())
    readers = [
        asyncio.create_task(_pump_stream(proc.stdout, sink, STDOUT_STREAM)),
        asyncio.create_task(_pump_stream(proc.stderr, sink, STDERR_STREAM)),
    ]
    try:
        await proc.wait()
        for reader in readers:
            await reader
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        raise
    finally:
        for reader in readers:
            reader.cancel()
            with suppress(asyncio.CancelledError):
                await reader
        flusher.request_stop()
        await flusher_task
        await flusher.flush_final()
        if spec.cleanup_argv:
            await _run_cleanup_argv(spec.cleanup_argv)
    if proc.returncode != 0:
        raise RuntimeError(f"exit {proc.returncode}")


def _sanitize_for_json(value: Any) -> Any:
    """Replace NaN/Inf floats with None for JSON compatibility."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {k: _sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_for_json(v) for v in value]
    return value


async def _materialize_lazies(value: Any) -> Any:
    """Recursively materialize any ``LazyOperator`` in a task return value.

    A ``@task`` body may return ``a + b`` (a LazyOperator) without an
    explicit ``await``. Orch's downstream ``_serialize_ref`` reads ``.table``
    sync — which raises on an unmaterialized LazyOperator. Walking the
    return value once at the orch boundary makes the unawaited shorthand
    a first-class pattern: tasks compose with operator results just as
    naturally as with materialized Objects.

    Walks ``TaskResult.data``, lists, tuples, and dicts. ``Task`` / ``Group``
    items are left untouched (they're DAG nodes, not data).
    """
    if isinstance(value, LazyOperator):
        return await value._materialize()
    if isinstance(value, TaskResult):
        data = await _materialize_lazies(value.data)
        if data is value.data:
            return value
        return TaskResult(data=data, tasks=value.tasks)
    if isinstance(value, list):
        return [await _materialize_lazies(v) for v in value]
    if isinstance(value, tuple):
        return tuple([await _materialize_lazies(v) for v in value])
    if isinstance(value, dict):
        return {k: await _materialize_lazies(v) for k, v in value.items()}
    return value


def serialize_task_result(result: Any, job_id: int) -> dict | None:
    """
    Serialize a task result to JSON-storable format.

    Handles Object and View types by creating reference dicts.

    Args:
        result: Task function return value
        job_id: ID of the job this task belongs to

    Returns:
        dict: Serialized result reference, or None if result is None
    """
    if result is None:
        return None

    if isinstance(result, Task):
        return upstream_ref(result.id)

    if isinstance(result, Object):
        ref = result._serialize_ref()
        ref["job_id"] = job_id
        return ref

    if isinstance(result, BaseModel):
        return {
            PYDANTIC_TYPE: f"{type(result).__module__}.{type(result).__qualname__}",
            PYDANTIC_DATA: result.model_dump(),
        }

    return native_value_ref(_sanitize_for_json(result))


def _flatten_item(item: Any) -> list:
    """Recursively extract Task/Group items, including Group._tasks."""
    if isinstance(item, Task):
        return [item]

    if isinstance(item, Group):
        return [item, *item.get_tasks()]

    if isinstance(item, (list, tuple)):
        result = []
        for sub in item:
            result.extend(_flatten_item(sub))
        return result

    return []


async def register_returned_tasks(result: Any, parent_task_id: int, job_id: int) -> Any:
    """Register dynamic child tasks returned from a task function.

    Handles three return shapes:
    - None                          → no tasks, return None
    - TaskResult(data, tasks)       → register .tasks, return .data
    - list/tuple of Task/Group      → register all, return None
    - (Object, list[Group])         → register groups from [1], return [0]
    - Any other value               → pure data, return as-is

    Args:
        result: The raw return value from the task function
        parent_task_id: ID of the task that returned this result
        job_id: ID of the job these tasks belong to

    Returns:
        The data portion of the result for serialization, or None.
    """
    if result is None:
        return None

    elif isinstance(result, TaskResult):
        task_items = _flatten_item(result.tasks)
        data_result = result.data
    elif isinstance(result, (Task, Group)):
        # Job entry tasks can return a single Task or Group directly
        task_items = _flatten_item(result)
        data_result = None
    else:
        return result

    if not task_items:
        return data_result

    # Wire dependency: each returned item depends on the parent task
    for item in task_items:
        if isinstance(item, (Task, Group)):
            dep = Dependency(
                previous_id=parent_task_id,
                previous_type="task",
                next_id=item.id,
                next_type="task" if isinstance(item, Task) else "group",
            )
            item.previous_dependencies.append(dep)

    await commit_tasks(task_items, job_id)

    return data_result


_READY_TASK_SQL = f"""
    SELECT t.id FROM tasks t
    JOIN jobs j ON t.job_id = j.id
    WHERE t.job_id = :job_id
    AND t.status = :pending_status
    AND (t.retry_after IS NULL OR t.retry_after <= :now)
    {DEPENDENCY_WHERE}
    ORDER BY t.id ASC
    LIMIT 1
"""


async def run_job_tasks(job: Job) -> None:
    """
    Execute all tasks for a job synchronously (test mode).

    This simulates worker behavior but runs in the current process.
    Tasks are fetched and executed one at a time, respecting dependencies.
    Supports dynamic task creation (tasks returned by other tasks).

    Args:
        job: Job whose tasks to execute

    Raises:
        Exception: If any task fails
    """
    async with get_sql_session() as session:
        # Update job to RUNNING
        job.status = JOB_RUNNING
        job.started_at = utc_now()
        session.add(job)
        await session.commit()

    job_failed = False
    error_msg = None

    # Fetch and execute one task at a time until no more ready tasks
    while True:
        async with get_sql_session() as session:
            # Fetch next ready task (dependency-aware)
            now = utc_now()
            result = await session.execute(
                text(_READY_TASK_SQL),
                {
                    "job_id": job.id,
                    "pending_status": TASK_PENDING,
                    "completed_status": TASK_COMPLETED,
                    "now": now,
                },
            )
            row = result.fetchone()

            if row is None:
                break

            task_id = row[0]

            # Fetch the full task and update to RUNNING
            db_result = await session.execute(select(Task).where(Task.id == task_id))
            task = db_result.scalar_one()

            task.status = TASK_RUNNING
            task.started_at = now
            session.add(task)
            await session.commit()

            task_job_id = task.job_id

        try:
            data_result = await execute_task(task)

            result_ref = serialize_task_result(data_result, task_job_id)

            async with get_sql_session() as session:
                db_result = await session.execute(select(Task).where(Task.id == task_id))
                task = db_result.scalar_one()

                task.status = TASK_COMPLETED
                task.completed_at = utc_now()
                task.result = result_ref
                if task.run_statuses:
                    task.run_statuses = [*task.run_statuses[:-1], TASK_COMPLETED]

                session.add(task)
                await session.commit()

        except Exception as e:
            job_failed = True
            error_msg = str(e)
            logger.exception("Task %r failed: %s", task.name, e)

            async with get_sql_session() as session:
                db_result = await session.execute(select(Task).where(Task.id == task_id))
                task = db_result.scalar_one()

                task.status = TASK_FAILED
                task.completed_at = utc_now()
                task.error = str(e)
                if task.run_statuses:
                    task.run_statuses = [*task.run_statuses[:-1], TASK_FAILED]
                session.add(task)
                await session.commit()

            break

    async with get_sql_session() as session:
        # Reload job and update final status
        result = await session.execute(select(Job).where(Job.id == job.id))
        db_job = result.scalar_one()

        if job_failed:
            db_job.status = JOB_FAILED
            db_job.error = error_msg
        else:
            db_job.status = JOB_COMPLETED

        db_job.completed_at = utc_now()
        session.add(db_job)
        await session.commit()

        # Update in-memory job object
        job.status = db_job.status
        job.completed_at = db_job.completed_at
        job.error = db_job.error
