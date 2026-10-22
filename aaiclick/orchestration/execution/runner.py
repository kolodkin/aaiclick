"""Task execution utilities for orchestration backend.

See docs/designs/task_interface.md — "Vocabulary", "Returned tasks and handles", "Where it runs".
"""

from __future__ import annotations

import asyncio
import codecs
import importlib
import inspect
import logging
import math
import os
import re
import shutil
import tempfile
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, NamedTuple

from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import col, select

from aaiclick.data.models import Schema
from aaiclick.data.object import LazyOperator, Object, View
from aaiclick.data.object.refs import (
    AAICLICK_TYPE,
    CALLABLE,
    DATA,
    DEPENDS_ON,
    ENTRYPOINT,
    GROUP,
    GROUP_ID,
    GROUP_RESULTS,
    HANDLE,
    NATIVE,
    OBJECT,
    PERSISTENT,
    PYDANTIC,
    PYDANTIC_CLASS,
    PYDANTIC_DATA,
    TASK,
    TASK_ID,
    TASK_RESULT,
    TASKS,
    UPSTREAM,
    VALUE,
    VIEW,
    ObjectRef,
    ViewRef,
    native_ref,
    pydantic_ref,
    ref_kind,
    upstream_ref,
)
from aaiclick.data.object_context import (
    get_data_lifecycle,
    register_object,
)
from aaiclick.data.object_context.lifecycle import read_table_schema
from aaiclick.log_models import STDERR_STREAM, STDOUT_STREAM, LogStream
from aaiclick.snowflake import get_snowflake_id

from ...datetime_utils import utc_now
from ..decorators import JobFactory, TaskFactory
from ..dependency_graph import successor_edges
from ..factories import create_task
from ..logging import ChLogSink, capture_task_output, stream_to_task_logs
from ..models import (
    DEPENDENCY_TASK,
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
from ..runner_config import ENTRY_JVM, ENTRY_MODULE, ENTRY_SHELL, EntryType, ImageBuild, validate_task_entry
from ..task_registry import fresh_task_registry
from .claiming import update_job_status, update_task_status
from .cli import overlay_env
from .db_handler import DEPENDENCY_WHERE
from .execution_worker_context import TaskInfo, current_task_info
from .io_plane import IO_DIR_ENV, read_out_json, write_in_json
from .runner_env import ENV_WORKER_RUNNER, WorkerRunner

logger = logging.getLogger(__name__)

# How long a vehicle's output follower may keep draining before it is
# cancelled: after a clean exit (a wedged ``docker logs -f`` must not stall
# cleanup), and after a vehicle error (timeout, vanished Pod), where the
# container may still be running until ``cleanup`` removes it.
OUTPUT_FOLLOWER_DRAIN_TIMEOUT = 10.0
OUTPUT_FOLLOWER_ERROR_GRACE = 1.0


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


async def _resolve_upstream_ref(ref: dict, session: AsyncSession, job_id: int) -> Any:
    """Resolve an upstream task reference to its result.

    Args:
        ref: An ``upstream`` ref dict with ``task_id``
        session: Database session for querying task result
        job_id: The consumer's job — a ref never reaches another job's task

    Returns:
        The upstream task's result (Object, View, or native value)

    Raises:
        ValueError: If upstream task not found or not completed
    """
    task_id = ref[TASK_ID]
    db_result = await session.execute(select(Task.result, Task.status).where(Task.id == task_id, Task.job_id == job_id))
    row = db_result.one_or_none()

    if row is None:
        raise ValueError(f"Upstream task {task_id} not found in job {job_id}")

    result, status = row
    if status != TASK_COMPLETED:
        raise ValueError(f"Upstream task {task_id} is not completed (status: {status})")

    if result is None:
        return None

    return result


async def _deserialize_value(value: Any, session: AsyncSession, job_id: int) -> Any:
    """Recursively deserialize a value from JSON format.

    Dispatches on :func:`~aaiclick.data.object.refs.ref_kind`: upstream refs
    resolve to the task result, Object/View refs are reconstructed, lists and
    plain dicts recurse, anything else passes through.

    Args:
        value: Serialized value from task kwargs
        session: Database session for resolving upstream refs
        job_id: Job the refs must belong to

    Returns:
        Deserialized value ready for function call
    """
    if isinstance(value, list):
        return [await _deserialize_value(v, session, job_id) for v in value]

    kind = ref_kind(value)

    if kind == UPSTREAM:
        upstream_result = await _resolve_upstream_ref(value, session, job_id)
        return await _deserialize_value(upstream_result, session, job_id)

    if kind == CALLABLE:
        return import_callback(value["entrypoint"])

    # A Group passed as a kwarg: the members' results, in task order
    if kind == GROUP_RESULTS:
        group_id = value["group_id"]
        result = await session.execute(
            select(Task.result)
            .where(
                Task.group_id == group_id,
                Task.job_id == job_id,
                Task.status == TASK_COMPLETED,
            )
            .order_by(col(Task.id))
        )
        rows = result.all()
        deserialized = []
        for (task_result,) in rows:
            deserialized.append(await _deserialize_value(task_result, session, job_id))
        return deserialized

    if kind == NATIVE:
        return value[VALUE]

    if kind == PYDANTIC:
        cls = _import_class(value[PYDANTIC_CLASS])
        return cls.model_validate(value[PYDANTIC_DATA])

    if kind == OBJECT:
        ref = ObjectRef.model_validate(value)
        fieldtype, columns = await read_table_schema(ref.table)
        schema = Schema(fieldtype=fieldtype, columns=columns)
        obj = Object(table=ref.table, schema=schema)
        if not ref.persistent:
            obj._register()  # enqueues INCREF
        register_object(obj)
        return obj

    if kind == VIEW:
        ref = ViewRef.model_validate(value)
        fieldtype, columns = await read_table_schema(ref.table)
        schema = Schema(fieldtype=fieldtype, columns=columns)
        source = Object(table=ref.table, schema=schema)
        source._register()  # enqueues INCREF
        register_object(source)
        view = View.from_schema(source, ref.view_schema)
        register_object(view)
        return view

    if not isinstance(value, dict):
        return value
    # Regular dict - recursively deserialize values
    return {k: await _deserialize_value(v, session, job_id) for k, v in value.items()}


async def deserialize_task_params(serialized_params: dict, job_id: int) -> dict:
    """
    Deserialize task parameters from JSON format.

    Supports:
    - Upstream references: Resolved to completed task results
    - Object/View references: Reconstructed and registered with ObjectContext
    - Native Python values: Passed through unchanged
    - Nested structures: Lists and dicts are recursively processed

    Args:
        serialized_params: Task kwargs from database (JSON-deserialized)
        job_id: The task's job; upstream refs resolve only within it

    Returns:
        dict: Deserialized kwargs ready for function call

    Raises:
        ValueError: Unknown ``_aaiclick_type`` or upstream task not completed
    """
    if not serialized_params:
        return {}

    async with get_sql_session() as session:
        return {k: await _deserialize_value(v, session, job_id) for k, v in serialized_params.items()}


async def _resolve_plain_value(name: str, value: Any, session: AsyncSession, job_id: int) -> Any:
    if isinstance(value, list):
        return [await _resolve_plain_value(name, v, session, job_id) for v in value]
    kind = ref_kind(value)
    if kind == UPSTREAM:
        return await _resolve_plain_value(name, await _resolve_upstream_ref(value, session, job_id), session, job_id)
    if kind == NATIVE:
        return value[VALUE]
    if kind is not None:
        raise ValueError(f"kwarg {name!r} resolves to a {kind} ref, which a shell task cannot receive")
    if isinstance(value, dict):
        return {k: await _resolve_plain_value(name, v, session, job_id) for k, v in value.items()}
    return value


async def stage_in_json(task: Task) -> Path:
    """A fresh per-attempt IO dir holding the task's resolved ``in.json``; the
    caller removes it once the task is done."""
    # Resolve before mkdtemp: a resolve error must not leave the dir behind.
    kwargs = await resolve_plain_kwargs(task.kwargs, task.job_id)
    io_dir = Path(tempfile.mkdtemp(prefix="aaiclick-io-"))
    try:
        write_in_json(io_dir, kwargs)
    except BaseException:
        shutil.rmtree(io_dir, ignore_errors=True)
        raise
    return io_dir


async def resolve_plain_kwargs(kwargs: dict, job_id: int) -> dict:
    """Resolve a shell/jvm task's kwargs into the plain JSON ``in.json`` carries.

    ``upstream`` refs become the producer's stored value (recursively) and
    ``native`` wrappers unwrap; a value of any other kind — an ``object`` a
    Python producer stored, say — raises ``ValueError`` naming the kwarg. The
    commit-point validator rejects such refs written into kwargs directly;
    this catches the ones that only appear once a producer has run.
    """
    if not kwargs:
        return {}
    async with get_sql_session() as session:
        return {k: await _resolve_plain_value(k, v, session, job_id) for k, v in kwargs.items()}


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
    if task.entry_type == ENTRY_JVM:
        raise RuntimeError(
            f"jvm task {task.id} cannot execute in-process — the aaiclick-task-api "
            "shim runs it inside its container image"
        )

    run_id = await register_run(task.id)

    with current_task_info(TaskInfo.from_task(task)):
        async with capture_task_output(task.id, task.job_id, run_id):
            # Inside capture so an import failure's traceback lands in this run's log.
            func = import_callback(task.entrypoint)
            async with task_scope(
                task_id=task.id,
                job_id=task.job_id,
                run_id=run_id,
            ):
                kwargs = await deserialize_task_params(task.kwargs, task.job_id)
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
                lifecycle = get_data_lifecycle()
                if lifecycle is not None and isinstance(pin_target, (Object, View)) and not pin_target.persistent:
                    lifecycle.pin(pin_target.table)

                # Register returned tasks INSIDE task_scope so the registry
                # (ContextVar) that was populated during func() is still active.
                data_result = await register_returned_tasks(result, task.id, task.job_id, task.entry_type)

                # Last, once nothing can fail: a failed attempt's pins hold its inputs for the retry.
                if lifecycle is not None:
                    lifecycle.release_pins()

    return data_result


async def start_shell_process(
    command: list[str] | None, command_env: dict[str, str] | None
) -> asyncio.subprocess.Process:
    """Spawn a shell task's argv with ``command_env`` overlaid on the worker env.

    stdout and stderr are separate pipes so captured lines keep their source
    stream (the docker / kubectl wrappers preserve the split for non-TTY
    runs). :func:`execute_shell_task` pumps both.
    """
    return await asyncio.create_subprocess_exec(
        *(command or []),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=overlay_env(command_env),
    )


class ShellSpec(NamedTuple):
    """A subprocess shell task's launch command: its argv plus the
    ``command_env`` overlay. Container shell tasks never reach the task child
    — their launch is a host-side vehicle."""

    argv: list[str]
    env: dict[str, str] | None


async def _pump_stream(stream: asyncio.StreamReader, sink: ChLogSink, source: LogStream) -> None:
    """Feed one output pipe into the sink until EOF.

    Decodes incrementally so a multibyte character split across reads stays intact.
    """
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    while True:
        chunk = await stream.read(65536)
        if text := decoder.decode(chunk, final=not chunk):
            sink.write(source, text)
        if not chunk:
            return


async def pump_process_output(
    proc: asyncio.subprocess.Process,
    task_id: int,
    job_id: int,
    run_id: int | None,
    *,
    record_exit: bool = False,
    console_prefix: str = "",
) -> None:
    """Stream ``proc``'s stdout and stderr into a :func:`stream_to_task_logs`
    sink until it exits.

    Cancellation kills ``proc``. Shared by :func:`execute_shell_task` (the
    shell process itself) and :func:`follow_vehicle_output` (a log follower).
    ``record_exit`` adds an ``error: exit N`` stderr line on a nonzero exit — only
    meaningful when ``proc`` is the task, not a follower.
    """
    async with stream_to_task_logs(task_id, job_id, run_id, console_prefix=console_prefix) as sink:
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
            if record_exit and proc.returncode:
                sink.write(STDERR_STREAM, f"error: exit {proc.returncode}\n")


async def finish_shell_result(task: Task, out_value: Any) -> Any:
    """Interpret what a shell/jvm task wrote to ``out.json`` — register any
    task/group dicts it returned, unwrap a top-level ``native`` — and return
    the data portion for :func:`serialize_task_result`.

    Runs under the task's info so children built here inherit its image
    (``stamp_inherited_image``), restored afterwards: on the container
    runners this is the worker loop's own context. A fresh task registry
    keeps the commit to the returned nodes: the loop's registry holds the
    claimed task itself, which would otherwise be inserted again.
    """
    if ref_kind(out_value) == NATIVE:
        out_value = out_value[VALUE]
    with current_task_info(TaskInfo.from_task(task)), fresh_task_registry():
        return await register_returned_tasks(out_value, task.id, task.job_id, task.entry_type, task.image_source)


async def execute_shell_task(task: Task, spec: ShellSpec | None = None) -> Any:
    """Run a shell task's argv, streaming its output to CH ``task_logs``.

    One code path for every in-process shell run: the in-process worker and
    ``job_test`` call it directly (``spec=None`` → the task's own argv), and
    the mp worker's task child calls it with the dispatch-built ``ShellSpec``.
    Output is drained to ``task_logs`` every ``LOG_FLUSH_INTERVAL`` seconds
    and finally at exit, so long-running commands are tailed live.

    The task's kwargs are resolved into ``in.json`` under a per-attempt temp
    dir named by ``AAICLICK_IO_DIR``; whatever the process wrote to
    ``out.json`` is the result (see :func:`finish_shell_result`). Raises
    ``RuntimeError`` on nonzero exit, matching the ``"exit <code>"`` error the
    workers report — ``out.json`` is ignored then.
    """
    if spec is None:
        spec = ShellSpec(task.command or [], task.command_env)
    io_dir = await stage_in_json(task)
    try:
        run_id = await register_run(task.id)
        proc = await start_shell_process(spec.argv, {**(spec.env or {}), IO_DIR_ENV: str(io_dir)})
        await pump_process_output(proc, task.id, task.job_id, run_id, record_exit=True)
        if proc.returncode != 0:
            raise RuntimeError(f"exit {proc.returncode}")
        return await finish_shell_result(task, read_out_json(io_dir))
    finally:
        shutil.rmtree(io_dir, ignore_errors=True)


async def get_run_count(task_id: int) -> int:
    """How many attempts the task has registered (``len(run_ids)``)."""
    async with get_sql_session() as session:
        run_ids = (await session.execute(select(Task.run_ids).where(col(Task.id) == task_id))).scalar_one_or_none()
    return len(run_ids or [])


async def register_host_log_run(task: Task, entry_type: EntryType) -> int | None:
    """Register an image task's attempt on the host; return its run_id, or
    None for ``module``.

    A ``module`` image's ``execute_task`` registers its own run and captures
    its own output. Other shims (``jvm``) do neither, so the host registers
    the run and follows the container's output under it
    (:func:`follow_vehicle_output`) — a new language SDK needs no logging code.
    """
    if entry_type == ENTRY_MODULE:
        return None
    return await register_run(task.id)


async def follow_vehicle_output(argv: list[str], task_id: int, job_id: int, run_id: int | None) -> None:
    """Stream a container's output from the host to the task-log destinations,
    console lines prefixed ``[task N]``.

    ``argv`` follows the output until the container exits (``docker logs -f``,
    ``kubectl logs -f``). ``run_id=None`` prints it without writing
    ``task_logs``. Best-effort, like ``flush_task_logs``: a failure is logged
    and never fails the task.
    """
    try:
        proc = await start_shell_process(argv, None)
        await pump_process_output(proc, task_id, job_id, run_id, console_prefix=f"[task {task_id}] ")
    except Exception:
        logger.error("Failed to follow output for task %s run %s", task_id, run_id, exc_info=True)


async def stop_output_follower(follower: asyncio.Task[None] | None, drain_timeout: float) -> None:
    """Let a :func:`follow_vehicle_output` task drain for up to
    ``drain_timeout`` seconds, then cancel it.

    Called before the vehicle is removed (its output is gone after
    ``docker rm`` / ``kubectl delete``); the follower normally ends with the
    container.
    """
    if follower is None:
        return
    if drain_timeout > 0:
        with suppress(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(follower), timeout=drain_timeout)
    follower.cancel()
    with suppress(asyncio.CancelledError):
        await follower


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
        return pydantic_ref(result)

    return native_ref(_sanitize_for_json(result))


def _contains_dag_node(item: Any) -> bool:
    """True if a Task/Group sits anywhere inside a possibly nested container."""
    if isinstance(item, (Task, Group)):
        return True

    if isinstance(item, (list, tuple)):
        return any(_contains_dag_node(sub) for sub in item)

    return False


def _tasks_from(items: Any) -> list[Task | Group]:
    """Validate a tasks position and expand it one level deep.

    A Group also contributes its member tasks. Nothing beyond that one level is
    flattened: nesting carries no meaning in the graph, so flattening it would
    quietly discard the grouping it appears to express — ``Group`` is how tasks
    are grouped.

    Raises:
        TypeError: if the position holds anything but Tasks and Groups.
    """
    tasks: list[Task | Group] = []

    for item in items if isinstance(items, (list, tuple)) else [items]:
        if isinstance(item, Task):
            tasks.append(item)
        elif isinstance(item, Group):
            tasks.extend([item, *item.get_tasks()])
        elif _contains_dag_node(item):
            raise TypeError(
                "A nested list/tuple of Tasks is not flattened. Return one flat "
                "list of Tasks, or a Group when the tasks belong together."
            )
        else:
            raise TypeError(
                f"Only Task/Group items can be returned for registration (got "
                f"{type(item).__name__}). Use task_result(data=..., tasks=[...]) "
                "to return data alongside tasks."
            )

    return tasks


def _is_dag_dict(value: Any) -> bool:
    """True for a ``task`` / ``group`` dict — the JSON form of a DAG node."""
    return ref_kind(value) in (TASK, GROUP)


Handles = dict[str, "Task | Group"]

_BUILD_FIELDS = ImageBuild.model_fields.keys() - {"type"}

_CHILD_REMOTE_SCHEME = re.compile(r"^(https://|git@[^:/]+:)")
# git's HTTP client normalises ``..``, and a server may decode ``%2F`` / ``\`` into
# separators, either of which walks out of a prefix; plain remotes need none of them.
_UNSAFE_REMOTE = re.compile(r"(^|[/:])\.\.(/|$)|[%\\]")


def _under_prefix(remote: str, prefix: str) -> bool:
    """``remote`` starts with ``prefix`` on a path boundary, so ``myorg`` does not admit ``myorg-evil``."""
    if not remote.startswith(prefix):
        return False
    return prefix.endswith(("/", ":")) or remote[len(prefix) : len(prefix) + 1] in ("", "/")


def _check_child_remote(remote: str | None, parent_image_source: dict | None) -> None:
    """Admit a shell/jvm child's build remote: the parent's own build repo, or
    a prefix in ``AAICLICK_CHILD_BUILD_REMOTES``, over ``https://`` or
    ``git@host:`` only. The worker clones it with its own git credentials and
    network, so the container must not pick an arbitrary one.

    Raises:
        ValueError: naming the remote and the setting.
    """
    parent_remote = (parent_image_source or {}).get("git_remote")
    prefixes = [p.strip() for p in os.environ.get("AAICLICK_CHILD_BUILD_REMOTES", "").split(",") if p.strip()]
    if remote and _CHILD_REMOTE_SCHEME.match(remote) and not _UNSAFE_REMOTE.search(remote):
        if remote == parent_remote or any(_under_prefix(remote, p) for p in prefixes):
            return
    raise ValueError(
        f"a shell/jvm task may build only its own repo or a remote listed in "
        f"AAICLICK_CHILD_BUILD_REMOTES (https:// or git@host:); got {remote!r}"
    )


def _build_dag_from_dicts(
    items: list[dict], parent_entry_type: EntryType, parent_image_source: dict | None = None
) -> tuple[list[Task | Group], Handles]:
    """Build ``Task``/``Group`` objects from their JSON form, minting ids.

    Returns the nodes in order plus the handle → node map for the handles the
    dicts declared. Handles are scoped to this one return value. Each task
    dict passes the same entry validation a Python caller gets, and only a
    ``module`` parent may return a ``module`` child: module tasks run with
    aaiclick's credentials, which a shell/jvm container must not grant itself.
    A shell/jvm child may declare an image build only from an admitted remote
    (``_check_child_remote``): the build itself runs on buildkitd, but the
    worker clones the repo.

    Raises:
        ValueError: on a duplicate handle or an invalid entry.
    """
    handles: Handles = {}

    def claim(handle: str | None, node: Task | Group) -> None:
        if handle is None:
            return
        if handle in handles:
            raise ValueError(f"duplicate handle {handle!r} in returned tasks")
        handles[handle] = node

    def build(spec: dict) -> Task | Group:
        fields = {k: v for k, v in spec.items() if k != AAICLICK_TYPE}
        handle = fields.pop(HANDLE, None)
        if spec[AAICLICK_TYPE] == GROUP:
            group = Group(id=get_snowflake_id(), name=fields.get("name", "group"))
            claim(handle, group)
            for member in fields.get(TASKS, []):
                if ref_kind(member) != TASK:
                    raise TypeError("a group dict may hold only task dicts")
                group.add_task(build(member))
            return group
        fields.pop(DEPENDS_ON, None)  # applied after every handle is known
        entry_type = fields.get("entry_type", ENTRY_MODULE)
        validate_task_entry(entry_type=entry_type, command=fields.get("command"))
        if parent_entry_type != ENTRY_MODULE:
            if entry_type == ENTRY_MODULE:
                raise ValueError(f"a {parent_entry_type} task cannot return a module task")
            if _BUILD_FIELDS & fields.keys():
                _check_child_remote(fields.get("git_remote"), parent_image_source)
        task = create_task(fields.pop(ENTRYPOINT, None), **fields)
        claim(handle, task)
        return task

    return [build(spec) for spec in items], handles


_HANDLE_KEYS = {UPSTREAM: TASK_ID, GROUP_RESULTS: GROUP_ID}


def _handle_ref(value: Any) -> tuple[str, str] | None:
    """``(kind, handle)`` when ``value`` is an upstream/group ref.

    Raises:
        ValueError: when the ref names a raw id instead of a sibling handle —
            it would read any task's result in the job, not just a sibling's.
    """
    kind = ref_kind(value)
    if kind is None or kind not in _HANDLE_KEYS:
        return None
    handle = value.get(_HANDLE_KEYS[kind])
    if not isinstance(handle, str):
        raise ValueError(f"a returned {kind} ref must name a sibling handle, not {handle!r}")
    return kind, handle


def _handle_node(handles: Handles, handle: str, kind: str) -> Task | Group:
    """The node a handle names, checked against the ref kind using it."""
    if handle not in handles:
        raise ValueError(f"unknown handle {handle!r} in returned tasks")
    node = handles[handle]
    want_group = kind == GROUP_RESULTS
    if isinstance(node, Group) != want_group:
        names, expects = ("group", "task") if isinstance(node, Group) else ("task", "group")
        raise ValueError(f"handle {handle!r} names a {names}, not a {expects}")
    return node


def _swap_handles(value: Any, handles: Handles, resolved: list[Task | Group]) -> Any:
    """Replace string handles inside ``upstream`` / ``group_results`` refs with
    the ids minted for them, appending each node to ``resolved``; recurses
    through lists and dicts, leaving ``native`` values literal.

    Raises:
        ValueError: on an undeclared handle or one of the wrong kind.
    """
    if isinstance(value, list):
        return [_swap_handles(v, handles, resolved) for v in value]
    if not isinstance(value, dict) or ref_kind(value) == NATIVE:
        return value
    if (ref := _handle_ref(value)) is not None:
        kind, handle = ref
        node = _handle_node(handles, handle, kind)
        resolved.append(node)
        return {**value, _HANDLE_KEYS[kind]: node.id}
    return {k: _swap_handles(v, handles, resolved) for k, v in value.items()}


def _wire_dag_dicts(specs: list[dict], nodes: list[Task | Group], handles: Handles) -> None:
    """Second pass over the dict DAG, now that every handle has an id: swap
    handles in kwargs, and add an edge for each handle a task consumes (as a
    Task kwarg does in Python) plus its ``depends_on``."""

    def walk(spec: dict, node: Task | Group) -> None:
        if isinstance(node, Group):
            for member_spec, member in zip(spec.get(TASKS, []), node.get_tasks(), strict=True):
                walk(member_spec, member)
        else:
            consumed: list[Task | Group] = []
            node.kwargs = _swap_handles(node.kwargs, handles, consumed)
            for upstream in consumed:
                node.depends_on(upstream)
        for handle in spec.get(DEPENDS_ON, []):
            if handle not in handles:
                raise ValueError(f"unknown handle {handle!r} in returned tasks")
            node.depends_on(handles[handle])

    for spec, node in zip(specs, nodes, strict=True):
        walk(spec, node)


async def _hold_dependencies(data_task: Task, parent_task_id: int) -> list[Dependency]:
    """Copy every edge leaving the parent (or its group) onto ``data_task``.

    The parent's result is not ready until ``data_task`` completes. A group
    consumer stays a group edge, so members it gains later are held too. Runs
    before the children are committed, so they are not successors yet.
    """
    async with get_sql_session() as session:
        targets = await successor_edges(session, {parent_task_id})
    return [
        Dependency(
            previous_id=data_task.id,
            previous_type=DEPENDENCY_TASK,
            next_id=target.id,
            next_type=target.type,
        )
        for target in sorted(targets)
    ]


def _dict_result_data(data: Any, handles: Handles) -> Any:
    """The ``data`` of a ``task_result`` dict: a plain value (a ``native``
    wrapper unwraps, like out.json's top level) or a task handle, handed on as
    the Task itself — as Python's ``task_result(data=child)`` — so the
    parent's consumers are held. Any other handle would be stored as a
    dangling ref, so it is an error."""
    if ref_kind(data) == NATIVE:
        data = data[VALUE]
    if (ref := _handle_ref(data)) is not None and ref[0] == UPSTREAM:
        return _handle_node(handles, ref[1], UPSTREAM)
    resolved: list[Task | Group] = []
    _swap_handles(data, handles, resolved)
    if resolved:
        raise ValueError("data may be a plain value or a task handle")
    return data


def _register_dict_dag(
    specs: list, data: Any, parent_entry_type: EntryType, parent_image_source: dict | None
) -> tuple[list[Task | Group], Any]:
    """Build the nodes of a dict-form DAG and resolve its ``data``."""
    if not all(map(_is_dag_dict, specs)):
        raise TypeError("a returned task list may hold only task/group dicts")
    nodes, handles = _build_dag_from_dicts(specs, parent_entry_type, parent_image_source)
    _wire_dag_dicts(specs, nodes, handles)
    return _tasks_from(nodes), _dict_result_data(data, handles)


async def register_returned_tasks(
    result: Any,
    parent_task_id: int,
    job_id: int,
    parent_entry_type: EntryType,
    parent_image_source: dict | None = None,
) -> Any:
    """Register dynamic child tasks returned from a task function.

    Handles these return shapes:
    - None                          → no tasks, return None
    - TaskResult(data, tasks)       → register .tasks, return .data
    - Task / Group                  → register it, return None
    - flat list/tuple of Task/Group → register all, return None
    - Any other value               → pure data, return as-is

    When ``data`` is one of the returned tasks, the parent's existing consumers
    wait for it (see ``_hold_dependencies``): return data that the children
    produce through a task that depends on them.

    The list/tuple must be flat and hold nothing but Tasks and Groups. Mixing
    in data is rejected — that is what ``task_result(data=..., tasks=[...])``
    separates, and guessing which element is data would silently drop the
    other. Nesting is rejected too: it carries no meaning in the graph, so
    flattening it would quietly discard the grouping it appears to express.

    Args:
        result: The raw return value from the task function
        parent_task_id: ID of the task that returned this result
        job_id: ID of the job these tasks belong to
        parent_entry_type: The parent's entry type, which bounds its children's
            (see ``_build_dag_from_dicts``)
        parent_image_source: The parent's own image source, whose build repo a
            shell/jvm child may always build from

    Returns:
        The data portion of the result for serialization, or None.

    Raises:
        TypeError: if a returned list/tuple is nested or holds non-Task items.
    """
    if result is None:
        return None

    if isinstance(result, dict) and ref_kind(result) == TASK_RESULT:
        task_items, data_result = _register_dict_dag(
            result.get(TASKS, []), result.get(DATA), parent_entry_type, parent_image_source
        )
    elif _is_dag_dict(result) or (isinstance(result, list) and any(map(_is_dag_dict, result))):
        task_items, data_result = _register_dict_dag(
            result if isinstance(result, list) else [result], None, parent_entry_type, parent_image_source
        )
    elif isinstance(result, TaskResult):
        task_items = _tasks_from(result.tasks)
        data_result = result.data
    elif isinstance(result, (Task, Group)):
        # Job entry tasks can return a single Task or Group directly
        task_items = _tasks_from(result)
        data_result = None
    elif isinstance(result, (list, tuple)):
        if not _contains_dag_node(result):
            # No DAG nodes anywhere — an ordinary data return like [1, 2, 3].
            return result
        task_items = _tasks_from(result)
        data_result = None
    else:
        return result

    if not task_items:
        return data_result

    hold_rows = await _hold_dependencies(data_result, parent_task_id) if isinstance(data_result, Task) else []

    # Wire dependency: each returned item depends on the parent task
    for item in task_items:
        item.link_previous(parent_task_id, DEPENDENCY_TASK)

    await commit_tasks(task_items, job_id, extra_dependencies=hold_rows)
    await _pin_child_inputs(task_items)

    return data_result


def _referenced_tables(value: Any) -> set[str]:
    """Ephemeral table names referenced by Object/View refs anywhere inside ``value``."""
    if isinstance(value, dict):
        if ref_kind(value) in (OBJECT, VIEW):
            return set() if value.get(PERSISTENT) else {value["table"]}
        return set().union(*(_referenced_tables(v) for v in value.values()))
    if isinstance(value, (list, tuple)):
        return set().union(*(_referenced_tables(v) for v in value))
    return set()


async def _pin_child_inputs(items: list) -> None:
    """Pin each dynamic child on the tables its kwargs reference.

    The parent produced those tables but no dependency row names the
    children when it pins, so the producer fan-out cannot reach them. The
    child releases the pins once it has deserialized its inputs, as any
    consumer does.
    """
    rows = [
        {"table_name": table, "task_id": item.id}
        for item in items
        if isinstance(item, Task)
        for table in sorted(_referenced_tables(item.kwargs))
    ]
    if not rows:
        return
    async with get_sql_session() as session:
        await session.execute(
            text(
                "INSERT INTO table_pin_refs (table_name, task_id) VALUES (:table_name, :task_id) "
                "ON CONFLICT (table_name, task_id) DO NOTHING"
            ),
            rows,
        )
        await session.commit()


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


class DispatchError(RuntimeError):
    """A task this executor cannot run: a container task where there is no
    container runner. The worker loop records it as the task's failure."""


def require_container_runner(task: Task, runner: WorkerRunner | None) -> None:
    """Fail a task that declares an image when ``runner`` is None — the mp
    worker without ``AAICLICK_RUNNER``, or an in-process executor (local mode,
    ``job_test``), which never has one. Raises ``DispatchError``."""
    if task.image_source is not None and runner is None:
        raise DispatchError(
            f"task {task.name!r} declares an image_source but this executor has no container runner; "
            f"container tasks need an execution worker with {ENV_WORKER_RUNNER}=docker|kubernetes"
        )


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
            result = await session.execute(
                text(_READY_TASK_SQL),
                {
                    "job_id": job.id,
                    "pending_status": TASK_PENDING,
                    "completed_status": TASK_COMPLETED,
                    "now": utc_now(),
                },
            )
            row = result.fetchone()

        if row is None:
            break

        task_id = row[0]
        await update_task_status(task_id, TASK_RUNNING)
        async with get_sql_session() as session:
            task = (await session.execute(select(Task).where(Task.id == task_id))).scalar_one()

        try:
            require_container_runner(task, None)
            data_result = await execute_task(task)
            result_ref = serialize_task_result(data_result, task.job_id)
            await update_task_status(task_id, TASK_COMPLETED, result=result_ref)
        except Exception as e:
            job_failed = True
            error_msg = str(e)
            logger.exception("Task %r failed: %s", task.name, e)
            await update_task_status(task_id, TASK_FAILED, error=str(e))
            break

    await update_job_status(job.id, JOB_FAILED if job_failed else JOB_COMPLETED, error=error_msg)

    # Update in-memory job object
    async with get_sql_session() as session:
        db_job = (await session.execute(select(Job).where(Job.id == job.id))).scalar_one()
        job.status = db_job.status
        job.completed_at = db_job.completed_at
        job.error = db_job.error
