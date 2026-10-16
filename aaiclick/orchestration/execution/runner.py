"""Task execution utilities for orchestration backend."""

from __future__ import annotations

import asyncio
import codecs
import importlib
import inspect
import logging
import math
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

from aaiclick.data.data_context import (
    get_data_lifecycle,
    register_object,
)
from aaiclick.data.data_context.lifecycle import read_table_schema
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
    is_upstream_ref,
    native_ref,
    pydantic_ref,
    ref_kind,
    upstream_ref,
)
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
from ..runner_config import ENTRY_JVM, ENTRY_MODULE, ENTRY_SHELL, EntryType
from .claiming import update_job_status, update_task_status
from .cli import overlay_env
from .db_handler import DEPENDENCY_WHERE
from .execution_worker_context import set_current_task_info
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


async def _resolve_upstream_ref(ref: dict, session: AsyncSession) -> Any:
    """Resolve an upstream task reference to its result.

    Args:
        ref: An ``upstream`` ref dict with ``task_id``
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

    Dispatches on :func:`~aaiclick.data.object.refs.ref_kind`: upstream refs
    resolve to the task result, Object/View refs are reconstructed, lists and
    plain dicts recurse, anything else passes through.

    Args:
        value: Serialized value from task kwargs
        session: Database session for resolving upstream refs

    Returns:
        Deserialized value ready for function call
    """
    if isinstance(value, list):
        return [await _deserialize_value(v, session) for v in value]

    kind = ref_kind(value)

    if kind == UPSTREAM:
        upstream_result = await _resolve_upstream_ref(value, session)
        return await _deserialize_value(upstream_result, session)

    if kind == CALLABLE:
        return import_callback(value["entrypoint"])

    # A Group passed as a kwarg: the members' results, in task order
    if kind == GROUP_RESULTS:
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
        ValueError: Unknown ``_aaiclick_type`` or upstream task not completed
    """
    if not serialized_params:
        return {}

    async with get_sql_session() as session:
        return {k: await _deserialize_value(v, session) for k, v in serialized_params.items()}


async def _resolve_plain_value(name: str, value: Any, session: AsyncSession) -> Any:
    if isinstance(value, list):
        return [await _resolve_plain_value(name, v, session) for v in value]
    kind = ref_kind(value)
    if kind == UPSTREAM:
        return await _resolve_plain_value(name, await _resolve_upstream_ref(value, session), session)
    if kind == NATIVE:
        return value[VALUE]
    if kind is not None:
        raise ValueError(f"kwarg {name!r} resolves to a {kind} ref, which a shell task cannot receive")
    if isinstance(value, dict):
        return {k: await _resolve_plain_value(name, v, session) for k, v in value.items()}
    return value


async def resolve_plain_kwargs(kwargs: dict) -> dict:
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
        return {k: await _resolve_plain_value(k, v, session) for k, v in kwargs.items()}


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

    set_current_task_info(task_id=task.id, job_id=task.job_id, image_source=task.image_source, group_id=task.group_id)

    async with capture_task_output(task.id, task.job_id, run_id):
        # Inside capture so an import failure's traceback lands in this run's log.
        func = import_callback(task.entrypoint)
        async with task_scope(
            task_id=task.id,
            job_id=task.job_id,
            run_id=run_id,
        ):
            kwargs = await deserialize_task_params(task.kwargs)
            lifecycle = get_data_lifecycle()
            if lifecycle is not None:
                lifecycle.release_pins()  # FIFO: after every INCREF from deserialization
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
    return await asyncio.create_subprocess_exec(
        *(command or []),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=overlay_env(command_env),
    )


class ShellSpec(NamedTuple):
    """A shell task's fully-resolved launch command.

    Built host-side (``dispatch.build_shell_spec``) so runner-specific
    wrapping (``docker run`` / ``kubectl run``) stays out of the executing
    process. ``cleanup_argv`` is run after the process ends however it ends —
    killing the wrapper CLI alone would leave its container/pod running.
    ``env_file`` is a private (0600) host file the argv reads ``command_env``
    from, keeping the values out of ``ps``; the dispatcher removes it once
    the task child returns."""

    argv: list[str]
    env: dict[str, str] | None
    cleanup_argv: list[str] | None = None
    env_file: str | None = None


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

    Sets the current task info first so children built here inherit the
    task's image (``stamp_inherited_image``).
    """
    set_current_task_info(task_id=task.id, job_id=task.job_id, image_source=task.image_source, group_id=task.group_id)
    if ref_kind(out_value) == NATIVE:
        out_value = out_value[VALUE]
    return await register_returned_tasks(out_value, task.id, task.job_id)


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
    io_dir = Path(tempfile.mkdtemp(prefix="aaiclick-io-"))
    try:
        write_in_json(io_dir, await resolve_plain_kwargs(task.kwargs))
        run_id = await register_run(task.id)
        proc = await start_shell_process(spec.argv, {**(spec.env or {}), IO_DIR_ENV: str(io_dir)})
        try:
            await pump_process_output(proc, task.id, task.job_id, run_id, record_exit=True)
        finally:
            if spec.cleanup_argv:
                await _run_cleanup_argv(spec.cleanup_argv)
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


def _build_dag_from_dicts(items: list[dict]) -> tuple[list[Task | Group], dict[str, int]]:
    """Build ``Task``/``Group`` objects from their JSON form, minting ids.

    Returns the nodes in order plus the handle → id map for the handles the
    dicts declared. Handles are scoped to this one return value.

    Raises:
        ValueError: on a duplicate handle.
    """
    handles: dict[str, int] = {}
    nodes: list[Task | Group] = []

    def claim(handle: str | None, node_id: int) -> None:
        if handle is None:
            return
        if handle in handles:
            raise ValueError(f"duplicate handle {handle!r} in returned tasks")
        handles[handle] = node_id

    def build(spec: dict) -> Task | Group:
        fields = {k: v for k, v in spec.items() if k != AAICLICK_TYPE}
        handle = fields.pop(HANDLE, None)
        if spec[AAICLICK_TYPE] == GROUP:
            group = Group(id=get_snowflake_id(), name=fields.get("name", "group"))
            claim(handle, group.id)
            for member in fields.get(TASKS, []):
                if ref_kind(member) != TASK:
                    raise TypeError("a group dict may hold only task dicts")
                group.add_task(build(member))
            return group
        fields.pop(DEPENDS_ON, None)  # applied after every handle is known
        task = create_task(fields.pop(ENTRYPOINT, None), **fields)
        claim(handle, task.id)
        return task

    nodes = [build(spec) for spec in items]
    return nodes, handles


def _swap_handles(value: Any, handles: dict[str, int]) -> Any:
    """Replace string handles inside ``upstream`` / ``group_results`` refs with
    the ids minted for them; recurses through lists and dicts.

    Raises:
        ValueError: on a handle this return value did not declare.
    """
    if isinstance(value, list):
        return [_swap_handles(v, handles) for v in value]
    if not isinstance(value, dict):
        return value
    kind = ref_kind(value)
    key = TASK_ID if kind == UPSTREAM else GROUP_ID if kind == GROUP_RESULTS else None
    if key is not None and isinstance(value.get(key), str):
        if value[key] not in handles:
            raise ValueError(f"unknown handle {value[key]!r} in returned tasks")
        return {**value, key: handles[value[key]]}
    return {k: _swap_handles(v, handles) for k, v in value.items()}


def _wire_dag_dicts(specs: list[dict], nodes: list[Task | Group], handles: dict[str, int]) -> None:
    """Second pass over the dict DAG: swap handles in kwargs and apply
    ``depends_on``, now that every handle has an id."""
    by_id = {node.id: node for node in nodes}
    for group in [n for n in nodes if isinstance(n, Group)]:
        by_id.update({t.id: t for t in group.get_tasks()})

    def walk(spec: dict, node: Task | Group) -> None:
        if isinstance(node, Group):
            for member_spec, member in zip(spec.get(TASKS, []), node.get_tasks(), strict=True):
                walk(member_spec, member)
            return
        node.kwargs = _swap_handles(node.kwargs, handles)
        for handle in spec.get(DEPENDS_ON, []):
            if handle not in handles:
                raise ValueError(f"unknown handle {handle!r} in returned tasks")
            node.depends_on(by_id[handles[handle]])

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


async def register_returned_tasks(result: Any, parent_task_id: int, job_id: int) -> Any:
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

    Returns:
        The data portion of the result for serialization, or None.

    Raises:
        TypeError: if a returned list/tuple is nested or holds non-Task items.
    """
    if result is None:
        return None

    kind = ref_kind(result) if isinstance(result, dict) else None
    if kind == TASK_RESULT or _is_dag_dict(result) or (isinstance(result, list) and any(map(_is_dag_dict, result))):
        specs = result[TASKS] if kind == TASK_RESULT else result if isinstance(result, list) else [result]
        if not all(map(_is_dag_dict, specs)):
            raise TypeError("a returned task list may hold only task/group dicts")
        nodes, handles = _build_dag_from_dicts(specs)
        _wire_dag_dicts(specs, nodes, handles)
        task_items = _tasks_from(nodes)
        data_result = _swap_handles(result.get(DATA), handles) if kind == TASK_RESULT else None
        if is_upstream_ref(data_result) and result[DATA][TASK_ID] in handles:
            # ``data`` names a child: hand the Task itself on, as Python's
            # ``task_result(data=child)`` does, so its consumers are held.
            data_result = next(t for t in task_items if t.id == data_result[TASK_ID])
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
