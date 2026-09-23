# TaskResult Hold Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Consumers of a task returning `task_result(data=..., tasks=[...])` wait for the returned tasks; `map()` and `reduce()` return the expander `Task` whose result is the output Object.

**Architecture:** `register_returned_tasks` (runner.py) collects the parent's existing successors before committing the children, then inserts `child >> successor` dependency rows so the scheduler's existing `DEPENDENCY_WHERE` blocks the consumers. `map()` / `reduce()` become plain expander tasks; their runtime children stay grouped. `Group._result_task` is removed.

**Tech Stack:** Python 3.10+, SQLModel/SQLAlchemy async sessions, pytest with `asyncio_mode = "auto"`, chdb + SQLite local backend.

**Spec:** `docs/designs/task_result_hold.md`

## Global Constraints

- All imports at the top of the file, in three groups (stdlib, third party, package). Never inside functions.
- No `Any` shortcuts for typing. No `__all__`. No history comments.
- Tests are flat module-level functions, `async def test_*` with no decorator, next to the module under test.
- Run tests with `uv run --frozen python -m pytest <path> -q`. `filterwarnings = ["error"]` is on.
- Callbacks and jobs used by `ajob_test` must be module-level (workers resolve them by entrypoint).
- Docs in subdirectories follow `markdown-style`: setext title, `#` sections, `##` subsections, aligned tables, no line-number references.

## Review Focus

1. A parent that returns `task_result(data=x, tasks=[...])` with **no** successors: the successor query returns an empty set and no rows are inserted. Covered by the existing reduce tests, which have no consumer.
2. A returned `Group` whose members were all added via `group.add_task` (reduce's layers): the hold edge is `group >> S`, and `DEPENDENCY_WHERE` blocks `S` until every member is `COMPLETED`. Pinned in Task 2's reduce consumer test.
3. Parent in a group `G` with `G >> S`: `S` waits for the children (Task 1's group-successor test).
4. A consumer that reads `task_result(data=child)`: the parent's result is an upstream ref to a child that is `PENDING` at parent-completion time. Task 1's data-as-child test pins that the consumer now resolves it.
5. `tasks_list(...)` keeps the early start (Task 1's no-hold test).

---

### Task 1: Hold edges in `register_returned_tasks`

**Files:**
- Modify: `aaiclick/orchestration/execution/runner.py` (`register_returned_tasks`)
- Test: `aaiclick/orchestration/execution/test_execution.py` (unit, dependency rows)
- Create: `aaiclick/orchestration/test_task_result_hold.py` (end to end via `ajob_test`)

**Interfaces:**
- Produces: `_hold_successors(items: list, parent_task_id: int, successor_ids: set[int]) -> None` — internal helper inserting one `Dependency` per (top-level returned item, successor).
- Produces: `_existing_successor_ids(parent_task_id: int) -> set[int]` — wraps `successor_task_ids` in a session.

- [ ] **Step 1: Write the failing unit tests** in `aaiclick/orchestration/execution/test_execution.py`, after `test_register_returned_tasks_task_result_tasks_only`:

```python
async def _dependency_pairs(next_id: int) -> set[tuple[int, str]]:
    """(previous_id, previous_type) of every edge pointing at ``next_id``."""
    async with get_sql_session() as session:
        rows = await session.execute(
            select(Dependency.previous_id, Dependency.previous_type).where(Dependency.next_id == next_id)
        )
        return set(rows.all())


async def test_register_returned_tasks_holds_direct_successor(orch_ctx):
    """task_result(data=..., tasks=[child]) with parent >> consumer adds child >> consumer."""
    job = await create_job("hold_direct", "mod.func")
    parent = create_task("mod.parent")
    consumer = create_task("mod.consumer")
    parent >> consumer
    await commit_tasks([parent, consumer], job_id=job.id)

    child = create_task("mod.child")
    await register_returned_tasks(task_result(data="x", tasks=[child]), parent_task_id=parent.id, job_id=job.id)

    assert await _dependency_pairs(consumer.id) == {(parent.id, "task"), (child.id, "task")}


async def test_register_returned_tasks_holds_group_successor(orch_ctx):
    """Parent in G with G >> consumer: a returned Group holds the consumer as group >> consumer."""
    from aaiclick.snowflake import get_snowflake_id

    job = await create_job("hold_group", "mod.func")
    parent_group = Group(id=get_snowflake_id(), name="pg")
    parent = create_task("mod.parent")
    parent_group.add_task(parent)
    consumer = create_task("mod.consumer")
    parent_group >> consumer
    await commit_tasks([parent_group, parent, consumer], job_id=job.id)

    layer = Group(id=get_snowflake_id(), name="layer")
    member = create_task("mod.member")
    layer.add_task(member)
    await register_returned_tasks(task_result(data="x", tasks=[layer]), parent_task_id=parent.id, job_id=job.id)

    assert await _dependency_pairs(consumer.id) == {(parent_group.id, "group"), (layer.id, "group")}


async def test_register_returned_tasks_no_hold_without_data(orch_ctx):
    """tasks_list(...) carries no data, so consumers are not held."""
    job = await create_job("hold_none", "mod.func")
    parent = create_task("mod.parent")
    consumer = create_task("mod.consumer")
    parent >> consumer
    await commit_tasks([parent, consumer], job_id=job.id)

    child = create_task("mod.child")
    await register_returned_tasks(tasks_list(child), parent_task_id=parent.id, job_id=job.id)

    assert await _dependency_pairs(consumer.id) == {(parent.id, "task")}
```

Move the `get_snowflake_id` import to the top of the file if it is not already there (the file already imports it inside two tests; hoist it and delete the inline imports).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/execution/test_execution.py -q -k "holds or no_hold"`
Expected: the two `holds_*` tests FAIL (only the parent edge exists); `no_hold` PASSES.

- [ ] **Step 3: Implement the hold** in `aaiclick/orchestration/execution/runner.py`.

Add to the imports: `from ..dependency_graph import successor_task_ids` (the runner already imports from `..orch_context`, which imports `dependency_graph`, so no cycle).

Add above `register_returned_tasks`:

```python
def _hold_sources(items: Any) -> list:
    """Top-level Task/Group items of a tasks position, unexpanded.

    A returned Group holds its consumers as one ``group >> consumer`` edge;
    its members are covered by the group edge and need no rows of their own.
    """
    return [item for item in (items if isinstance(items, (list, tuple)) else [items]) if isinstance(item, (Task, Group))]


async def _existing_successor_ids(parent_task_id: int) -> set[int]:
    """Task ids one hop downstream of the parent, through direct and group edges."""
    async with get_sql_session() as session:
        return await successor_task_ids(session, {parent_task_id})


async def _hold_successors(items: list, successor_ids: set[int]) -> None:
    """Insert ``item >> successor`` for every returned item and existing successor.

    Consumers of a task that returned data alongside children must not start
    until the children have completed: the data usually is an Object the
    children fill. Returned items are fresh, so no existing row can collide.
    """
    if not items or not successor_ids:
        return
    async with get_sql_session() as session:
        for item in items:
            previous_type = DEPENDENCY_TASK if isinstance(item, Task) else DEPENDENCY_GROUP
            for successor_id in sorted(successor_ids):
                session.add(
                    Dependency(
                        previous_id=item.id,
                        previous_type=previous_type,
                        next_id=successor_id,
                        next_type=DEPENDENCY_TASK,
                    )
                )
        await session.commit()
```

Import `DEPENDENCY_GROUP` and `DEPENDENCY_TASK` from `..models` alongside the existing `Dependency` import.

In `register_returned_tasks`, change the `TaskResult` branch and the commit:

```python
    hold_items: list = []
    if result is None:
        return None

    elif isinstance(result, TaskResult):
        task_items = _tasks_from(result.tasks)
        data_result = result.data
        if data_result is not None:
            hold_items = _hold_sources(result.tasks)
    ...
    if not task_items:
        return data_result

    # Successors that exist now, before the children are committed: the
    # children themselves become successors of the parent below and must
    # not hold one another.
    successor_ids = await _existing_successor_ids(parent_task_id) if hold_items else set()

    # Wire dependency: each returned item depends on the parent task
    for item in task_items:
        ...

    await commit_tasks(task_items, job_id)
    await _hold_successors(hold_items, successor_ids)
    await _pin_child_inputs(task_items)

    return data_result
```

Update the `register_returned_tasks` docstring: add a line under the return shapes, "TaskResult with data also holds the parent's existing consumers until every returned task completes."

- [ ] **Step 4: Run the unit tests**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/execution/test_execution.py -q`
Expected: all PASS.

- [ ] **Step 5: Write the end-to-end tests** in a new file `aaiclick/orchestration/test_task_result_hold.py`:

```python
"""Execution tests: consumers of task_result(data=..., tasks=[...]) wait for the tasks."""

from aaiclick.orchestration import get_job_result, task_result, tasks_list
from aaiclick.orchestration.decorators import job, task
from aaiclick.orchestration.execution.debug import ajob_test
from aaiclick.orchestration.models import JOB_COMPLETED, Group
from aaiclick.snowflake import get_snowflake_id


@task
async def child_value() -> int:
    return 7


@task
async def parent_with_child_data():
    """Returns the child itself as data: a consumer must wait for it to resolve."""
    child = child_value()
    return task_result(data=child, tasks=[child])


@task
async def parent_without_data():
    return tasks_list(child_value())


@task
async def consume(value) -> int:
    return value


@job("test_hold_child_data")
def hold_child_data():
    parent = parent_with_child_data()
    seen = consume(value=parent)
    return task_result(data=seen, tasks=[parent, seen])


@job("test_hold_group_successor")
def hold_group_successor():
    group = Group(id=get_snowflake_id(), name="producers")
    parent = parent_with_child_data()
    group.add_task(parent)
    seen = consume(value=parent)
    group >> seen
    return task_result(data=seen, tasks=[group, seen])


@job("test_no_hold_without_data")
def no_hold_without_data():
    parent = parent_without_data()
    seen = consume(value=parent)
    return task_result(data=seen, tasks=[parent, seen])


async def test_consumer_waits_for_child_named_as_data(orch_ctx):
    """The parent's result is an upstream ref to a PENDING child; the consumer must not run yet."""
    j = await hold_child_data()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == 7


async def test_consumer_of_parent_group_waits_for_children(orch_ctx):
    """G >> consumer with the parent a member of G: the returned child holds the consumer too."""
    j = await hold_group_successor()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) == 7


async def test_tasks_list_does_not_hold(orch_ctx):
    """tasks_list carries no data, so the consumer starts after the parent alone and sees None."""
    j = await no_hold_without_data()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    assert await get_job_result(j) is None
```

Note: `consume(value=parent)` serializes `parent` as an upstream ref to the parent's result. For `parent_with_child_data` that result is itself an upstream ref to `child`, and `_deserialize_value` follows refs recursively. Before this change the job FAILS with `Upstream task … is not completed (status: PENDING)`. Check `get_job_result` is imported from `aaiclick.orchestration` the same way `test_orchestration_reduce.py` does; the data_context wrapper is needed only for Object results, and here the results are native values.

- [ ] **Step 6: Run the end-to-end tests**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/test_task_result_hold.py -q`
Expected: all PASS. Temporarily `git stash` the runner change and re-run to confirm the first two FAIL without it; `git stash pop`.

- [ ] **Step 7: Commit**

```bash
git add aaiclick/orchestration/execution/runner.py aaiclick/orchestration/execution/test_execution.py aaiclick/orchestration/test_task_result_hold.py
git commit -m "Hold consumers of task_result(data=...) until returned tasks complete"
```

---

### Task 2: `reduce()` returns the expander Task

**Files:**
- Modify: `aaiclick/orchestration/operators.py` (`reduce`)
- Modify: `aaiclick/orchestration/models.py` (remove `Group._result_task`)
- Modify: `aaiclick/orchestration/test_orchestration_reduce.py`
- Modify: `aaiclick/orchestration/examples/orchestration_operators.py`

**Interfaces:**
- Produces: `reduce(cbk, obj, *, partition=5000, args=(), kwargs=None) -> Task`.

- [ ] **Step 1: Update the reduce tests.** In `aaiclick/orchestration/test_orchestration_reduce.py` replace every `data=reduced._result_task` with `data=reduced`, and add the regression job and test:

```python
@task
async def read_total(total: Object) -> list:
    return await total.data()


@job("test_reduce_consumer")
def reduce_consumer(values: list, partition_size: int):
    data = create_test_object(values=values)
    reduced = reduce(sum_reduce, data, partition=partition_size)
    seen = read_total(total=reduced)
    return task_result(data=seen, tasks=[data, reduced, seen])


async def test_reduce_consumer_sees_filled_result(orch_ctx):
    """A consumer of reduce() runs after every layer, not after the expander alone."""
    j = await reduce_consumer(values=[1, 2, 3, 4, 5], partition_size=2)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [15]
```

`read_total` returns a Python list; `serialize_task_result` auto-converts it to an Object, so `get_job_result` yields an Object whose `.data()` is `[15]`. If the assertion shape differs, assert `(await (await get_job_result(j)).data()) == [15]` instead — check `serialize_task_result` for the exact behavior before choosing.

- [ ] **Step 2: Run the reduce tests to verify they fail**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/test_orchestration_reduce.py -q`
Expected: existing tests FAIL (`data=reduced` is a Group, serialized as a group-results ref); the new test FAILS.

- [ ] **Step 3: Implement.** In `aaiclick/orchestration/operators.py`:

```python
def reduce(
    cbk: Callable | TaskFactory,
    obj: Task | Object,
    *,
    partition: int = 5000,
    args: tuple = (),
    kwargs: dict[str, Any] | None = None,
) -> Task:
    """Create a layered parallel reduction over an Object.

    Returns the expander Task. At runtime it queries the row count,
    pre-allocates every layer Object, and registers all layer groups and
    partition tasks at once. Its result is the final single-row Object, and
    tasks that consume it wait for every layer to finish.

    Args:
        cbk: Callback applied to each partition. Must be homomorphic:
             output schema must match input schema. Returns 1 row.
             Signature: async def f(partition: Object, output: Object, *args, **kwargs) -> None
        obj: Task or Object to reduce. If Task, the expander waits for it.
        partition: Max rows per partition task (default 5000).
        args: Extra positional arguments forwarded to cbk.
        kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        The expander Task; its result is the final single-row Object.
    """
    if kwargs is None:
        kwargs = {}

    return _expand_reduce(
        cbk=cbk,
        obj=obj,
        partition=partition,
        cbk_args=list(args),
        cbk_kwargs=kwargs,
    )
```

Remove the `Group` import from operators.py only if Task 3 no longer needs it (it still does: `_expand_map` creates the parts group). In `aaiclick/orchestration/models.py` delete `_result_task: Any = None` and `self._result_task = None` from `Group`.

- [ ] **Step 4: Update the example.** In `aaiclick/orchestration/examples/orchestration_operators.py`:

```python
@task
async def show_total(total: Object) -> None:
    """Consumer of reduce(): runs after every layer, so the Object is filled."""
    print(f"Reduced total: {(await total.data())[0]}")  # → 15


@job("reduce_example")
def reduce_job():
    """reduce() with partition=2 builds layers of 3, 2 and 1 tasks for 5 rows."""
    values = create_values()
    total = reduce(sum_partition, values, partition=2)
    shown = show_total(total=total)
    return task_result(data=total, tasks=[values, total, shown])
```

Keep the `get_job_result` read in `amain()`; it still resolves `data=total`. Update the module docstring: "Both return the expander Task; its result is the output Object, and consumers wait for the partition tasks."

- [ ] **Step 5: Run the reduce tests and the example**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/test_orchestration_reduce.py aaiclick/orchestration/test_orchestration_dynamic.py -q`
Expected: PASS.
Run: `uv run --frozen python aaiclick/orchestration/examples/orchestration_operators.py`
Expected: prints `Reduced total: 15` twice (consumer and `amain`) and completes.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/orchestration/operators.py aaiclick/orchestration/models.py aaiclick/orchestration/test_orchestration_reduce.py aaiclick/orchestration/examples/orchestration_operators.py
git commit -m "reduce() returns the expander Task; drop Group._result_task"
```

---

### Task 3: `map()` returns the expander Task with `out` as its result

**Files:**
- Modify: `aaiclick/orchestration/operators.py` (`map`, `_expand_map`, `_map_part`)
- Modify: `aaiclick/orchestration/test_orchestration_dynamic.py`
- Modify: `aaiclick/orchestration/examples/orchestration_operators.py`

**Interfaces:**
- Produces: `map(cbk, obj, partition=5000, args=(), kwargs=None) -> Task`; callback signature `cbk(row, *args, **kwargs) -> value | None`.

- [ ] **Step 1: Update the map tests.** In `aaiclick/orchestration/test_orchestration_dynamic.py` replace `tasks_list(data, group)` with `tasks_list(data, mapped)` after renaming `group` to `mapped` in the three pipelines, and add:

```python
@task
async def scale(row: int, factor: int) -> int:
    return row * factor


@task
async def keep_large(row: int) -> int | None:
    return row if row >= 30 else None


@task
async def read_values(values: Object) -> list:
    return sorted(await values.data())


@job("test_map_output")
def map_output_pipeline(factor: int):
    data = create_test_data()
    mapped = map(cbk=scale, obj=data, partition=2, kwargs={"factor": factor})
    seen = read_values(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


@job("test_map_filter")
def map_filter_pipeline():
    data = create_test_data()
    mapped = map(cbk=keep_large, obj=data, partition=2)
    seen = read_values(values=mapped)
    return task_result(data=seen, tasks=[data, mapped, seen])


async def test_map_output_is_callback_returns(orch_ctx):
    """A consumer of map() reads the callback's return values, after every partition."""
    j = await map_output_pipeline(factor=2)
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [20, 40, 60, 80, 100]


async def test_map_none_return_drops_row(orch_ctx):
    """A None return contributes no row, so map() doubles as a filter."""
    j = await map_filter_pipeline()
    await ajob_test(j)

    assert j.status == JOB_COMPLETED, f"Job failed: {j.error}"
    async with data_context():
        assert await get_job_result(j) == [30, 40, 50]
```

Add `from aaiclick.data.data_context import data_context` and `from aaiclick.orchestration import get_job_result, task_result` to the imports. `create_test_data` builds `[10, 20, 30, 40, 50]` without `aai_id`; with `partition=2` the slices need a stable order, so change it to `create_object_from_value([10, 20, 30, 40, 50], aai_id=True)` (the existing file-writing tests sort their output and are unaffected). Apply the same `get_job_result` shape note as in Task 2.

- [ ] **Step 2: Run to verify failure**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/test_orchestration_dynamic.py -q`
Expected: the two new tests FAIL; the three existing ones still PASS (a Group in `tasks_list` is fine).

- [ ] **Step 3: Implement.** In `aaiclick/orchestration/operators.py`:

```python
def map(
    cbk: Callable | TaskFactory,
    obj: Task | Object,
    partition: int = 5000,
    args: tuple = (),
    kwargs: dict[str, Any] | None = None,
) -> Task:
    """Create a parallel map over partitions of an Object.

    Returns the expander Task. At runtime it queries the row count and
    creates one ``_map_part`` child per partition. Its result is the output
    Object holding every value the callback returned, and tasks that consume
    it wait for every partition to finish.

    Args:
        cbk: Callback applied to each row: ``cbk(row, *args, **kwargs)``.
            Its return value is appended to the output; ``None`` adds no row.
            The output schema equals the input schema.
        obj: Task or Object to partition. If Task, the expander waits for it.
        partition: Number of rows per partition (default 5000).
        args: Extra positional arguments forwarded to cbk after row.
        kwargs: Extra keyword arguments forwarded to cbk.

    Returns:
        The expander Task; its result is the output Object.
    """
    if kwargs is None:
        kwargs = {}

    return _expand_map(
        cbk=cbk,
        obj=obj,
        partition=partition,
        cbk_args=list(args),
        cbk_kwargs=kwargs,
    )


@task
async def _expand_map(cbk: Callable, obj: Object, partition: int, cbk_args: list, cbk_kwargs: dict) -> TaskResult:
    """Expander task: queries Object row count and creates partition tasks.

    Returns the pre-allocated output Object as data and a ``map`` Group of
    ``_map_part`` children as tasks. Registration pins the output for the
    children and, via the hold on returned tasks, for the consumers.
    """
    table_name = obj.table
    row_count = await obj.count().data()
    n_partitions = max(1, ceil(row_count / partition))

    out = await create_object(obj.schema)

    # Partitioning uses LIMIT/OFFSET, which needs a stable ordering for the
    # slices to be disjoint. Honour the Object's declared order_by; fall back
    # to tuple() (no-op). Callers who care wrap in .view(order_by=...) first.
    partition_order = obj.order_by or "tuple()"

    group = Group(id=get_snowflake_id(), name="map")
    for i in range(n_partitions):
        group.add_task(
            _map_part(
                cbk=cbk,
                part=ViewRef(
                    table=table_name,
                    limit=partition,
                    offset=i * partition,
                    order_by=partition_order,
                ).to_dict(),
                out=out,
                cbk_args=cbk_args,
                cbk_kwargs=cbk_kwargs,
            )
        )

    return task_result(data=out, tasks=[group])


@task
async def _map_part(
    cbk: Callable, part: View, out: Object, cbk_args: list | None = None, cbk_kwargs: dict | None = None
) -> None:
    """Apply a callback to each row in a partition View, appending returns to ``out``.

    Args:
        cbk: Callback function. Signature: cbk(row, *args, **kwargs) -> value | None.
        part: View (partition) of the source Object.
        out: Output Object; every non-None return is inserted.
        cbk_args: Extra positional arguments forwarded to cbk.
        cbk_kwargs: Extra keyword arguments forwarded to cbk.
    """
    if cbk_args is None:
        cbk_args = []
    if cbk_kwargs is None:
        cbk_kwargs = {}
    is_async = inspect.iscoroutinefunction(cbk)
    rows = await part.data()
    results = []
    for row in rows:
        value = await cbk(row, *cbk_args, **cbk_kwargs) if is_async else cbk(row, *cbk_args, **cbk_kwargs)
        if value is not None:
            results.append(value)
    if results:
        await out.insert(results)
```

Remove the now-unused `tasks_list` import from operators.py if nothing else uses it. Update the module docstring's usage block so `map(...)` is consumed as a Task result.

- [ ] **Step 4: Run the map tests**

Run: `uv run --frozen python -m pytest aaiclick/orchestration/test_orchestration_dynamic.py aaiclick/orchestration/test_orchestration_reduce.py aaiclick/orchestration/test_task_result_hold.py -q`
Expected: PASS.

- [ ] **Step 5: Update the example.** In `aaiclick/orchestration/examples/orchestration_operators.py` replace `print_row` and `map_job` with:

```python
@task
async def double(row: int) -> int:
    """map() callback: called once per row; the return value lands in the output."""
    return row * 2


@task
async def show_doubled(doubled: Object) -> None:
    """Consumer of map(): runs after every partition task."""
    print(f"Doubled: {sorted(await doubled.data())}")  # → [2, 4, 6, 8, 10]


@job("map_example")
def map_job():
    """map() creates one _map_part child per partition of the Object."""
    values = create_values()
    doubled = map(double, values, partition=2)
    shown = show_doubled(doubled=doubled)
    return task_result(data=doubled, tasks=[values, doubled, shown])
```

Run: `uv run --frozen python aaiclick/orchestration/examples/orchestration_operators.py`
Expected: prints `Doubled: [2, 4, 6, 8, 10]` and `Reduced total: 15`.

- [ ] **Step 6: Run the orchestration suite**

Run: `uv run --frozen python -m pytest aaiclick/orchestration -q -x`
Expected: PASS. Then `uv run --frozen ruff check aaiclick && uv run --frozen ruff format --check aaiclick`.

- [ ] **Step 7: Commit**

```bash
git add aaiclick/orchestration/operators.py aaiclick/orchestration/test_orchestration_dynamic.py aaiclick/orchestration/examples/orchestration_operators.py
git commit -m "map() returns the expander Task whose result is the output Object"
```

---

### Task 4: Docs, future.md, and cleanup

**Files:**
- Modify: `docs/user_guide/orchestration.md` (Dynamic tasks, Parallel Operators)
- Modify: `docs/designs/orchestration.md` (Orchestration Operators table, reduce section)
- Modify: `docs/designs/future.md` (remove "`map()` Has No Output Path")
- Delete: `docs/designs/task_result_hold.md`, `docs/superpowers/plans/2026-09-23-task-result-hold.md`

- [ ] **Step 1: User guide.** In `docs/user_guide/orchestration.md`, under "Dynamic tasks", after the sentence ending "when the task also returns data", add:

```markdown
Consumers of a task that returns `task_result(data=..., tasks=[...])` start only
once every returned task has completed as well: the data is usually an Object
the children fill. `tasks_list(...)` carries no data and does not hold consumers.
```

Replace the "Parallel Operators" bullets with:

```markdown
- `map(cbk, obj, partition=5000)` — partitions the Object and creates one
  child task per partition; `cbk(row, *args, **kwargs)` is applied to each row
  and its return value is appended to the output Object. A `None` return adds
  no row. Output schema equals input schema.
- `reduce(cbk, obj, partition=5000)` — layered parallel reduction; each layer
  reduces partitions down until a single row remains. `cbk(partition, output)`
  receives an input partition and a pre-allocated output Object and writes via
  `output.insert()`. The callback must be homomorphic: output schema equals
  input schema.

Both accept a `Task` or an `Object` as input and return the expander `Task`.
Its result is the output Object; a task that consumes it waits for every
partition task. See
[Examples: Orchestration Operators](../examples/orchestration_operators.md).
```

- [ ] **Step 2: Design doc.** In `docs/designs/orchestration.md` update the operators table rows:

```markdown
| `map(cbk, obj, partition, args, kwargs) -> Task`          | Expander Task. Partitions Object into Views, creates N `_map_part` children; result is the output Object. |
| `_map_part(cbk, part, out) -> None`                       | Applies `cbk(row, *args, **kwargs)` to each row; non-None returns are inserted into `out`. |
| `reduce(cbk, obj, partition, args, kwargs) -> Task`       | Expander Task. Layered parallel reduction; result is the final single-row Object. |
```

Re-pad the table so the columns align. Under "## reduce()" append: "Consumers of the expander wait for every layer: `register_returned_tasks` adds `layer >> consumer` edges for each existing consumer (see `_hold_successors` in `runner.py`)."

- [ ] **Step 3: future.md.** Delete the "# `map()` Has No Output Path" section (heading through its trailing `---`).

- [ ] **Step 4: Delete the spec and plan**, then run the `shortify` skill on the edited docs and `uv run --frozen mkdocs build --strict` if mkdocs is installed (skip if not).

```bash
git rm docs/designs/task_result_hold.md docs/superpowers/plans/2026-09-23-task-result-hold.md
```

- [ ] **Step 5: Commit and push**

```bash
git add docs
git commit -m "Document TaskResult hold and Task-returning map()/reduce(); close future.md item"
git push -u origin claude/future-md-top-4-emm3dn
```

Then run the `check-pr` skill.
