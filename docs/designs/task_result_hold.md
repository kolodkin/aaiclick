TaskResult Consumers Wait for Returned Tasks
---

Consumers of a task that returns `task_result(data=..., tasks=[...])` must not
start until every returned task has completed. `map()` and `reduce()` return the
expander `Task` whose result is the output Object.

# Problem

`register_returned_tasks` (`aaiclick/orchestration/execution/runner.py`) stores
`data` as the parent's result and commits `tasks` as children with
`parent >> child` edges. Nothing wires those children to the parent's downstream
consumers, so a consumer starts as soon as the parent completes, while the
children may still be running.

`reduce()` hits this. `_expand_reduce` returns
`task_result(data=layer_objs[-1], tasks=all_groups)`: the final Object is
pre-allocated empty and the layer tasks fill it later. A consumer reads the
empty Object and the job still reports `COMPLETED`. The existing reduce tests
pass only because they read the result via `get_job_result` after the whole job
has finished.

`map()` has the related gap: `_expand_map` allocates `out` and nothing returns
it, so the sweep drops it once the children finish.

# Runtime rule

Consumers of a task that returns a `TaskResult` with non-`None` `data` start only
when the task **and every task in `tasks`** have completed.

In `register_returned_tasks`, before the parent is marked `COMPLETED`:

1. Collect the parent's existing successors with `successor_task_ids`
   (`dependency_graph.py`). This follows both direct edges (`parent >> S`) and
   group edges (`G >> S` where the parent is a member of `G`), and expands group
   targets to member tasks. The just-returned children are excluded.
2. Commit the children as today.
3. Insert one `Dependency` row `item >> S` for each top-level returned item
   (a `Task`, or a `Group` standing for its members) and each successor `S`.

Returned children are fresh, so no existing row can have one as `previous_id`;
inserts cannot collide.

There is no opt-out flag. No in-repo caller needs the early start, and an
opt-out would reintroduce the silent empty-Object bug whenever `data` depends on
the children. Fire-and-forget children belong as siblings in the job body, not
as returned `tasks`.

`tasks_list(...)`, `task_result(tasks=[...])` without data, and a returned bare
list, `Task` or `Group` carry no data, so there is nothing for a consumer to read
early. They keep today's behavior: no hold.

# Operators return the expander Task

```python
def map(cbk, obj, partition=5000, args=(), kwargs=None) -> Task     # result = out Object
def reduce(cbk, obj, *, partition=5000, args=(), kwargs=None) -> Task   # result = final Object
```

Both expanders are plain tasks; their runtime children are grouped
(`map` group for the parts, `layer_N` groups for reduce). `Group._result_task`
is removed.

`_expand_map` returns `task_result(data=out, tasks=[group])`. Each `_map_part`
inserts the callback's return values into `out`; a `None` return contributes no
row, so side-effect callbacks keep working and `out` doubles as a filter.
Output schema equals input schema, as with `reduce()`.

```python
@job("reduce_example")
def reduce_job():
    values = create_values()
    total = reduce(sum_partition, values, partition=2)   # a Task
    shown = report(total=total)                          # waits for all layers, gets the Object
    return task_result(data=total, tasks=[values, total, shown])
```

Breaking: code that passed the `map()` / `reduce()` group as a kwarg received a
list of member results; it now receives the single output Object.

# Rejected alternatives

- **`hold_downstream` flag on `task_result()`**: two behaviors for no current
  caller; choosing the early start while `data` depends on the children
  silently returns wrong results.
- **Static `finish` task per operator**: fixes only `map` / `reduce`, needs the
  expander to know the finish task's id, and leaves hand-written `task_result`
  broken.
- **Hold only on tasks named in `data`**: misses `reduce()`, whose `data` is an
  Object the children fill; the runner cannot tell which tasks write to it.

# Tests

- `reduce()` with a downstream consumer reads `[15]`, not `[]`.
- `map()` with a returning callback: a consumer reads the mapped values.
- `task_result(data=child, tasks=[child])` with a consumer: today fails with
  `Upstream task … is not completed`; with the hold the consumer gets `child`'s
  result.
- Group successor: the parent is a member of `G`, `G >> S`; `S` waits for the
  returned children.
- `tasks_list(...)` with a consumer: no hold edges are added.
