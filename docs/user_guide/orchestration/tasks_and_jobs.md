Tasks and Jobs
---

# Defining Tasks and Jobs

## @task

Wraps an async function into a task.

<!-- req: task-decorator -->
| Parameter     | Default       | Description                                            |
|---------------|---------------|--------------------------------------------------------|
| `name`        | function name | Task display name                                      |
| `max_retries` | `0`           | Failed runs are retried this many times before failing |

<!-- req: task-return-values -->
A task may return a plain value or an `Object`/`View`; data results are
serialized and handed to downstream tasks.

## @job

<!-- req: job-decorator -->
Wraps a workflow function into a job. Use `@job("name")`, `@job(name="name")`,
or bare `@job` (the function name becomes the job name). Calling the decorated
function creates the job and its tasks.

## Dynamic tasks

<!-- req: dynamic-tasks -->
A task may itself return child tasks to register them at runtime — the graph
grows while the job runs. Return `tasks_list(a, b)` (or a plain list, `[a, b]`)
for tasks alone, and `task_result(data=..., tasks=[...])` when the task also
returns data. See
[Examples: Orchestration Dynamic](../../examples/orchestration_dynamic.md).

<!-- req: dynamic-task-data -->
When `data` is one of the returned tasks, consumers of the parent wait for that
task. So when the data is something the children produce, return it through a
task that depends on them: `map()`, `foreach()` and `reduce()` return a
finalize task that runs after the partition tasks and hands their result on.
Any other `data` is readable as soon as the parent completes.

!!! warning "A list carries tasks only, unnested"
    <!-- req: dynamic-list-only -->
    `return [obj, group]` raises `TypeError` — use
    `task_result(data=obj, tasks=[group])` to return data alongside tasks. So
    does `return [[a, b], [c, d]]`: nesting means nothing to the graph, so use a
    `Group` when tasks belong together.

## Explicit dependencies

<!-- req: explicit-dependencies -->
When no data flows between two tasks, order them with `>>` (or `<<`): `a >> b`
runs `b` after `a`, `[a, b] >> c` fans in, and `a >> [b, c]` fans out. Chains
read left to right: `first >> group >> last`. `b.depends_on(a)` records the
same edge as a method call.

Pyright flags a bare `a >> b` as an unused expression: use `depends_on()`, or
put `# pyright: reportUnusedExpression=false` at the top of files that define jobs.

## Groups

<!-- req: groups -->
A `Group` bundles tasks that belong together. `group.add_task(t)` makes `t` a
member, and a returned Group registers its members with it. Dependencies work
on the whole group: `a >> group` runs every member after `a`, and
`group >> b` runs `b` once every member has completed.

<!-- req: group-kwarg -->
Passing a Group as a kwarg does both at once — it wires `group >> consumer`
and hands the consumer the members' results as a list, in task order:

```python
group = Group(name="producers")
for _ in range(3):
    group.add_task(step_a())
total = sum_results(results=group)  # receives [42, 42, 42]
return [group, total]
```

See [Examples: Orchestration Groups](../../examples/orchestration_groups.md).

## Testing jobs

<!-- req: testing-jobs -->
`job_test(j)` (sync) and `await ajob_test(j)` (async) execute every task of a
job in dependency order in the current process, for debugging before handing it
to workers. Both also accept the `@job` function plus its kwargs and create the
job first (`job_test(pipeline, x=3, y=4)`), and return the executed `Job`.
