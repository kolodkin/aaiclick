Orchestration
---

Everything so far ran inline. To turn a sequence of steps into a pipeline that a
worker can schedule, wrap the steps in tasks and compose them into a job. The
data operations are unchanged — orchestration just adds structure and dependency
tracking on top.

# Define tasks

`@task` marks an async function as a unit of work. A task can return a plain
value or an `Object`:

```python
--8<-- "aaiclick/orchestration/examples/orchestration_basic.py:tasks"
```

# Compose a job

`@job` defines the workflow. Calling tasks inside it records them; passing one
task's result as another's argument creates a dependency:

```python
--8<-- "aaiclick/orchestration/examples/orchestration_basic.py:job"
```

# Run it

`ajob_test()` runs every task locally, in order, which is ideal for developing
and debugging a pipeline before handing it to a worker:

```python
--8<-- "aaiclick/orchestration/examples/orchestration_basic.py:run"
```

# Where to go next

That completes the tour — you can create Objects, operate on them, aggregate,
group, filter, combine, and orchestrate. From here, the User Guide covers each
area in depth.

# See Also

- [Orchestration](../orchestration.md) — `@task`/`@job`, workers, and scheduling
- [Examples: Orchestration Basics](../examples/orchestration_basic.md) — the complete runnable script
- [Examples: Orchestration Dynamic](../examples/orchestration_dynamic.md) — dynamic task generation
