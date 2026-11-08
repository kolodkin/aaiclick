Orchestration
---

Everything so far ran inline. To get a pipeline a worker can schedule, wrap the
steps in tasks and compose them into a job; the data operations are unchanged.

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

`ajob_test()` runs every task locally, in order — for developing and debugging
before handing the pipeline to a worker:

```python
--8<-- "aaiclick/orchestration/examples/orchestration_basic.py:run"
```

# See Also

- [Orchestration](../user_guide/orchestration.md) — `@task`/`@job`, workers, and scheduling
- [Examples: Orchestration Basics](../examples/orchestration_basic.md) — the complete runnable script
- [Examples: Orchestration Dynamic](../examples/orchestration_dynamic.md) — dynamic task generation
- [Examples: Orchestration Groups](../examples/orchestration_groups.md) — groups as dependencies and fan-in
- [Examples: Orchestration Operators](../examples/orchestration_operators.md) — `map()` and `reduce()`
