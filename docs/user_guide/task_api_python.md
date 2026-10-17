Task API: Python
---

The `aaiclick.orchestration` package, layered on the
[Task Interface](task_interface.md).

# Types

| `_aaiclick_type` | Python                                   |
|------------------|------------------------------------------|
| `task_result`    | `TaskResult(data=…, tasks=[…])`          |
| `task`           | a `@task` call, `create_task(…)`         |
| `group`          | `Group(name=…)`                          |
| `upstream`       | a `Task` passed as a kwarg               |
| `group_results`  | a `Group` passed as a kwarg              |
| `native`         | any JSON-serializable value              |
| `callable`       | a `@task` function passed as a value     |
| `object`         | `Object`                                 |
| `view`           | `View`                                   |
| `pydantic`       | a `BaseModel` instance                   |

# Example

Two workers, a sum that consumes them, and the sum as the parent's result:

```python
@task
async def fan_out() -> TaskResult:
    w1 = worker(n=1)
    w2 = worker(n=2)
    fin = add(a=w1, b=w2)
    return TaskResult(data=fin, tasks=[w1, w2, fin])
```

# Inputs and results

Kwargs arrive deserialized: upstream results as values, `Object` / `View` /
`BaseModel` as themselves. The return value is serialized: `None` →
`null`, `Object` / `View` → a table ref, `BaseModel` → `pydantic`, anything
else → `native`.

More: [Orchestration](orchestration.md).

**Implementation**: `aaiclick/orchestration/`
