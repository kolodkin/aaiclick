Task API
---

Typed packages for writing tasks, layered on the language-agnostic
[Task Interface](task_interface.md):

| Layer          | What                                         | Who uses it                     |
|----------------|----------------------------------------------|---------------------------------|
| Task API       | Python `aaiclick`, Java `aaiclick-task-api`  | Task authors in those languages |
| Task Interface | JSON files: `in.json`, `out.json`, vocabulary | Any language; the APIs above    |

Each API type is one Task Interface dict; the API builds and serializes it
for you.

# Types

Each row is one [vocabulary](task_interface.md#vocabulary) shape:

| `_aaiclick_type` | Python                            | Java                            |
|------------------|-----------------------------------|---------------------------------|
| `task_result`    | `task_result(data=…, tasks=[…])`  | `TaskResult.of(…)`, `.spawn(…)` |
| `task`           | `@task` call, `create_task(…)`    | `Task.shell/jvm/module(…)`      |
| `group`          | `Group(name=…)`                   | `Group.of(id, …)`               |
| `upstream`       | pass the `Task` as a kwarg        | `task.ref()`, `Upstream.of(id)` |
| `group_results`  | pass the `Group` as a kwarg       | `group.results()`               |
| `native`         | any JSON-serializable value       | any value, written untagged     |
| `callable`       | a `@task` function as a value     | —                               |
| `object`         | `Object`                          | —                               |
| `view`           | `View`                            | —                               |
| `pydantic`       | a `BaseModel` instance            | —                               |

A `tasks` entry is a `Task | Group` in Python and a `DagNode` (sealed:
`Task` or `Group`) in Java. Java needs an explicit `ref()` because a handle
is a string `id` you name; Python passes the object and the serializer
writes the ref.

# Spawning tasks

The same DAG in both: two workers, a sum that consumes them, and the sum as
the parent's result.

```python
@task
async def fan_out():
    w1 = worker(n=1)
    w2 = worker(n=2)
    fin = add(a=w1, b=w2)
    return task_result(data=fin, tasks=[w1, w2, fin])
```

```java
@AaiTask
public static TaskResult fanOut() {
    Task w1 = Task.jvm("w1", "com.example.Worker").kwarg("n", 1);
    Task w2 = Task.jvm("w2", "com.example.Worker").kwarg("n", 2);
    Task fin = Task.jvm("fin", "com.example.Add").kwarg("a", w1.ref()).kwarg("b", w2.ref());
    return TaskResult.of(fin.ref(), w1, w2, fin);
}
```

Python details: [Orchestration](orchestration.md). Java image layout:
[Container Images](container_images.md).

# Java

`Task` setters mirror `create_task()`: `kwarg`, `kwargs`, `name`,
`maxRetries`, `env`, `image`, `git(remote, sha, branch, dockerfile)`,
`dependsOn(tasks…)`. With no image source a child inherits the parent's
image. `Upstream.of(long)` / `GroupResults.of(long)` reference committed
tasks and groups.

Inputs bind from `in.json` to the `@AaiTask` method's parameters by name;
any other return value is written to `out.json` via Jackson.

**Implementation**: `java/aaiclick-task-api/src/main/java/io/github/kolodkin/aaiclick/task/`
