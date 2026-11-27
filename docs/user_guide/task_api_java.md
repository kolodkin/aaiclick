Task API: Java
---

The `aaiclick-task-api` jar, layered on the
[Task Interface](task_interface.md).

# Types

| `_aaiclick_type` | Java                                     |
|------------------|------------------------------------------|
| `task_result`    | `TaskResult.of(data, …)`, `.spawn(…)`    |
| `task`           | `Task.shell` / `.jvm` / `.module(id, …)` |
| `group`          | `Group.of(id, …)`                        |
| `upstream`       | `task.ref()`                             |
| `group_results`  | `group.results()` — module children only |
| `native`         | any value, written untagged              |
| `callable`       | —                                        |
| `object`         | —                                        |
| `view`           | —                                        |
| `pydantic`       | —                                        |

A `tasks` entry is a `DagNode` (sealed: `Task` or `Group`). `id` is the
handle siblings reference; it may be `null` unless one does.

# Example

Two workers, a sum that consumes them, and the sum as the parent's result:

```java
@AaiTask
public static TaskResult fanOut() {
    Task w1 = Task.jvm("w1", "com.example.Worker").kwarg("n", 1);
    Task w2 = Task.jvm("w2", "com.example.Worker").kwarg("n", 2);
    Task fin = Task.jvm("fin", "com.example.Add").kwarg("a", w1.ref()).kwarg("b", w2.ref());
    return TaskResult.of(fin.ref(), w1, w2, fin);
}
```

# Inputs and results

`in.json` binds to the `@AaiTask` method's parameters by name via Jackson
(compile with `-parameters`). The return value is written to `out.json`
via Jackson; `void` / `null` → no result.

`Task` setters mirror the [`task` fields](task_interface.md#task-fields): `kwarg`, `kwargs`, `name`,
`maxRetries`, `env`, `image`, `git(remote, sha, branch, dockerfile)`,
`dependsOn(tasks…)`.

More: [Container Images](container_images.md) for the image layout.

# Packaging and submission

Image contract — the `ENTRYPOINT` *is* the shim:

```dockerfile
FROM eclipse-temurin:21-jre
COPY target/app.jar /app/app.jar
COPY target/libs /app/libs
ENTRYPOINT ["java", "-cp", "/app/app.jar:/app/libs/*", \
            "io.github.kolodkin.aaiclick.task.AaiTaskShim"]
```

Submission:

```python
await internal_api.run_job(RunJobRequest(
    name="aggregate", entry_type="jvm",
    kwargs={"date": "2026-08-20", "window": 7},
    image="ghcr.io/example/pipeline:1.0",
))
```

!!! warning "Compile task classes with `-parameters`"
    Kwargs bind by Java parameter name. Without
    `<parameters>true</parameters>` (maven-compiler-plugin) the names compile
    away to `arg0…` and the shim fails with an explicit message instead of
    guessing positionally.
