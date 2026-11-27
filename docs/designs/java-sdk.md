Java Task SDK — Shim Jar (`jvm` Entry Type)
---

Java payloads run through the shell/container path, claimed by Python
workers — no second worker implementation. A `.jar` already runs on every
runner as `entry_type="shell"` on a JVM image, speaking the Task Interface
(`in.json` / `out.json` under `AAICLICK_IO_DIR`,
`docs/designs/task_interface.md`). The `aaiclick-task-api` shim jar makes
that plane typed: the image embeds the SDK, the runner invokes the shim with
the class name, and the shim binds `in.json` to the `@AaiTask` method and
writes its return value to `out.json`. No database, no runner env.

# Architecture

`jvm` is the third row of the entry-type/layers table in
`docs/designs/orchestration.md` ("Execution layers"): the layer-2 runner
invocation is the image's own `ENTRYPOINT` (the SDK shim) plus the class
name as its one argument, and layer-3 is the shim reflecting and invoking
the `@AaiTask` method.

A `jvm` task is dispatched exactly like a container `shell` task: the worker
resolves its kwargs into `in.json`, launches the container / Pod on the shell
vehicle with **only** `command_env` and `AAICLICK_IO_DIR` (no DB credentials
reach the image), waits, and reads `out.json` back. The one difference is the
argv: the class name, for the image's `ENTRYPOINT` (the SDK bootstrap); the
runner cannot know a JVM classpath, the image owns it. The shim never touches
`task_runs` or `task_logs`, so the host registers each attempt and follows
the container's output into `task_logs`.

## `jvm` entry type

**Implementation**: `aaiclick/orchestration/runner_config.py`

- `"jvm"` is an `EntryType` value on a plain String column, so adding it
  needed no migration. `tasks.entrypoint` holds the Java class name
  (`com.example.Pipeline`), or `com.example.Pipeline#method` when the class
  annotates more than one method.
- `command` is rejected (like `module`); the runner passes the class name as
  the argv. `command_env` is injected as for `shell`.
- The CLI (`--entry-type jvm`) and `run_job` accept it; the class name rides
  in the existing `name` / `entrypoint` fields.

## Validation

Enforced at commit and at submission (`run_job`):

- A `jvm` task's kwargs may carry only plain values, `native` wrappers, and
  upstream refs — the file data plane, shared with `shell`. Object/View,
  pydantic, callable, and group refs at any depth are rejected at commit
  (each kwarg checked on its own, as the host resolves them).
- A `jvm` task requires its own `image_source` on a docker/kubernetes job —
  there is no host-subprocess JVM contract, so a `jvm` task that would fall
  back to the subprocess runner is rejected. It never inherits the committing
  task's Python image, which has no JVM entrypoint.
- A `jvm` task's entrypoint (class name) must be non-empty.
- Results are never auto-converted to Objects: the shim writes `native`
  values (or a returned task DAG), which downstream Python tasks consume as
  native values.

# Data-plane contract (what the shim does)

1. Take the class name from argv (`com.example.Pipeline`, or
   `com.example.Pipeline#method`) and the directory from `AAICLICK_IO_DIR`.
2. Read `in.json`: a JSON object of kwargs the host has already resolved to
   plain values (every `upstream` ref replaced, every `native` wrapper
   unwrapped), so a parameter named `_aaiclick_type` is just a parameter.
3. Jackson-bind the kwargs to the `@AaiTask` method's parameters by name
   (requires `-parameters` compilation — see the warning below). Missing or extra keys are errors,
   mirroring Python's `TypeError` on bad kwargs; so is `null` for a
   primitive at any depth. The class loads through the thread context
   classloader (Spring Boot, layered fat jars).
4. Invoke the method and write its return value to `out.json` as is: the
   host treats untagged JSON as data and a `Map` / `JsonNode` carrying
   `_aaiclick_type` as a returned task DAG (`docs/designs/task_interface.md`).
   `void` / `null` writes nothing. On any exception: `Type: message` on
   stderr, exit code 1.

The shim never writes task status — the host worker owns it (the reaper
invariant) and reads `out.json` only after exit code 0.

# Java module

**Implementation**: `java/aaiclick-task-api/`

`java/` is a Maven monorepo: `java/pom.xml` (parent `aaiclick-parent`) with
one module, `aaiclick-task-api`, whose only dependency is Jackson. Packages
live under `io.github.kolodkin.aaiclick.task` to match the Maven Central
namespace. User code sees the `@AaiTask` method annotation and the typed
return-DAG classes (`TaskResult`, `Task`, `Group`, `Upstream`,
`GroupResults`), which Jackson writes as the Task Interface vocabulary;
`AaiTaskShim` is the bootstrap `main()`.

User code:

```java
public class Pipeline {
    @AaiTask
    public static Map<String, Object> aggregate(String date, int window) {
        return Map.of("rows", 42, "date", date);
    }
}
```

Image contract, submission and the `-parameters` requirement: [Task API: Java](../user_guide/task_api_java.md#packaging-and-submission).
