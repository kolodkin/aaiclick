Java Task SDK — Shim Jar (`jvm` Entry Type)
---

Java payloads run through the shell/container path, claimed by Python workers
— no second worker implementation. A `.jar` already runs on every runner as
`entry_type="shell"` on a prebuilt JVM image, with the Task Interface
(`in.json` / `out.json` under `AAICLICK_IO_DIR`,
`docs/designs/task_interface.md`). The `aaiclick-task-api` shim jar makes
that plane typed:
the image embeds the SDK, the runner invokes the shim with the class name,
and the shim binds `in.json` to the `@AaiTask` method and writes its return
value to `out.json`. No database, no runner env.

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
argv: the class name, which the image's `ENTRYPOINT` — the SDK bootstrap —
receives; the runner cannot know a JVM classpath, the image owns it. The shim
never touches `run_ids` or `task_logs`, so the host registers each attempt
(`register_host_log_run`) and follows the container's output into
`task_logs`.

## `jvm` entry type

**Implementation**: `aaiclick/orchestration/runner_config.py`

- `EntryType` Literal gains `"jvm"` (plain String column — code change only,
  no migration). `tasks.entrypoint` holds the Java class name
  (`com.example.Pipeline`), optionally `com.example.Pipeline#method` when the
  class annotates more than one method.
- `command` is rejected (like `module`); the runner passes the class name as
  the argv. `command_env` is injected as for `shell`.
- CLI (`--entry-type jvm`), `run_job()`, and `RunJobRequest` accept the new
  choice; the entrypoint/class name rides in the existing `name` /
  `entrypoint` fields.

## Validation

Enforced at commit points alongside `validate_image_sources()`
(`aaiclick/orchestration/orch_context.py` — see `commit_tasks()`) and at the
submission surface (`aaiclick/orchestration/registered_jobs.py` — see
`run_job()`):

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
   (requires `-parameters` compilation; the shim reports a clear error when
   parameter names were compiled away). Missing or extra keys are errors,
   mirroring Python's `TypeError` on bad kwargs; so is `null` for a
   primitive at any depth. The class loads through the thread context
   classloader (Spring Boot, layered fat jars).
4. Invoke the method and write its return value to `out.json` as is: the
   host treats untagged JSON as data and a `Map` / `JsonNode` carrying
   `_aaiclick_type` as a returned task DAG (`docs/designs/task_interface.md`). `void` / `null` writes nothing. On any
   exception: `Type: message` on stderr, exit code 1.

The shim never writes task status — the host worker owns that (reaper
invariant, see the `docker_worker` module docstring); it reads `out.json`
only after an exit code 0.

# Java module

`java/` is a Maven monorepo: `java/pom.xml` (parent `aaiclick-parent`) with
one module, `aaiclick-task-api`, whose only dependency is Jackson. Packages
live under `io.github.kolodkin.aaiclick.task` to match the Maven Central
namespace.

| Class            | Role                                                                     |
|------------------|--------------------------------------------------------------------------|
| `AaiTask`        | Runtime method annotation marking a task entrypoint                      |
| `AaiTaskShim`    | The bootstrap `main()` — steps 1–4 above                                 |
| `TaskRegistry`   | Class name → annotated `Method` (`Class#method` disambiguation)          |
| `KwargsBinder`   | JSON kwargs → typed method arguments via Jackson                         |
| `TaskResult`, `Task`, `Group`, `Upstream`, `GroupResults` | Typed return DAG; Jackson writes the Task Interface vocabulary |

User code:

```java
public class Pipeline {
    @AaiTask
    public static Map<String, Object> aggregate(String date, int window) {
        return Map.of("rows", 42, "date", date);
    }
}
```

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

# Testing

The shim's only I/O is two files, so the suite is infra-free: each test gets
a temp dir, writes `in.json`, runs `AaiTaskShim.run` and reads `out.json`.

The docker-runner e2e covers what the unit suite cannot: a `jvm` task between
two Python tasks, each in its own container — the real `docker create` /
`docker cp` handoff of `in.json` into a JVM image, the host resolving an
upstream Python result into it, `out.json` read back, and a Python consumer
of the jvm return value. The fixture image (`test_e2e/fixtures/jvm_task/`)
builds the SDK from the checkout, so the shim under test is the commit's.

# Publishing

**Implementation**: `.github/workflows/publish.yaml`

`aaiclick-task-api` publishes to Maven Central via the Central Publisher
Portal on the **same tag** as the Python package — lockstep versioning; the
compatibility contract is a release's file data plane and task semantics.

- Namespace `io.github.kolodkin` auto-verifies against the GitHub account.
- The parent POM uses the CI-friendly `${revision}` version
  (flatten-maven-plugin); the publish job passes `-Drevision=X.Y.Z` from the
  tag.
- The `release` profile adds GPG signing plus sources/javadoc jars and the
  `central-publishing-maven-plugin`.
- Secrets: `MAVEN_CENTRAL_USERNAME` / `MAVEN_CENTRAL_PASSWORD` (portal
  token), `MAVEN_GPG_PRIVATE_KEY` / `MAVEN_GPG_PASSPHRASE`.
- De-risk with a one-time `0.0.x` dry-run publish (manual
  `workflow_dispatch`) before the first real lockstep release.
- The Java leg runs after the PyPI upload, so the release is gated on
  `mvn verify` at the release version first: a broken jar blocks the
  irreversible upload instead of orphaning a tagged Python release.
