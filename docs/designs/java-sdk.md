Java Task SDK — Shim Jar (`jvm` Entry Type)
---

Java payloads run through the existing shell/container path, claimed by Python
workers — no second worker implementation. A `.jar` already runs on every
runner via `entry_type="shell"` plus a prebuilt JVM image; what shell tasks
lack is the data plane: typed kwargs, a return value, downstream consumption.
The `aaiclick-task-api` shim jar closes that gap: the user's image embeds the
SDK, the runner invokes the shim, and the shim speaks the same SQL result
contract as the Python container bootstrap (`remote_result`).

# Architecture

The three-layer split from `docs/designs/orchestration.md` ("Execution layers")
gains one row:

| entry_type | layer-2 runner invocation                                        | layer-3 execution                          |
|------------|------------------------------------------------------------------|--------------------------------------------|
| `module`   | the aaiclick Python shim (`remote_result --task-id N ...`)       | `execute_task` imports and runs entrypoint |
| `shell`    | the user's argv directly — the definition *is* the invocation    | none — the argv *is* the execution         |
| `jvm`      | the image's own `ENTRYPOINT` (the SDK shim) + `--task-id N ...`  | the shim reflects and invokes the `@AaiTask` method |

A `jvm` task is dispatched exactly like a `module` container task: the Python
worker claims it, launches a detached container / Pod with the **full runner
env** (same trust model as module images — an image that embeds the SDK is
framework-trusted, unlike a vanilla `shell` image), waits, and reads the
`remote_task_results` row back. The only difference is the container command:
the runner passes just `--task-id N --run-epoch M` as *arguments*, so the
image's `ENTRYPOINT` — which the user points at the SDK bootstrap — receives
them. The runner cannot know a JVM classpath; the image owns it.

**Implementation**: `aaiclick/orchestration/execution/docker_worker.py` — see
`_build_docker_run_cmd()`; `aaiclick/orchestration/execution/kubernetes_worker.py`
— see `_pod_manifest()`.

## `jvm` entry type

- `EntryType` Literal gains `"jvm"` (plain String column — code change only,
  no migration). `tasks.entrypoint` holds the Java class name
  (`com.example.Pipeline`), optionally `com.example.Pipeline#method` when the
  class annotates more than one method.
- `command` is rejected (like `module`); `command_env` is not injected
  (`jvm` containers get the runner env, mirroring `module`).
- CLI (`--entry-type jvm`), `run_job()`, and `RunJobRequest` accept the new
  choice; the entrypoint/class name rides in the existing `name` /
  `entrypoint` fields.

**Implementation**: `aaiclick/orchestration/runner_config.py` — see
`EntryType`, `validate_task_entry()`.

## Validation

Enforced at commit points alongside `validate_image_sources()`
(`aaiclick/orchestration/orch_context.py` — see `commit_tasks()`) and at the
submission surface (`aaiclick/orchestration/registered_jobs.py` — see
`run_job()`):

- A `jvm` task must not receive Object/View refs as kwargs — the shim has no
  ClickHouse data plane. Any nested kwargs dict carrying `object_type` is
  rejected at commit.
- A `jvm` task requires an `image_source` on a docker/kubernetes job — there
  is no host-subprocess JVM contract, so a `jvm` task that would fall back to
  the subprocess runner is rejected.
- A `jvm` task's entrypoint (class name) must be non-empty.
- Results are never auto-converted to Objects: the shim writes plain values
  only (`{"native_value": ...}`), which downstream Python tasks consume as
  native values.

**Implementation**: `aaiclick/orchestration/image_injection.py` — see
`validate_jvm_tasks()`.

# Data-plane contract (what the shim does)

Mirror of the Python layer-2 bootstrap
(`aaiclick/orchestration/execution/remote_result.py` — see
`remote_entry_main()`):

1. Parse `--task-id N --run-epoch M` from argv; read `AAICLICK_SQL_URL` from
   the env and translate it to JDBC (`postgresql+asyncpg://…` →
   `jdbc:postgresql://…`; `sqlite+aiosqlite:///…` → `jdbc:sqlite:…` for
   tests).
2. Load the task row (`entrypoint`, `kwargs`) by id.
3. Resolve kwargs: recurse into JSON objects/arrays; an
   `{"ref_type": "upstream", "task_id": K}` dict resolves to task K's
   `result` column (task must be `COMPLETED`); `{"native_value": v}` unwraps
   to `v`; a dict carrying `object_type` (Object/View ref) or any other
   `ref_type` fails the task with a clear error — the JVM data plane is plain
   values only.
4. Jackson-bind the resolved kwargs to the `@AaiTask` method's parameters by
   name (requires `-parameters` compilation; the shim reports a clear error
   when parameter names were compiled away). Missing or extra keys are
   errors, mirroring Python's `TypeError` on bad kwargs.
5. Invoke the method; serialize the return value with Jackson and write the
   `remote_task_results` row keyed `(task_id, run_epoch)`:
   `success=true, result_ref={"native_value": <json>}` (`null` return / `void`
   → SQL NULL `result_ref`). On any exception:
   `success=false, error="ExceptionType: message"`, exit code 1.

The shim never writes terminal `Task` status — the host worker owns that
(reaper invariant, see the `docker_worker` module docstring). A stale attempt
writes under a stale `(task_id, run_epoch)` key and is ignored.

# Java module

`java/` is a Maven monorepo: `java/pom.xml` (parent `aaiclick-parent`) with
one module, `aaiclick-task-api`. Salvaged from the removed
`java/aaiclick-worker` (git history): `NamedParamSql` (named-parameter SQL),
`Db` (JDBC factory), the URL-translation logic from `WorkerConfig`. Packages
live under `io.github.kolodkin.aaiclick.task` to match the Maven Central
namespace.

| Class          | Role                                                                     |
|----------------|--------------------------------------------------------------------------|
| `AaiTask`      | Runtime method annotation marking a task entrypoint                      |
| `AaiTaskShim`  | The bootstrap `main()` — steps 1–5 above                                 |
| `TaskRegistry` | `entrypoint` string → annotated `Method` (`Class#method` disambiguation) |
| `KwargsBinder` | JSON kwargs → typed method arguments via Jackson                         |
| `TaskStore`    | Task-row load, upstream-ref resolution, result-row upsert                |
| `SqlConfig`    | `AAICLICK_SQL_URL` → JDBC url + credentials                              |
| `Db`           | JDBC connection factory                                                  |
| `NamedParamSql`| `:named` SQL → positional `PreparedStatement` SQL                        |

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

The SDK's only backend is SQL over JDBC, so the suite is infra-free: tests run
against per-test SQLite files (`sqlite-jdbc`, test scope) with a DDL fixture
covering the `tasks` / `remote_task_results` subset the shim touches. The CI
job additionally generates the SQLite schema from the Python SQLModel metadata
and points the Java suite at it (`AAICLICK_TEST_SQLITE_DB`), guarding
cross-language schema drift. Production containers use the
PostgreSQL driver (a runtime dependency of the SDK); URL translation is
unit-tested for both schemes.

# Publishing

`aaiclick-task-api` publishes to Maven Central via the Central Publisher
Portal on the **same tag** as the Python package — lockstep versioning; the
compatibility contract is a release's PostgreSQL schema and task semantics.

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

**Implementation**: `.github/workflows/publish.yaml` — see the `publish-java`
job; `.github/workflows/test.yaml` — see the `java-sdk` job.
