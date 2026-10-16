Task Interface — The JSON Contract Between a Task and the Orchestrator
---

What a task process and the orchestrator say to each other, in JSON alone,
so a process in any language takes part in a job with nothing but a JSON
parser. Two halves: a **vocabulary** of tagged dicts (refs, data, returned
tasks) and a **file data plane** (how a non-Python process gets its inputs
and hands back its result). The Python objects (`Task`, `Group`,
`TaskResult`, `Object`) and the Java shim are bindings of the contract, not
the contract.

User guide: `docs/user_guide/task_interface.md`.

# Vocabulary

Task kwargs and results are JSON. Plain values are data; a dict carrying the
reserved key `_aaiclick_type` is one of the shapes below, and an unknown
value is an error, never data.

| `_aaiclick_type` | Fields                                                    | Python binding                        |
|------------------|-----------------------------------------------------------|---------------------------------------|
| `upstream`       | `task_id` (int = committed task; string = local handle)   | a `Task` passed as a kwarg            |
| `group_results`  | `group_id` (int or handle)                                | a `Group` passed as a kwarg           |
| `callable`       | `entrypoint`                                              | a `@task` function passed as a value  |
| `object`         | `table`, `persistent`, `job_id`                           | `Object`                              |
| `view`           | `table`, `view_schema` (the serialized `ViewSchema`)      | `View`                                |
| `native`         | `value`                                                   | any JSON-serializable value           |
| `pydantic`       | `type` (dotted class path), `data`                        | a `BaseModel` instance                |
| `task`           | `create_task()`'s parameters + `id` (handle), `depends_on` | `Task` (uncommitted)                 |
| `group`          | `id` (handle), `name`, `tasks: [task…]`                   | `Group`                               |
| `task_result`    | `data`, `tasks: [task | group …]`                         | `task_result(data=…, tasks=[…])`      |

`job_id` on an `object` ref marks the producing task's job — the background
worker skips tables with a non-NULL `job_id` pin until the job completes. The
table's `Schema` is read back from `table_registry.schema_doc` via
`read_table_schema()` — see `aaiclick/data/data_context/lifecycle.py`.

**Return values**: `None` → `null`; Object/View → serialized ref + `job_id`;
a `BaseModel` → `pydantic`; any other value → `native`.

**Implementation**: `aaiclick/data/object/refs.py` (single source of truth:
constants, Pydantic models, `ref_kind()`);
`aaiclick/orchestration/execution/runner.py` — see `serialize_task_result()`,
`_deserialize_value()`.

## Returned tasks and handles

`task`, `group` and `task_result` are the JSON form of what a task function
may return, so any task spawns children by returning them — a Python task as
a dict, a shell or jvm task by writing them to `out.json`. A `task` dict
carries `create_task()`'s parameters (`name`, `entry_type`, `entrypoint` or
`command`, `command_env`, `kwargs`, `max_retries`, `image` or the `git_*`
build fields) and passes the same entry validation a Python caller gets.

A foreign process cannot mint snowflake ids, so siblings in one return value
refer to each other through string **handles** on `id`: a string `task_id` /
`group_id` inside an `upstream` / `group_results` ref names a sibling, an int
names a committed task. The host swaps handles for the ids it mints at
registration, and every handle a task consumes becomes a dependency edge —
as a `Task` kwarg does in Python — alongside `depends_on`. Handles are scoped
to that one return value; an unknown or duplicate handle, or one of the wrong
kind (a task where a group is expected), fails the parent. `data` may be a
plain value (a `native` wrapper unwraps, like `out.json`'s top level) or a
task handle, handed on as the Task itself so the parent's consumers wait for
it — Python's `task_result(data=child)`.

```json
{"_aaiclick_type": "task_result",
 "data": {"_aaiclick_type": "upstream", "task_id": "fin"},
 "tasks": [
   {"_aaiclick_type": "task", "id": "w1", "entry_type": "shell", "command": ["node", "w.js"], "kwargs": {"n": 1}},
   {"_aaiclick_type": "task", "id": "w2", "entry_type": "shell", "command": ["node", "w.js"], "kwargs": {"n": 2}},
   {"_aaiclick_type": "task", "id": "fin", "entry_type": "shell", "command": ["node", "fin.js"],
    "kwargs": {"parts": [{"_aaiclick_type": "upstream", "task_id": "w1"},
                         {"_aaiclick_type": "upstream", "task_id": "w2"}]}}
 ]}
```

**Implementation**: `aaiclick/orchestration/execution/runner.py` — see
`register_returned_tasks()`, `_build_dag_from_dicts()`, `_wire_dag_dicts()`.

# File data plane

A shell or jvm task takes its inputs and returns its result through two JSON
files in the directory named by `AAICLICK_IO_DIR`:

- `in.json` — the kwargs as a plain JSON object. The host resolves them before
  launch: every `upstream` ref is replaced by the producer's value, every
  `native` wrapper unwrapped. The process reads a plain object; nothing is
  left to resolve.
- `out.json` — the result, if the process writes it: any JSON value. Untagged
  JSON is data; a top-level `native` wrapper unwraps; a `task_result` /
  `task` / `group` dict registers children exactly as a Python return would.
  No file → no result. Nonzero exit → the file is ignored and the task fails.
- Stdout and stderr are logs (`task_logs`), untouched.

Kwargs may carry only plain values, `native` wrappers and `upstream` refs —
`object` / `view` / `callable` / `group_results` / `pydantic` refs are
rejected at commit by `validate_plain_data_plane_tasks` (shell and jvm
alike). An upstream producer that stored an Object fails the consumer at
launch, since that is only knowable once the producer ran. Objects in
non-Python tasks are unsupported (`future.md`).

## Transport per runner

| Runner          | `in.json` in                                                  | `out.json` out                                   |
|-----------------|---------------------------------------------------------------|--------------------------------------------------|
| host subprocess | per-attempt temp dir, `AAICLICK_IO_DIR` in the child's env    | read from the temp dir, removed after            |
| docker          | `docker create` → `docker cp` → `docker start`                | `docker cp` of the IO dir back, then `docker rm` |
| kubernetes      | `kubectl cp` into an `emptyDir` while init container `io-in` waits for an `in.ready` marker | `kubectl exec -c io -- cat` through a `sleep` sidecar, then delete |

In containers `AAICLICK_IO_DIR` is `/aaiclick`, and the argv follows the
image's `ENTRYPOINT` on both runners (`docker create <image> <argv>`, Pod
`args`) — which is how the jvm shim, the image's `ENTRYPOINT`, receives its
class name. The host-side temp dir is world-writable so an image running as
a non-root user can write `out.json`; `docker cp` copies the whole IO dir
back, so an absent `out.json` needs no daemon error text to interpret.

A Pod has no filesystem the host can reach in either direction, hence two
helper containers from one image (`busybox:1.36`, override with
`AAICLICK_K8S_IO_IMAGE` on air-gapped clusters; no resources requested):
`io-in` holds the Pod until `in.json` and its `in.ready` marker have landed
(`kubectl cp` extracts in place, so the marker is what proves the file is
whole), `io` keeps the Pod alive so `out.json` can be read after `task`
exits. The host waits on the `task` container's state, since the Pod never
ends on its own; `activeDeadlineSeconds` (the task timeout plus slack, else
seven days) bounds a Pod a crashed worker orphans, and a Pod that reaches
`Failed` / `Succeeded` before the task container ran ends the wait with an
error. `kubectl cp` / `exec` need `pods/exec` in the worker's Role; a copy
that keeps failing fails the task with kubectl's reason after
`COPY_IN_ATTEMPTS`. `command_env` values never sit on an argv (visible in
`ps`): docker reads them from a private `--env-file`, kubernetes from a
per-attempt Secret deleted with the Pod.

In an isolated environment a shell or jvm task receives **only**
`command_env` plus `AAICLICK_IO_DIR` — never the aaiclick runner env — so
no DB credentials reach an arbitrary image. The subprocess runner has no
isolation boundary, so the command inherits the worker's process env with
`command_env` overlaid.

## Where it runs

On the in-process paths (the local-mode server's worker, `job_test` /
`ajob_test`) the command runs in the worker's own event loop; on the mp
worker a subprocess shell task runs in the spawned task child module tasks
use (it enforces the timeout and polls cancellation itself; the parent's
kill is a `CHILD_TIMEOUT_GRACE` backstop). Container shell and jvm tasks run
host-side on a `ShellVehicle`, like module containers — their launch is a
multi-step sequence, not one argv — and the host interprets `out.json` after
exit under the task's info, so children it returned inherit its image.

**Implementation**: `aaiclick/orchestration/execution/io_plane.py`;
`aaiclick/orchestration/execution/runner.py` — see `execute_shell_task()`,
`stage_in_json()`, `resolve_plain_kwargs()`, `finish_shell_result()`;
`aaiclick/orchestration/execution/execution_worker.py` — see
`ShellVehicle`, `drive_shell_vehicle()`;
`aaiclick/orchestration/execution/dispatch.py` — see `dispatch_execute()`;
`aaiclick/orchestration/execution/docker_worker.py` — see
`_DockerShellVehicle`; `aaiclick/orchestration/execution/kubernetes_worker.py`
— see `build_shell_pod_manifest()`, `_KubernetesShellVehicle`;
`aaiclick/orchestration/execution/mp_worker.py` — see `_child_run_task()`;
`aaiclick/orchestration/image_injection.py` — see
`validate_plain_data_plane_tasks()`, `stamp_inherited_image()`.

# Bindings

| Binding | Inputs                                     | Result                                             |
|---------|--------------------------------------------|----------------------------------------------------|
| Python  | kwargs deserialized in-process (all refs)  | the return value, serialized in-process            |
| jvm     | `in.json` bound to the `@AaiTask` method   | the return value written to `out.json` as is       |
| shell   | `in.json`                                  | `out.json`                                         |

A `jvm` task is a shell task whose argv is the class name; the shim is the
SDK jar in the user's image (`docs/designs/java-sdk.md`).

# Decisions

- **Files, not stdout**: stdout is the log channel on every runner; a result
  parsed out of it is fragile (a library warning after the result, a
  buffered logger at exit). A file is unambiguous and one write call.
- **Files, not Kubernetes termination messages**: no sidecar but a 4 KB cap,
  which a returned child-task list exceeds.
- **Files, not sockets**: a Unix socket needs the shared filesystem files
  need; TCP needs the worker reachable from the cluster plus a client with
  framing and retry in every shim — the library the interface avoids.
- **`kubectl cp`, not a Secret, for `in.json`**: `in.json` holds resolved
  upstream results, so a 1 MiB object cap would be a result cap.
- **Handles on the existing `task_id` field**, not a new ref type: one rule
  (`str` → sibling, `int` → committed), nothing extra to learn.
- **`task` fields = `create_task` parameters**: the JSON form never gains an
  option Python lacks, and validation lives in one place.
- **Untagged `out.json` is data**: a shim returning a number writes `42`.
- **One discriminator, no old-key dual-read**: pre-release; rows written
  before the rename are unreadable rather than doubling every consumer.
