Language-Agnostic Task Contract
---

Any process in any language — a Node script, a bash one-liner, a Java class —
should be able to take part in an aaiclick job without importing an aaiclick
library: read its inputs as JSON, write its result as JSON, and spawn child
tasks by describing them as JSON. Today only Python can return a `Task`; the
JVM shim can return plain values but needs JDBC to do it; shell tasks have no
data plane at all.

# Intent

Make the task contract definable in JSON alone, so the Python objects
(`Task`, `Group`, `TaskResult`, `Object`) are one *binding* of the contract
rather than the contract itself. Two pieces:

1. **One discriminator key, `_aaiclick_type`**, on every tagged dict in
   `tasks.kwargs`, `tasks.result` and `remote_task_results.result_ref`,
   replacing the four keys used today (`ref_type`, `object_type`,
   `native_value`, `pydantic_type`). The vocabulary grows to cover what a
   Python task can *return*: `task`, `group`, `task_result`.
2. **A file data plane for shell tasks**: the host writes resolved kwargs to
   `in.json`, the process writes its result to `out.json`, under a directory
   named by `AAICLICK_IO_DIR`. No DB access, no library.

# Goals

- A shell task in any language can receive kwargs, return a value, and return
  child tasks, by reading one JSON file and writing one JSON file.
- The JVM shim and Python tasks speak the same vocabulary; a `task` dict
  returned from Java or Python is registered by the same host code.
- Reading the `refs.py` docstring is enough to write a shim for a new language.

# Non-goals

- `Object`/`View` in non-Python tasks. The refs stay in the vocabulary (Python
  needs them) but shell and jvm tasks still reject them at commit. Tracked in
  `docs/designs/future.md` until other languages can read ClickHouse.
- Backward compatibility with rows already stored under the old keys. The
  project is pre-release: old `kwargs`/`result` JSON becomes unreadable, no
  dual-read, no migration (the columns are untyped JSON, so no schema change).
- Replacing the JVM shim's JDBC result path with `out.json`. It keeps working
  as is with the renamed keys; moving it to the file plane is a follow-up.
- Sockets or any host-reachable network channel (needs a route from pod to
  worker that nothing in the cluster has today, and a client in every shim).

# Anti-goals

- A result-detection rule that depends on stdout content (last line, sentinel
  prefix). Logs go to stdout; a result channel must not be parsed out of them.
- A size cap on results. Kubernetes' termination-message path (4 KB) was
  considered and rejected — a task returning dozens of children with kwargs
  exceeds it.
- A second discriminator key for "specs" vs "refs". One key, one lookup.

# The vocabulary

`aaiclick/data/object/refs.py` stays the single source of truth; its constants
and Pydantic models rename to match. `ref_kind()` becomes
`value.get("_aaiclick_type")`.

| `_aaiclick_type` | Fields                                                   | Python binding                        |
|------------------|----------------------------------------------------------|---------------------------------------|
| `upstream`       | `task_id` (int = committed task; string = local handle)  | a `Task` passed as a kwarg            |
| `group_results`  | `group_id` (int or string handle)                        | a `Group` passed as a kwarg           |
| `callable`       | `entrypoint`                                             | a `@task` function passed as a value  |
| `object`         | `table`, `persistent`, `job_id`                          | `Object`                              |
| `view`           | `table`, `view_schema`                                   | `View`                                |
| `native`         | `value`                                                  | any JSON-serializable value           |
| `pydantic`       | `type` (dotted class path), `data`                       | a `BaseModel` instance                |
| `task`           | see *Task spec* below                                    | `Task` (uncommitted)                  |
| `group`          | `id` (handle), `name`, `tasks: [task…]`                  | `Group`                               |
| `task_result`    | `data`, `tasks: [task | group …]`                        | `task_result(data=…, tasks=[…])`      |

`_aaiclick_type` is a reserved key: a user dict carrying it is interpreted,
not passed through. Untagged JSON values are plain data, as today.

## Task spec

A `task` dict carries exactly `create_task()`'s parameters, so the host builds
it with `create_task(**fields)` and gets the same boundary validation
(`Literal` entry types, image exclusivity) a Python caller gets:

| Field                                           | Notes                                                           |
|-------------------------------------------------|-----------------------------------------------------------------|
| `id`                                            | Optional string handle, scoped to this one return value         |
| `name`                                          | As `create_task(name=)`                                         |
| `entry_type`                                    | `"module"`, `"shell"`, `"jvm"`                                  |
| `entrypoint`                                    | Module path or Java class; omitted for shell                    |
| `command`, `command_env`                        | Shell argv and env                                              |
| `kwargs`                                        | JSON dict; values may be tagged refs, handles included          |
| `max_retries`                                   | Default 0                                                       |
| `image` or `git_remote`/`git_sha`/`git_branch`/`dockerfile` | Image source; omitted → inherits the parent's image |
| `depends_on`                                    | List of handles — ordering without data flow (`Task.depends_on`) |

## Handles

A foreign process cannot mint snowflake IDs, so sibling references inside one
return value use string handles the host swaps for real IDs at registration:

```json
{"_aaiclick_type": "task_result",
 "data": {"_aaiclick_type": "upstream", "task_id": "fin"},
 "tasks": [
   {"_aaiclick_type": "task", "id": "w1", "name": "worker", "entry_type": "shell",
    "image": "node:22", "command": ["node", "worker.js"], "kwargs": {"n": 1}},
   {"_aaiclick_type": "task", "id": "w2", "name": "worker", "entry_type": "shell",
    "image": "node:22", "command": ["node", "worker.js"], "kwargs": {"n": 2}},
   {"_aaiclick_type": "task", "id": "fin", "name": "finalize", "entry_type": "shell",
    "image": "node:22", "command": ["node", "finalize.js"],
    "kwargs": {"parts": [{"_aaiclick_type": "upstream", "task_id": "w1"},
                         {"_aaiclick_type": "upstream", "task_id": "w2"}]}}
 ]}
```

- A **string** `task_id`/`group_id` is a handle; an **int** is a committed
  task or group, exactly as today.
- Handles resolve only within the return value they appear in. An unknown
  handle fails the *parent* task with an error naming it.
- `data` pointing at a handle gets the same treatment as Python's
  `data=finalize`: the parent's existing consumers wait for that child
  (`_hold_dependencies`).
- A `group` dict's members are inline; its handle serves `group_results` refs
  and `depends_on`.

## Where dicts are interpreted

`register_returned_tasks()` (`aaiclick/orchestration/execution/runner.py`)
accepts tagged dicts alongside Python objects: it resolves handles, builds
`Task`/`Group` objects through `create_task`, and continues down the existing
path (`_tasks_from`, `_hold_dependencies`, `commit_tasks`, image inheritance).
Python tasks may return dicts too — there is one code path.

It runs where the DB is reachable:

| Task kind                     | Who calls `register_returned_tasks`                   |
|-------------------------------|-------------------------------------------------------|
| Python module (any runner)    | the task process, inside `task_scope` — unchanged     |
| Shell (any runner)            | the host, after reading `out.json`                    |
| JVM                           | the host, on the `result_ref` read from the result row |

`serialize_task_result()` emits the renamed shapes. A `Task` in `data` still
becomes an `upstream` ref with its int ID.

# Shell data plane

## Contract for the process

- `AAICLICK_IO_DIR` names a directory. `in.json` inside it holds the task's
  kwargs as a plain JSON dict: every `upstream` ref already replaced by the
  producer's value, every `native` wrapper unwrapped. The process reads it
  with its JSON parser; nothing else to resolve.
- The process may write `out.json` in the same directory: any JSON value.
  Untagged JSON is the result as data; tagged dicts (`task_result`, `task`,
  `group`, `native`) are interpreted as above. No file → result `None`, as
  today. Nonzero exit → failure, as today; `out.json` is ignored.
- Stdout and stderr stay logs (`task_logs`), untouched.

In containers `AAICLICK_IO_DIR` is `/aaiclick`. On the host subprocess runner
it is a per-attempt temp dir (several attempts share one machine). The var is
the only path a shim reads, so a shim is written once.

```js
// Node, no dependencies
const fs = require("fs");
const dir = process.env.AAICLICK_IO_DIR;
const {n} = JSON.parse(fs.readFileSync(`${dir}/in.json`));
fs.writeFileSync(`${dir}/out.json`, JSON.stringify({_aaiclick_type: "native", value: n * 2}));
```

## Resolving kwargs on the host

A shell task's kwargs may carry only plain values, `native` wrappers and
`upstream` refs — the rule `validate_jvm_tasks()` enforces today, now applied
to `shell` and `jvm` alike by one validator (`validate_plain_data_plane_tasks`
or similar; builder's name). `object`/`view`/`callable`/`group_results`/
`pydantic` at any depth are rejected at commit. An `upstream` ref whose
producer stored a non-plain result (an `object` ref from a Python task) fails
the consumer at launch with a clear error — it cannot be known at commit.

## Per-runner transport

| Runner          | `in.json` in                                                   | `out.json` out                                            |
|-----------------|----------------------------------------------------------------|-----------------------------------------------------------|
| host subprocess | temp dir, `AAICLICK_IO_DIR` set in the child's env             | read from the temp dir; dir removed after                 |
| docker          | `docker create` → `docker cp` → `docker start --attach`        | `docker cp` from the stopped container, then `docker rm`  |
| kubernetes      | per-attempt Secret mounted at `/aaiclick/in.json` (`subPath`)  | `kubectl exec -c io -- cat /aaiclick/out.json`, see below |

**Docker**: `build_shell_run_spec` moves from a single foreground
`docker run --rm` to create/cp/start/cp/rm, because `--rm` leaves nothing to
copy from. `docker cp` rather than a bind mount so a remote `DOCKER_HOST`
keeps working. The `command_env` env-file and `cleanup_argv` kill path are
unchanged; `docker rm` joins cleanup.

**Kubernetes**: there is no shared filesystem after a container exits, so the
shell Pod gains a sidecar:

- Container `task` runs the user's argv as today; container `io` runs
  `sleep infinity`. Both mount one `emptyDir` at `/aaiclick`.
- The host waits on the **`task` container's** terminated state
  (`containerStatuses[?(@.name=="task")].state.terminated`), not the Pod
  phase, since the Pod never ends on its own. Logs are followed with
  `kubectl logs -f -c task` through the existing `follow_vehicle_output`.
- After `task` terminates: `kubectl exec -c io -- cat /aaiclick/out.json`
  (missing file → no result), then delete the Pod and its Secret as today.
  Cancellation and the reaper already delete the Pod explicitly.
- This replaces `kubectl run --attach --rm` for shell tasks with the
  manifest-based create/poll/delete path module Pods already use
  (`_build_pod_manifest`, `_pod_status`).
- The `io` image is `busybox`, overridable with `AAICLICK_K8S_IO_IMAGE` for
  air-gapped clusters; it requests no resources.

# Decisions and trade-offs

- **Files, not stdout**: stdout is the log channel on every runner; a result
  parsed out of it is fragile (a library warning after the result, a buffered
  logger at exit). A file is unambiguous and `fs.writeFileSync`-simple.
- **Files, not Kubernetes termination message**: zero sidecar but a 4 KB cap,
  which a child-task list exceeds. The sidecar costs one `busybox` container
  per shell Pod and a status-poll change; no cap.
- **Files, not sockets**: a Unix socket needs the shared filesystem files
  need; TCP needs the worker reachable from the cluster plus a client with
  framing and retry in every shim — the library we are avoiding.
- **Handles as strings on the existing `task_id` field**, not a new ref type:
  one lookup rule (`str` → sibling, `int` → committed), and a shim author
  learns nothing extra.
- **`task` fields = `create_task` parameters**: the JSON form never gains an
  option Python lacks, and validation lives in one place.
- **Untagged `out.json` is data**: a shim returning a number writes `42`, not
  a wrapper. The host wraps into `native` when storing, as it does for Python
  returns today.
- **No old-key dual-read**: pre-release; a dual-read would double every
  consumer for rows nobody keeps.

# Touch points

- `aaiclick/data/object/refs.py` — constants, models, docstring, `ref_kind`.
- `aaiclick/orchestration/execution/runner.py` — `_deserialize_value`,
  `serialize_task_result`, `register_returned_tasks` (+ handle resolution,
  dict → `create_task`), shell execution reading/writing the IO dir.
- `aaiclick/orchestration/image_injection.py` — `validate_jvm_tasks` →
  shell+jvm plain-data-plane validator.
- `aaiclick/orchestration/execution/dispatch.py`, `docker_worker.py`,
  `kubernetes_worker.py`, `mp_worker.py` — IO dir transport per runner; the
  host-side `register_returned_tasks` call for shell and jvm results.
- `aaiclick/orchestration/execution/execution_worker.py` —
  `_handle_task_result` path for jvm `result_ref` dicts.
- `java/aaiclick-task-api` — `KwargsResolver`, `TaskStore`: renamed keys.
- Tests asserting the old literals; a shell-task end-to-end test with a
  script that reads `in.json` and writes a `task_result` with handles, on
  the subprocess runner (docker/k8s runners in their existing CI jobs).
- `docs/designs/orchestration.md`, `docs/designs/java-sdk.md` — vocabulary and
  data-plane sections; `docs/designs/future.md` — Object support for
  non-Python tasks, JVM shim on the file plane.

# Left to the builder

- Exact name and placement of the merged plain-data-plane validator.
- How the host-side `register_returned_tasks` call threads through the
  vehicle/`_handle_task_result` split for jvm results (must stay inside the
  reaper invariant: the host writes terminal status, nothing else does).
- Secret vs ConfigMap for `in.json` on Kubernetes (Secret matches the
  `command_env` pattern; 1 MiB cap is far above today's kwargs).
- Temp-dir lifetime on the subprocess runner (remove after the result is read;
  keep on failure only if that helps debugging — builder's call).
