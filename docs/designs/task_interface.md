Task Interface — The JSON Contract Between a Task and the Orchestrator
---

The JSON-only contract that lets a process in any language take part in a
job: a **vocabulary** of tagged dicts and a **file data plane** for inputs
and results. Python and Java are bindings of it
([Python](../user_guide/task_api_python.md), [Java](../user_guide/task_api_java.md)).
The user-facing contract — files, vocabulary, spawn example, `task` fields,
limits — is in the [user guide](../user_guide/task_interface.md); this
document covers mechanics and decisions.

# Vocabulary

**Implementation**: `aaiclick/data/object/refs.py`

Shapes and fields: [user guide, Vocabulary](../user_guide/task_interface.md#vocabulary).

- `job_id` on an `object` ref pins the table: the background worker skips it
  until the job completes. Its schema is read back from the table registry.
- Python return values: `None` → `null`; Object/View → ref + `job_id`;
  `BaseModel` → `pydantic`; anything else → `native`.
- No compat for the pre-#484 keys (`ref_type`, `object_type`, `native_value`):
  rows using them read as plain dicts. No deployment predates #484; drain jobs
  before any future key change.

## Returned tasks and handles

A `task` dict is built through `create_task()`, so it gets Python's entry
validation. Rules enforced at registration:

- A string `task_id` / `group_id` is a handle naming a sibling in the same
  return value; an int names a committed task. Handles become minted ids, and
  each consumed handle becomes a dependency edge, alongside `depends_on`.
- An unknown, duplicate or wrong-kind handle (a task where a group is
  expected) fails the parent.
- `data` is a plain value (`native` unwraps) or a task handle — the parent's
  consumers then wait for that task, like `task_result(data=child)`. Any
  other handle in `data` is an error.

Lifecycle of the user guide's fan-out (`w1`, `w2` → `fin`; ids illustrative):

| Step                        | `fin`'s `values` kwarg                                         |
|-----------------------------|----------------------------------------------------------------|
| Parent writes `out.json`    | `[{"_aaiclick_type": "upstream", "task_id": "w1"}, …"w2"]`     |
| Host mints ids after exit 0 | `w1` → 7301, `w2` → 7302, `fin` → 7303                         |
| Host swaps handles, commits | `[{…, "task_id": 7301}, {…, "task_id": 7302}]` + edges to 7303 |
| `fin` launches              | `in.json`: `{"values": [2, 4]}`                                |

Handles live only in that `out.json`; the parent's result points at 7303.


# File data plane

Host side of the `in.json` / `out.json` exchange under `AAICLICK_IO_DIR`:

- **Before launch**: the host replaces each `upstream` ref with
  the producer's stored value and unwraps `native`. Any other ref is rejected
  at commit; a producer that stored an
  Object fails the consumer at launch, the first point it is knowable.
  Objects in non-Python tasks: `future.md`.
- **After exit 0**: the host reads `out.json` — a top-level
  `native` unwraps, a returned DAG registers under the task's info (children
  inherit its image), data is serialized like a Python return. Any other exit
  ignores the file.

## Transport per runner

| Runner          | `in.json` in                                                                       | `out.json` out                                        |
|-----------------|------------------------------------------------------------------------------------|-------------------------------------------------------|
| host subprocess | per-attempt temp dir, `AAICLICK_IO_DIR` in the child's env                         | read from the temp dir, removed after                 |
| docker          | `docker create` → `docker cp` → `docker start`                                     | `docker cp` of the IO dir back, then `docker rm`      |
| kubernetes      | `kubectl cp` into an `emptyDir`; init container `io-in` waits for an `in.ready` marker | `kubectl exec -c io -- cat` via a `sleep` sidecar, then delete |

- **Containers**: `AAICLICK_IO_DIR` is `/aaiclick`; the argv follows the
  image's `ENTRYPOINT`, which is how the jvm shim gets its class name.
- **Docker**: the host IO dir is world-writable (non-root images can write
  `out.json`); copying the whole dir back makes a missing `out.json` plain.
- **Kubernetes**: helpers use `busybox:1.36` (`AAICLICK_K8S_IO_IMAGE`).
  `io-in` waits for the `in.ready` marker, proving `in.json` is whole; `io`
  keeps the Pod alive to read `out.json`. The host watches the `task`
  container; `activeDeadlineSeconds` bounds orphaned Pods. Needs `pods/exec`.
- **Env**: `command_env` never goes on an argv (docker `--env-file`,
  kubernetes per-attempt Secret). Containers get no aaiclick env; the
  subprocess runner inherits the worker's.

## Where it runs

**Implementation**: `aaiclick/orchestration/execution/io_plane.py`

| Worker                                      | Runs the command                                                 |
|---------------------------------------------|------------------------------------------------------------------|
| In-process (local mode, `job_test`)         | in the worker's event loop                                       |
| mp worker                                   | in the spawned task child, which owns timeout and cancellation   |
| Container runners (docker, kubernetes)      | host-side on a `ShellVehicle` (multi-step launch)                |

What each runner launches for `module` / `shell` / `jvm`:
[orchestration.md, Execution layers](orchestration.md#execution-layers).

# Decisions

- **Files, not stdout**: stdout is the log channel; a result parsed from it
  breaks on a late library warning or buffered logger.
- **Files, not Kubernetes termination messages**: 4 KB cap, which a returned
  child-task list exceeds.
- **Files, not sockets**: a Unix socket needs the same shared filesystem;
  TCP needs the worker reachable plus a framing/retry client in every shim.
- **`kubectl cp`, not a Secret, for `in.json`**: a 1 MiB Secret cap would cap
  upstream results.
- **Handles on the existing `task_id` field**, not a new ref type: `str` →
  sibling, `int` → committed.
- **`task` fields = `create_task` parameters**: JSON never gains an option
  Python lacks; validation lives in one place.
- **Untagged `out.json` is data**: a shim returning a number writes `42`.
- **One discriminator, no old-key dual-read**: pre-release; older rows are
  unreadable rather than doubling every consumer.
