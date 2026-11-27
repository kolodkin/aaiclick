Task Interface — The JSON Contract Between a Task and the Orchestrator
---

Vocabulary (the wire contract): `docs/architecture/contracts.md`.

The JSON-only contract that lets a process in any language take part in a
job: a **vocabulary** of tagged dicts and a **file data plane** for inputs
and results. Python and Java are bindings of it
([Python](../user_guide/task_api_python.md), [Java](../user_guide/task_api_java.md)).
The user-facing contract — files, vocabulary, spawn example, `task` fields,
limits — is in the [user guide](../user_guide/task_interface.md); this doc
covers mechanics and decisions.

# File data plane

<!-- req: file-data-plane -->
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

<!-- req: transport-per-runner -->
| Runner          | `in.json` in                                                                           | `out.json` out                                                 |
|-----------------|----------------------------------------------------------------------------------------|----------------------------------------------------------------|
| host subprocess | per-attempt temp dir, `AAICLICK_IO_DIR` in the child's env                             | read from the temp dir, removed after                          |
| docker          | `docker create` → `docker cp` → `docker start`                                         | `docker cp` of the IO dir back, then `docker rm`               |
| kubernetes      | `kubectl cp` into an `emptyDir`; init container `io-in` waits for an `in.ready` marker | `kubectl exec -c io -- cat` via a `sleep` sidecar, then delete |

<!-- req: transport-details -->
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

<!-- req: where-it-runs -->
| Worker                                 | Runs the command                                               |
|----------------------------------------|----------------------------------------------------------------|
| In-process (local mode, `job_test`)    | in the worker's event loop                                     |
| mp worker                              | in the spawned task child, which owns timeout and cancellation |
| Container runners (docker, kubernetes) | host-side on a `ShellVehicle` (multi-step launch)              |

What each runner launches for `module` / `shell` / `jvm`:
[orchestration.md, Execution layers](../architecture/topology.md#execution-layers).

# Why

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
