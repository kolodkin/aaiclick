# Prebuilt Images & Shell Tasks — Design

## Problem

Today every `docker`/`kubernetes` job is built from the user's git repo: a
`docker_build` task is auto-injected, clones the repo at a SHA, and runs
`docker build`. The container is then always invoked as

```
python -m aaiclick.orchestration.execution.docker_worker --task-id N
```

so the image **must** contain aaiclick plus the task's Python code. There is no
way to:

1. Run a job against a **prebuilt image** (e.g. `python:3.12`) without a build
   stage, or
2. Run an **arbitrary command** in that image instead of an aaiclick `module`
   entrypoint.

This spec adds both, and in the process folds the sprawling per-job Docker/K8s
columns into a single typed config.

## Goals

- Run a task as a literal command (an argv) in **any** runner's execution
  environment — a host subprocess, a Docker container, or a Kubernetes Pod —
  with no aaiclick required in that environment.
- Supply a prebuilt `image_tag` so the auto-injected build task is skipped
  (docker/kubernetes only).
- Replace the flat `git_*` / `dockerfile` / `image_tag` / `kubernetes_config`
  columns with one typed, discriminated `runner` config.

## Non-goals (tracked in `docs/future.md`)

- Capturing shell stdout *as* a data result (`result.data()`). Shell tasks are
  exit-code-only.
- String-form (`sh -c "…"`) commands. Argv-list only.
- Moving `entrypoint`/`kwargs` into a unified entry config (full symmetry). The
  module entry keeps its existing columns — see "Scope decision".

## Orthogonal axes

The feature is independent dials that compose freely:

| Axis | Variants | Applies to |
| --- | --- | --- |
| **entry_type** (per *task*) | `module` (today) / `shell` (new) | **every** runner — `subprocess`, `docker`, `kubernetes` |
| **runner_mode** (per *job*) | `subprocess` / `docker` / `kubernetes` | — |
| **image_source** (per *job*, nested in docker/kubernetes runner config) | `build` (git → build task → computed tag, today) / `prebuilt` (explicit `image_tag`, no build) | `docker`, `kubernetes` only (subprocess has no image) |

`entry_type` is **orthogonal to the runner**: a `shell` task is "run this argv in
the runner's environment, success = exit 0," whether that environment is a host
subprocess, a container, or a Pod. So the full matrix is `module`/`shell` ×
`subprocess`/`docker`/`kubernetes`, with `image_source` an extra dial on the two
container runners.

Notable combinations:

- `shell` + `subprocess` — run a local command on the worker host (no image).
- `shell` + `prebuilt` — run a command on `python:3.12` (headline case).
- `module` + `prebuilt` — a pre-published aaiclick image, no rebuild.
- `shell` + `build` — a command against the user's freshly built image.

## Data model

### Task entry

`entry_type` is a flat `Literal` discriminator column; the module entry keeps
its existing `entrypoint`/`kwargs` columns; shell adds its own payload columns.

```python
ENTRY_MODULE = "module"
ENTRY_SHELL = "shell"
EntryType = Literal["module", "shell"]
ENTRY_TYPES: list[EntryType] = [ENTRY_MODULE, ENTRY_SHELL]
```

`Task` columns:

| Column | Type | Notes |
| --- | --- | --- |
| `entry_type` | `String`, not null, **no DB server default** | Discriminator. No DB server default; the Python model defaults to `module` only so the framework's module-task constructors stay terse. Every *submission-boundary* path (`run_job`, API request models, CLI) sets it explicitly — that's where "explicit, not silently defaulted" is enforced. |
| `entrypoint` | `str` | Module dotted path. Required for `module`; unused for `shell`. |
| `kwargs` | `JSON` | Module args. Empty for `shell`. |
| `command` | `JSON` (`list[str]`), nullable | Argv for `shell`. Null for `module`. |
| `command_env` | `JSON` (`dict[str, str]`), nullable | Env map injected as `-e` for `shell`. Null for `module`. |

A `module` task is unchanged on disk. A `shell` task sets
`entry_type="shell"`, `command=[…]`, optional `command_env`, and leaves
`entrypoint` empty.

### Job runner config

`runner_mode` stays a flat `Literal` discriminator column (indexed, read on the
dispatch hot path). The variant-specific fields move into one typed `runner`
config, serialized to a JSON column. The flat `git_remote`, `git_sha`,
`git_branch`, `dockerfile`, `image_tag`, `kubernetes_config` columns on `Job`
and the `dockerfile`/`git_remote`/`kubernetes_config` columns on `RegisteredJob`
are removed and folded in.

```python
class ImageBuild(BaseModel):
    type: Literal["build"] = "build"
    git_remote: str
    git_sha: str
    git_branch: str | None = None
    dockerfile: str | None = None
    # image_tag is computed (aaiclick-job:<sha>), not stored here.

class ImagePrebuilt(BaseModel):
    type: Literal["prebuilt"] = "prebuilt"
    image_tag: str  # e.g. "python:3.12"

ImageSource = Annotated[ImageBuild | ImagePrebuilt, Field(discriminator="type")]

class SubprocessRunner(BaseModel):
    type: Literal["subprocess"] = "subprocess"

class DockerRunner(BaseModel):
    type: Literal["docker"] = "docker"
    image: ImageSource

class KubernetesRunner(BaseModel):
    type: Literal["kubernetes"] = "kubernetes"
    image: ImageSource
    namespace: str | None = None
    service_account: str | None = None
    image_pull_secret: str | None = None
    resources: dict | None = None  # pod resource requests/limits (passthrough)

RunnerConfig = Annotated[
    SubprocessRunner | DockerRunner | KubernetesRunner,
    Field(discriminator="type"),
]
```

`Job.runner` / `RegisteredJob.runner` hold the serialized `RunnerConfig` JSON.
`runner_mode` remains the flat discriminator and must agree with
`runner.type`.

### Effective image tag

- `build` → `image_tag = compute_image_tag(git_sha)` =
  `[<registry>/]aaiclick-job:<sha>` (unchanged).
- `prebuilt` → `image_tag = image.image_tag` verbatim.

A single helper resolves the effective tag from a `RunnerConfig` so dispatch and
the workers don't branch on the union shape.

## Execution layers

The runner's *invocation* lives at a different layer than the task it runs, and
the existing `module` path blurs the two. Naming this explicitly keeps the
`entry_type` fork honest. The same three layers exist for **every** runner
(subprocess / docker / kubernetes), host → execution environment:

1. **Host worker** — the runner's ExecuteFn driving the task (heartbeat,
   cancellation poll, wait, result read): `_run_task_in_child` (subprocess),
   `_run_task_in_container` (docker), `_run_task_in_pod` (k8s). The genuine
   worker level — identical for `module` and `shell`.
2. **Runner invocation** — *what the runner launches* in its execution
   environment: the mp child target, the `docker run <image> …` argv, or the Pod
   `command`. This is the **runner/vehicle** level, *not* the task's own
   definition. **This is where `entry_type` branches.**
3. **Task execution** — `execute_task(task)` running the module entrypoint.

For the container runners, `python -m …docker_worker --task-id N` (and the Pod
equivalent) is **layer 2, not layer 3**: a per-task **bootstrap shim**,
framework plumbing that happens to live in the `docker_worker` module (named for
the host worker — the source of the confusion). Despite "worker" in the path it
is not a queue-claiming loop: it loads one task by id, boots `orch_context()`,
calls `execute_task`, writes the result, and exits. The subprocess runner's
`module` path has the analogous shim (the mp child target that calls
`execute_task`).

The `module`/`shell` fork differs in *what occupies layer 2* — uniformly across
runners:

| entry_type | layer-2 runner invocation | layer-3 execution |
| --- | --- | --- |
| `module` | a fixed bootstrap shim (mp child target / `…docker_worker --task-id N` / Pod shim) — plumbing, *not* the user's entry | shim calls `execute_task(entrypoint)` |
| `shell` | the user's argv directly — the task definition *is* the invocation | none — the argv *is* the execution; no `execute_task` |

So for `module` the user's entry (a dotted path) executes *inside* a worker-level
shim; for `shell` the user's entry (an argv) *replaces* the shim, bypassing both
the bootstrap and `execute_task`, in whichever environment the runner provides.
Module/code names stay as-is; a later split of the in-container shim out of
`docker_worker.py` is out of scope here.

## Behavior

### Conditional build-task injection

`_create_built_job` injects the `docker_build` prerequisite **only** when
`runner.image` is an `ImageBuild`. For `ImagePrebuilt`, the entry task is created
with no build dependency and runs straight away against the given `image_tag`.

`resolve_docker_config` (renamed/retyped to return a `RunnerConfig`) keeps the
existing precedence for `build` (explicit kwarg → registered default → git
auto-detect) and short-circuits to `ImagePrebuilt` when an `image` is supplied
explicitly or on the registered job.

### Shell invocation — per runner

Every runner branches on `entry_type` at layer 2; `module` is unchanged.

- **subprocess** (`mp_worker`): `module` spawns the mp child that calls
  `execute_task` (today). `shell` runs the argv as a host subprocess
  (`asyncio` subprocess), captures exit code + output, and never imports the
  task or boots an in-process execute path.
- **docker** (`_build_docker_run_cmd`): `module` mounts the IPC tmpdir + log
  base, passes `build_runner_env()`, runs `python -m …docker_worker
  --task-id N`. `shell` runs `docker run <image> <command…>` — no IPC mount,
  no `result.json`. The log base is still bind-mounted so output lands in the
  per-task log file.
- **kubernetes** (`_build_pod_manifest`): `module` keeps the Pod bootstrap
  shim. `shell` sets the Pod container `command` to the argv — no
  `RemoteTaskResult` round-trip.

### Shell result handling

Uniform across runners: success = the process/container/Pod **exit code is 0**,
non-zero → failure. `result_ref` is always `None` (shell tasks produce no
`result.data()`). stdout/stderr is captured into the normal per-task log file
(host subprocess capture / `docker logs` / `kubectl logs`), so logs surface
uniformly in the UI/CLI. The existing cancellation/timeout paths are unchanged.

### Env summary

`command_env` is the *only* env a shell task receives in an isolated environment
(container/Pod). On the subprocess runner there is no isolation boundary — the
command runs as a child of the worker — so it inherits the worker's process env
with `command_env` overlaid on top.

| Task | Env injected |
| --- | --- |
| `module` (any runner) | `build_runner_env()` (DB URLs + framework knobs + passthrough) |
| `shell` + docker / kubernetes | `command_env` only (on top of the image's own env) |
| `shell` + subprocess | worker process env + `command_env` overlay |

## Validation

Enforced at write boundaries (Pydantic models, `run_job`, API request models,
CLI `choices=`):

- `prebuilt` requires a non-empty `image_tag`.
- `build` requires `git_remote` + `git_sha` (auto-detected as today when not
  given).
- `shell` requires a non-empty `command` list. **Valid on every runner**
  (subprocess / docker / kubernetes).
- `module` does not take a `command`.
- `runner_mode` must equal `runner.type`.

## API / submission surface

- `run_job(...)`: add `entry_type`, `command`, `command_env`, and `image`
  (prebuilt tag) parameters alongside the existing git/k8s ones. The git
  parameters and an `image` are mutually exclusive per job.
- `RunJobRequest` / `RegisteredJobRequest` (`view_models.py`): add the same
  fields.
- CLI: extend the job-submit/register commands with `--entry-type`,
  `--command`, `--command-env`, `--image`.

## Migration

One Alembic migration (via the `generate-migration` skill — never hand-written):

1. Add `Task.command`, `Task.command_env`, then add `Task.entry_type` as
   nullable, backfill every existing row to `"module"` in the same migration,
   and finalize the column as not-null. The column carries no server default —
   new rows must supply `entry_type` explicitly from code.
2. Add `Job.runner`, `RegisteredJob.runner` JSON columns; backfill from the
   existing flat columns; drop the flat `git_*`/`dockerfile`/`image_tag`/
   `kubernetes_config` columns.

## Scope decision (recorded)

`entrypoint`/`kwargs` are the backbone of the task framework (`@task`,
`task_registry`, `create_task`, `execute_task`, and most tests read them
directly). Folding them into a unified entry config (full module/shell symmetry)
was rejected as too large and unrelated to this feature. The runner-side
unification stays because those columns are localized to the Docker/K8s path.

## Blast radius

- `orchestration/models.py` — columns, discriminator constants, migration.
- `orchestration/docker_config.py` — typed `RunnerConfig` unions, prebuilt
  resolution, effective-tag helper.
- `orchestration/factories.py` — conditional build-task injection.
- `orchestration/execution/dispatch.py` — read `runner` config / effective tag;
  carry `entry_type`/`command`/`command_env` into `JobDispatch`.
- `orchestration/execution/mp_worker.py` — **subprocess shell branch** (run the
  argv as a host subprocess instead of the mp `execute_task` child).
- `orchestration/execution/docker_worker.py` — shell branch + exit-code result.
- `orchestration/execution/kubernetes_worker.py` — mirror prebuilt + shell.
- `orchestration/execution/docker_build.py` — read git fields from the build
  source.
- `orchestration/registered_jobs.py`, `view_models.py`, CLI — submission params.
- `docs/orchestration.md` — document prebuilt + shell, **and** the
  "Execution layers" distinction (host worker / container command / task
  execution; that the in-container `docker_worker --task-id N` shim is layer 2
  plumbing, not task execution). Updating this section is part of the plan, not
  optional.
- Tests across the docker/k8s/factory/dispatch suites.

## Testing

- Unit: `RunnerConfig`/`ImageSource` discriminated-union validation round-trips;
  effective-tag helper; build-task injection present for `build`, absent for
  `prebuilt`; validation rejections (shell-without-command, module-with-command,
  prebuilt-without-tag, runner_mode mismatch).
- Subprocess shell (local, no infra): a `shell` + `subprocess` task runs the
  argv on the host, reports success on exit 0 / failure on non-zero, and inherits
  the worker env with `command_env` overlaid. This path is testable on the
  default backend.
- Integration (distributed backend, GitHub Actions): `shell` + `prebuilt` job
  on a small public image runs the argv and reports success on exit 0 / failure
  on non-zero; `command_env` reaches the container; no DB creds leak into a
  shell container; `module` + `build` path unchanged.
