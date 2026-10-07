Worker-Owned Runner
---

The runner that launches a task's container — Docker daemon or Kubernetes
cluster — is a property of the **execution worker's deployment**, not of the
job. This spec moves it there: `runner_mode` leaves `Job`, `RegisteredJob`,
the CLI, REST and MCP, and the worker reads it once from `AAICLICK_RUNNER`.
Users keep exactly the decision that is theirs: whether a job runs in a
container at all, and from which image.

# Motivation

Today `register-job --runner subprocess|docker|kubernetes` stores the runner
per job and every surface mirrors it (`RegisteredJob.runner_mode`,
`Job.runner_mode`, `Job.runner`, `RegisterJobRequest.runner_mode`). That models a
deployment fact as a user choice:

- The scaffolds already fix the mode. `compose init` ships the
  `aaiclick-docker` worker with the host socket mounted; `k8s init` ships the
  `aaiclick-kubectl` worker with a ServiceAccount. A job registered for the other
  runner cannot run there.
- `claim_next_task` ignores `runner_mode`, so any worker claims any task and the
  mismatch surfaces only at dispatch, as a failed task.
- The cluster-wide settings are already environment (`AAICLICK_REGISTRY`,
  `AAICLICK_K8S_*`); the runner is the one deployment property left on the job.
- Submission runs on the user's machine or the API server, so a runner snapshot
  taken at submit time captures the wrong environment anyway.

# User-Facing Model

A job is either a host subprocess or a container. For containers the user names
the image source — nothing else.

| You pass                       | Image source     | Where the task runs                                      |
|--------------------------------|------------------|----------------------------------------------------------|
| nothing                        | `NULL`           | Subprocess on the worker (unchanged default)             |
| `--image TAG` / `image=`       | `prebuilt`       | A container from `TAG`; no build task                    |
| `--build` / `build=True`       | `build`          | A container built from the repo at the submitted commit  |

`--git-remote`, `--git-sha`, `--git-branch` and `--dockerfile` are modifiers of
`--build`; without them the build coordinates auto-detect from the working tree
exactly as the docker runner does today (`docker_config.resolve_image_source`).
`image` and the build fields stay mutually exclusive
(`runner_config.validate_image_exclusivity`).

Whether that container is a Docker sibling container or a Kubernetes Pod is
decided once, by the worker's environment.

```bash
# Registration: the job's default image source
python -m aaiclick register-job myapp.pipelines.etl --name etl --build --dockerfile docker/etl.Dockerfile
python -m aaiclick register-job myapp.pipelines.report --name report --image ghcr.io/acme/report:1.4

# Per run: override or supply the source
python -m aaiclick run-job etl --git-sha 0123abcd...      # build modifier; inherits --build
python -m aaiclick run-job report --image ghcr.io/acme/report:1.5
python -m aaiclick run-job adhoc.task --build              # unregistered job, built from CWD repo
```

Python and REST mirror the CLI: `register_job(..., build=True)` or
`register_job(..., image="...")`; `run_job(..., build=True, git_sha=...)` or
`run_job(..., image="...")`.

**Resolution per run** (first match wins):

1. Run `image` given → `prebuilt`.
2. Run `build=True` or any build modifier given → `build` (modifiers fall through
   to the registration's `git_remote` / `dockerfile`, then auto-detect).
3. Registration `image` set → `prebuilt`.
4. Registration `build` set → `build`.
5. Otherwise → subprocess.

A run cannot turn a containerized registration back into a subprocess; nothing
asked for it.

!!! warning "Container jobs still need distributed mode"
    `run_job` keeps rejecting an `image_source` under chdb + SQLite
    (`is_local()`), with the existing message. The check moves from "runner is
    docker/kubernetes" to "the resolved image source is not `NULL`".

# Worker Configuration

| Variable                           | Values                     | Meaning                                                            |
|------------------------------------|----------------------------|--------------------------------------------------------------------|
| `AAICLICK_RUNNER`                  | `docker`, `kubernetes`     | How this worker launches container tasks. Unset → subprocess only  |
| `AAICLICK_REGISTRY`                | host                       | Unchanged                                                          |
| `AAICLICK_LOCAL_BUILD`             | any                        | Unchanged; invalid with `AAICLICK_RUNNER=kubernetes`               |
| `AAICLICK_K8S_NAMESPACE`           | namespace                  | Pod namespace, default `"default"`. Now the only source            |
| `AAICLICK_K8S_SERVICE_ACCOUNT`     | name                       | Pod service account. Now the only source                           |
| `AAICLICK_K8S_IMAGE_PULL_SECRET`   | name                       | Pod imagePullSecret. Now the only source                           |

No auto-detection: `aaiclick-kubectl` inherits the docker CLI from
`aaiclick-docker`, so "which CLI is installed" is ambiguous. The scaffolds set
the variable — `AAICLICK_RUNNER: docker` on the compose `worker` service,
`AAICLICK_RUNNER: kubernetes` in the helm worker Deployment.

**Startup validation** in `execution-worker start` (a new
`runner_env.validate_worker_runner()` called from the CLI entry):

- `AAICLICK_RUNNER` set to anything but `docker` / `kubernetes` → exit with
  the allowed values.
- `AAICLICK_RUNNER=kubernetes` with `AAICLICK_LOCAL_BUILD` → exit: the cluster
  cannot pull from the worker's daemon. This replaces the commit-point check in
  `image_injection.validate_image_sources`, which no longer has a runner to look
  at.

The per-job `namespace`, `service_account` and `image_pull_secret` go away. The
helm Role is namespaced to the worker's namespace, so a per-job namespace never
worked without widening RBAC by hand; the three fields were already documented
as cluster-wide defaults with env fallbacks.

# Pod Resources

`resources` (Kubernetes requests/limits) is the one Kubernetes setting that is
genuinely per job. It leaves the `kubernetes_config` blob and becomes a column of
its own:

- `RegisteredJob.resources: dict | None` (JSON) — default for every run.
- `Job.resources: dict | None` (JSON) — snapshot for this run (run kwarg →
  registration default → `None`).

Exposed on `RegisterJobRequest` / `RunJobRequest` and the Python API as
`resources`. The CLI does not grow a flag (it has none today). The docker runner
ignores it, as it does now; mapping it to `--cpus` / `--memory` is listed in
`docs/designs/future.md`.

# Data Model Changes

| Table             | Dropped                          | Added                              |
|-------------------|----------------------------------|------------------------------------|
| `registered_jobs` | `runner_mode`, `kubernetes_config` | `build` (bool, default false), `resources` (JSON, nullable) |
| `jobs`            | `runner_mode`, `runner`          | `resources` (JSON, nullable)       |
| `tasks`           | —                                | — (`image_source` already carries the per-task truth) |

`runner_config.py` loses `RunnerMode`, `RUNNER_*`, `IMAGE_RUNNERS`,
`SubprocessRunner` / `DockerRunner` / `KubernetesRunner`, `RunnerConfig`,
`parse_runner_config` / `dump_runner_config`, and `validate_runner_fields`. It
keeps the `ImageSource` models, `validate_image_exclusivity` and
`validate_task_entry`. `kubernetes_config.py` shrinks to the three `ENV_*` names
and a `resolve_pod_config()` that reads them (plus a `resources` argument) into
the existing `KubernetesConfig` NamedTuple. `docker_config.resolve_runner_config`
is deleted.

A worker-side `WorkerRunner = Literal["docker", "kubernetes"]` with
`RUNNER_DOCKER` / `RUNNER_KUBERNETES` constants lives in
`execution/runner_env.py` next to the other env readers, with
`get_worker_runner() -> WorkerRunner | None`.

Migration: one autogenerated revision via the `generate-migration` skill. Existing
rows: `registered_jobs.build` backfills to `true` where the old `runner_mode`
was `docker` / `kubernetes` and `image IS NULL`; `resources` backfills from
`kubernetes_config->'resources'` on both tables. Those two `UPDATE`s are the only
hand-added statements, placed in the generated `upgrade()` before the column
drops.

# Submission

`registered_jobs.run_job` and `register_job` lose `runner_mode`, `namespace`,
`service_account`, `image_pull_secret` and `kubernetes_config`; they gain `build`
and `resources`. Field validation becomes:

- `validate_image_exclusivity(image, git_remote, git_sha, git_branch, dockerfile)`
  — unchanged.
- A new `resolve_image_source(registered, *, image, build, git_*, dockerfile)
  -> ImageSourceT | None` implementing the resolution table above, replacing the
  current one that assumes a container runner. `None` means subprocess.
- `create_built_job` becomes `create_container_job(*, image_source, resources, ...)`
  with no `runner` argument; `new_job_row` loses `runner_mode` / `runner` and
  gains `resources`.

`image_injection.inject_build_tasks` drops its `job.runner_mode in IMAGE_RUNNERS`
guard and injects for any task whose source is an `ImageBuild`.
`validate_image_sources` reduces to parsing each declared source (shape check);
the registry requirement for Kubernetes builds is enforced at worker startup.
`orch_context.commit_tasks` no longer needs the `Job` row for validation, only
for injection.

# Dispatch

`execution/dispatch._resolve_dispatch` picks the runner from the task and the
worker, never from the job:

```python
if task.image_source is None:
    return _subprocess_dispatch(task)
runner = get_worker_runner()
if runner is None:
    raise DispatchError(
        f"task {task.name!r} declares an image_source but this worker has no "
        "AAICLICK_RUNNER; set it to docker or kubernetes on the worker"
    )
source = parse_image_source(task.image_source)
job = ...  # still fetched, for resources
return JobDispatch(runner, resolve_pod_config(resources=job.resources) if runner == RUNNER_KUBERNETES else None, ...)
```

`DispatchError` needs no new handling: the worker loop already catches
exceptions from `execute_fn` and fails the task with the message (the path a
missing image tag takes today). `JobDispatch.runner_mode: WorkerRunner`;
`JobDispatch.kubernetes_config` becomes `pod_config: KubernetesConfig | None`
and `kubernetes_worker._pod_spec_from` reads it directly instead of a dict. The
`_IMAGE_RUNNERS` registry and `build_shell_spec` are unchanged apart from the
type.

# Surfaces

| Surface   | Change                                                                                          |
|-----------|-------------------------------------------------------------------------------------------------|
| CLI       | `register-job`: drop `--runner`, `--namespace`, `--k8s-service-account`, `--k8s-image-pull-secret`; add `--build`. `run-job`: add `--build`; drop the three k8s flags. `execution-worker start`: validate `AAICLICK_RUNNER` |
| REST/MCP  | `RegisterJobRequest`: drop `runner_mode`, `kubernetes_config`; add `build`, `resources`. `RunJobRequest`: drop `namespace`, `service_account`, `image_pull_secret`; add `build`, `resources` |
| Frontend  | `RegisterForm.tsx` sends `build` instead of `runner_mode`; `schema.ts` regenerated                |
| Scaffolds | Compose `worker`: `AAICLICK_RUNNER: docker`. Helm `worker.yaml`: `AAICLICK_RUNNER: kubernetes`    |
| Images    | Unchanged: `aaiclick-docker` and `aaiclick-kubectl` stay the two worker variants                  |

# Testing

Existing tests parametrized over `runner_mode` collapse to the two worker modes
driven by `monkeypatch.setenv("AAICLICK_RUNNER", ...)`:

- `test_dispatch.py`: NULL source → subprocess regardless of env; declared
  source + unset env → `DispatchError`; declared source + each runner → routed
  handler; kubernetes `pod_config` carries env namespace and job `resources`.
- `test_registered_jobs.py` / `test_kubernetes_submission.py`: the resolution
  table (five rows, parametrized); `image` + `build` rejected; local mode
  rejects a resolved source; `resources` precedence.
- `test_image_injection.py`: injection keyed on `ImageBuild` alone.
- `test_runner_env.py`: `validate_worker_runner` on bad value and on
  `kubernetes` + `AAICLICK_LOCAL_BUILD`.
- `test_cli.py`: `--build` with `--image` rejected; `--runner` gone.
- E2E (`test_e2e/docker`, `test_e2e/kubernetes`, `test_e2e/compose`,
  `_helm-e2e-reusable.yaml`): replace `--runner X` with `--build` (or drop it
  for the subprocess smoke) and set `AAICLICK_RUNNER` on the worker, which the
  scaffolds now do.

Deleted as redundant: `test_runner_config.py` cases for the runner union,
`test_docker_config.py::test_resolve_runner_config*`, `test_kubernetes_config.py`
cases for per-job precedence.

# Documentation

Update `docs/designs/orchestration.md` ("Deployment Modes", "Runners & Entry
Types", "Image source"), `docs/designs/kubernetes_runner.md` ("Configuration",
"Selection and dispatch"), `docs/user_guide/orchestration.md` (runner defaults
paragraph), `docs/user_guide/container_images.md` (worker sections name
`AAICLICK_RUNNER`) and `docs/user_guide/deployment.md`. Add the docker
`resources` mapping to `docs/designs/future.md`. Delete this spec once the
feature lands.
