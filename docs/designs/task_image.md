Per-Task Image Requirement
---

Make the container image a **task requirement**, replacing the job-level
("entrypoint") image concept. A task declares the image it needs; jobs keep
only `runner_mode` (where containers run) and kubernetes cluster config. This
supersedes the `build_tasks` claim/lease machinery: the build becomes **just
another task running `docker build`**, and coordination reduces to graph
dependencies plus registry pull-first.

Status: design — not implemented.

# Model

| Piece               | Today                                       | Target                                                     |
|---------------------|---------------------------------------------|------------------------------------------------------------|
| Image declaration   | `Job.runner` JSON (`image` nested per job)  | `tasks.image_source` (JSON) on every container task        |
| Inheritance         | job → all tasks                             | stamped at commit: parent task → dynamic children          |
| Build coordination  | `build_tasks` row + lease + poll            | build task in the graph (registry) / inline build (none)   |
| Build↔task link     | `tasks.build_task_id` FK                    | the dependency edge itself                                 |

- `tasks.image_source` is a nullable JSON column holding the existing
  `ImageSource` discriminated union (`ImageBuild` / `ImagePrebuilt`) from
  `aaiclick/orchestration/runner_config.py`, unchanged.
- **`NULL` means "runs as a host subprocess"**, even in a docker/kubernetes
  job. The job's `runner_mode` is the vehicle for tasks that *have* an image.
  This pins the build task to the host (where the docker CLI and daemon
  socket live) with no dedicated flag, and lets users mix cheap host-side
  tasks into container jobs.
- `Job.runner` / `RegisteredJob.runner` shrink: `image` is removed from
  `DockerRunner` and `KubernetesRunner`; `KubernetesRunner` keeps
  `namespace` / `service_account` / `image_pull_secret` / `resources`.
  `DockerRunner` becomes an empty marker, preserving the discriminated union
  so `parse_runner_config` call sites are untouched.

# The build is an ordinary task

The mental model is plain graph wiring — `image_build_task >> entrypoint_task`
(job-level image) or `image_build_task >> taskX` (task-level image). The build
is a normal `tasks` row: normal statuses, normal `max_retries`, normal
retry/reaper crash recovery. No dedicated table, no lease, no scheduler
special-casing — the existing dependency filter is the whole coordination
story. Exactly three deviations from "just another task", each with a
concrete reason:

1. **Auto-injected, not user-written.** Users declare *what image a task
   needs* (`image_source`); the commit path expands that into the build task
   plus dependency edges, deduping so N tasks on the same image share one
   build task. Injection is only sugar that writes the `build >> dependents`
   edges you would otherwise wire by hand.
2. **Runs on the host.** Chicken-and-egg: the build task cannot run inside
   the image it is building, and it needs the docker CLI + daemon socket. Not
   a special flag — `image_source=NULL` ⇒ host subprocess is a rule for any
   task; the build task just uses it.
3. **No-registry mode gets no build task at all.** A task's `SUCCESS` is a
   global fact in the DB, but without a registry the built image exists only
   in one host's Docker daemon — a completed build task would tell workers on
   other hosts the image is ready, which is a lie. With a registry,
   `docker push` makes success a global fact again, which is why the
   graph-task model works there. See "No registry: always inline".

Build tasks (and their edges) stay identifiable for the UI without schema:
the build entrypoint is a fixed module path, so view models expose a computed
`is_image_build` flag on the task view — the UI can style build nodes and the
edges leaving them. No `dependencies.kind` column; promote to a `tasks.kind`
column only if more system-injected task kinds appear.

# Inheritance — stamped at commit, never resolved at dispatch

By the time a task row is committed, its `image_source` is final, so `NULL`
is unambiguous at dispatch.

- `run_job(image=... / git_*=...)` stays as API sugar: it resolves the image
  source exactly as today (`docker_config._resolve_image_source`, including
  git auto-detect) and stamps it on the **entry task**. The job row no longer
  carries it.
- `commit_tasks` stamps the **current task's** `image_source` (from the task
  execution context) onto any committed task that didn't declare its own —
  dynamic children inherit their parent. A child of a `NULL`-image task is
  `NULL` (subprocess) unless it declares an image explicitly.
- `create_task` grows optional image kwargs mirroring the `run_job` sugar
  (`image=` for prebuilt; `git_remote=` / `git_sha=` / `git_branch=` /
  `dockerfile=` for build) so a task can declare its own image.

# Registry mode (`AAICLICK_REGISTRY` set)

At each commit point (`run_job` entry-task submission and every dynamic
`commit_tasks`), when the job is docker/kubernetes:

1. Group the committed tasks by effective `image_key`
   (`docker_config.image_key`; `build` sources only — `prebuilt` needs
   nothing).
2. For each key without a build task in this job yet, inject one — an
   ordinary module task with `image_source=NULL` (⇒ host subprocess),
   `max_retries=2` for transient failures, kwargs carrying the dumped
   `ImageBuild` plus its `image_key`. Lookup is `WHERE job_id = ? AND
   entrypoint = <build entrypoint>`, matched on `image_key` in Python — a
   handful of rows per job, so no JSON querying or extra column.
3. Wire `task.depends_on(build_task)` for every committed task in the group.
   The scheduler's existing dependency filter then guarantees no task is
   claimed before its image is pushed.

The build task body is pull-first: `docker pull` from the registry (someone
already pushed this SHA → done), else clone + `docker build` + `docker push`.

Distinct images are independent roots: they build in parallel, and each task
subtree unlocks when *its* image is pushed.

Crash recovery is the ordinary task retry/reaper path: a worker dying
mid-build fails/orphans the build task and the normal retry re-runs it —
pull-first makes the retry cheap if the push happened.

**Races, accepted.** Two jobs (or two concurrent `commit_tasks` in one job)
racing on the same SHA may double-build — wasteful but correct (identical
images by construction, last push wins). No advisory lock per `image_key`:
the payoff of this design is deleting coordination machinery, and a lock
re-imports the failure modes (stale holders, lease expiry) being deleted.
Revisit only if double-builds show up as a real cost.

# No registry: always inline

No build task is injected without a registry (deviation 3 above — build
success is host-local, not a global fact). The docker launch path does
`docker image inspect` → hit ⇒ run; miss ⇒ clone + `docker build` inline,
holding the slot for the cold build (accepted — no-registry is de facto
single-host / small-scale mode). Kubernetes `build` sources already require
a registry, so inline mode applies to the docker runner only.

!!! warning "Submission and workers must agree on `AAICLICK_REGISTRY`"
    Registry presence is worker-side env, but build-task injection happens at
    commit time — the API server and workers must share the same
    `AAICLICK_REGISTRY` setting (same env layer, as today for k8s builds).
    Dynamic `commit_tasks` runs *inside* containers, so `AAICLICK_REGISTRY`
    joins `ALWAYS_PASSED_ENV_VARS` (`execution/runner_env.py`) — otherwise a
    dynamic child declaring a new build image would silently skip injection
    and later fail pulling an unpushed tag.

# Dispatch

`dispatch._resolve_dispatch` reads `task.image_source` instead of the job's
runner image:

| `task.image_source` | Vehicle                                                            |
|---------------------|--------------------------------------------------------------------|
| `NULL`              | mp child on the host worker, regardless of job `runner_mode`       |
| `ImagePrebuilt`     | container/pod with the tag verbatim                                |
| `ImageBuild`        | registry tag (registry mode — pushed by the dependency) or inline build (no registry) |

`resolve_image_tag`'s dispatch-time polling and `build_task_id` stamping
disappear. Kubernetes cluster config still comes from `job.runner`.

# Validation

Lives at commit points (`commit_tasks` / entry-task stamping), not
`create_task` — `create_task` has no job context:

- non-`NULL` `image_source` on a subprocess job → rejected (including
  `prebuilt` per-task overrides).
- `ImageBuild` on kubernetes without a registry → rejected at submission, as
  today.

# What this retires

- `build_tasks` table, `BUILD_*` statuses, `ensure_image` claim/lease/poll
  (`execution/image_builder.py`), `tasks.build_task_id` (one migration adds
  `tasks.image_source`, drops the column and the table). No data backfill —
  in-flight container jobs across this upgrade are not preserved.
- Build-task-specific view models and SPA views — a build is now an ordinary
  task in the graph, visible in the normal task UI.
- The "Non-Blocking Image-Build Wait (Release-and-Requeue)" item in
  `docs/designs/future.md`: dependency gating replaces polling in registry
  mode; shrink it to the no-registry inline-build case or delete it.

# Testing

- Unit: stamping/inheritance in `commit_tasks`, per-job injection dedup,
  validation rejections, build-task body pull-first branching (mocked CLI).
- Integration: docker/k8s runner tests rewired — registry mode asserts the
  dependency edge exists and gates claiming; no-registry asserts the inline
  build path.
- Deleted with the machinery: `test_image_builder.py`,
  `test_build_task_model.py` lease/claim coverage.
