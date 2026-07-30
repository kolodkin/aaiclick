Per-Task Image Requirement
---

Make the container image a **task requirement**, replacing the job-level
("entrypoint") image concept altogether. A task declares the image it needs;
jobs keep only `runner_mode` (where containers run) and kubernetes cluster
config. This supersedes the injected job-level build task *and* the
`build_tasks` claim/lease machinery: the build becomes **just another task
running `docker build`**, and coordination reduces to graph dependencies plus
registry pull-first.

Status: **design — not implemented**.

# Model

| Piece                  | Today                              | Target                                            |
|------------------------|------------------------------------|---------------------------------------------------|
| Image declaration      | `Job.runner.image` (one per job)   | `tasks.image_source` (JSON) on every container task |
| Inheritance            | job → all tasks                    | parent task → dynamic children (submission sugar stamps the entry task) |
| Build coordination     | `build_tasks` row + lease + poll   | build task in the graph (registry mode) / inline per-host build (no registry) |
| Build↔task link        | `tasks.build_task_id` FK           | the dependency edge itself                        |

- `run_job(image=... / git_*=...)` stays as API sugar: it stamps the entry
  task's `image_source`. Dynamic children inherit their parent's image unless
  they declare their own.
- `runner_mode` stays per job; a task-level `image_source` is only valid on
  docker/kubernetes jobs.

# Registry mode (`AAICLICK_REGISTRY` set)

Submission and every dynamic `commit_tasks`:

1. Group the committed tasks by effective `image_key` (`build` sources only —
   `prebuilt` needs nothing).
2. For each key without a build task in this job yet, inject one — a plain
   module task, host-runner pinned, `max_retries` for transient failures,
   image source in its kwargs.
3. **The wiring challenge**: every committed task depends on its image's
   build task (`task.depends_on(build_task)`). This is the load-bearing step —
   the scheduler's existing dependency filter then guarantees no task is
   claimed before its image is pushed.

The build task body is pull-first: `docker pull` from the registry (someone
already pushed this SHA → done), else clone + `docker build` + `docker push`.
Cross-job dedup is the registry itself — two jobs racing on the same SHA may
double-build in the worst case, which is wasteful but correct (identical
images by construction, last push wins). That accepted cost is what buys
deleting the lease/fencing/poll machinery.

Distinct images are independent roots: they build in parallel, and each task
subtree unlocks when *its* image is pushed.

# No registry: always inline

Without a registry the built image lives only in one host's Docker daemon —
a global "ready" marker would be a lie, and a build task's output could not
reach other hosts anyway. So no build task is injected; each dispatching
worker builds inline at launch: local `docker image inspect`, else clone +
`docker build`. Per-host daemon cache dedups repeats; holding the slot during
a cold build is accepted (no-registry is de facto single-host / small-scale
mode). Kubernetes `build` sources already require a registry, so inline mode
applies to the docker runner only.

Note: registry presence is worker-side env, but injection happens at
submission — the API server and workers must agree on `AAICLICK_REGISTRY`
(same env layer, as today for k8s builds).

# What this retires

- `build_tasks` table, `BUILD_*` statuses, `ensure_image` claim/lease/poll,
  `tasks.build_task_id` (migration drops both columns/table).
- `build_job_image` + job-level build-task injection in `create_built_job`
  (replaced by per-image injection at commit points).
- `resolve_image_tag`'s dispatch-time polling — launch either pulls (registry
  mode, image guaranteed pushed by the dependency) or builds inline (no
  registry).

Crash recovery moves from lease reclaim to ordinary task retries: a worker
dying mid-build fails/orphans the build task, and the normal retry/reaper
path re-runs it — pull-first makes the retry cheap if the push happened.

# Open questions

- Exactly-once building across concurrent same-image jobs is given up for
  simplicity — acceptable, or keep a lightweight advisory lock per image_key?
- Should `prebuilt` per-task overrides be allowed on subprocess jobs (no) —
  confirm validation lives in `create_task`.
