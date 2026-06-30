# On-Demand BuildTask Design

## Problem

Docker/Kubernetes jobs need a container image before any container task can run.
Today that image is produced by a `docker_build` task **injected at job submission**
in `create_built_job()` (`aaiclick/orchestration/factories.py:311-321`), wired ahead
of the entry task via `entry_task.depends_on(build_task)`.

Two issues with submission-time injection:

- **It is eager and job-scoped.** Every git-build job materializes a build task in its
  DAG, even when the image already exists. Tasks created *dynamically* at runtime
  (TaskFlow `@task` returning a `Task`) are never covered by an injected build — they
  only work because they inherit the job's image after the entry task's build
  dependency already ran.
- **Dedup is cache-only.** "Build once" across jobs relies on the registry→local→build
  cache hierarchy in `docker_build.py`. Concurrent jobs on the same SHA, or a setup with
  no registry, can each rebuild.

## Goal

Make image builds **dynamic and on-demand**, not submission-based. The image is built
the first time *any* task (static or dynamic) actually needs it, **exactly once**, and
every later task reuses it. The build stays **visible and observable** — but via a
dedicated `BuildTask` entity with its own table and lifecycle, not the generic `Task`
DAG.

## Architecture

### BuildTask model

A first-class entity keyed by **image identity**, not by job:

```python
class BuildTask(SQLModel, table=True):
    __tablename__ = "build_tasks"
    id: int                       # snowflake
    image_key: str                # UNIQUE — sha256 of (git_remote, git_sha, dockerfile)
    image_tag: str                # resolved tag, e.g. "aaiclick-job:<sha>" / registry-prefixed
    git_remote: str               # build coords, denormalized so the build is self-contained
    git_sha: str
    dockerfile: str | None
    status: BuildStatus           # "PENDING" | "BUILDING" | "READY" | "FAILED"
    holder_worker_id: int | None  # lease holder
    lease_expires_at: datetime | None
    log_path: str | None          # clone + build + push output — the "visible" part
    error: str | None
    attempts: int
    max_retries: int              # default 2 (matches today's build task)
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
```

The `UNIQUE(image_key)` constraint is the dedup primitive. `ImagePrebuilt` images never
create a `BuildTask` — there is nothing to build, only verify/pull.

`BuildStatus` is a `Literal["PENDING", "BUILDING", "READY", "FAILED"]` with module-level
constants, per the project's Literal-over-Enum convention. No DB CHECK constraint.

### Lifecycle

```
(absent) ──INSERT──▶ PENDING ──▶ BUILDING ──▶ READY
                                     └──────────▶ FAILED ──(reclaim if attempts < max_retries)
```

The row is created **at claim time, before the build starts** (not on completion). This
is what makes an in-flight build count as "already in place": concurrent tasks see the
`PENDING`/`BUILDING` row and back off instead of starting a second build.

### Dispatch seam: `ensure_image()`

Build moves from *submission* to *dispatch*. In `docker_worker` / `kubernetes_worker`,
before `docker run` / pod-create:

```text
ensure_image(runner, worker_id):
  if runner.image is ImagePrebuilt: return verify/pull(tag)
  key = image_key(runner.image)
  loop:
    row = SELECT * FROM build_tasks WHERE image_key = key
    if row and row.status == READY:   return row.image_tag
    if row and row.status == FAILED and row.attempts >= row.max_retries:
        raise BuildFailed(row)
    won = atomic_claim(key, worker_id)      # see below
    if won:
        try:    build_image(...)            # reused clone -> docker build -> push
                mark READY; return tag
        except: mark FAILED + error; continue
    else:
        sleep(backoff); continue            # poll existing BuildTask until READY/FAILED
```

The build body reuses `docker_build.build_image`'s clone → `docker build` → push and its
registry→local→build cache hierarchy, invoked from the "won the claim" branch instead of
as a `Task` entrypoint. It runs on the dispatching host, which already needs Docker to
launch the container — the same place builds run today.

`ensure_image()` blocks the dispatching worker while building. This is the same
occupancy cost as today's build task holding a worker.

### Deduplication guarantee

Two independent layers, both enforced by the database:

1. **`UNIQUE(image_key)`** — at most one `BuildTask` row per image. A duplicate `INSERT`
   raises a constraint violation, so a job (one image_key) can never have two build
   records, even under a full race. Stronger than per-job: dedup is shared across jobs on
   the same SHA.
2. **Atomic claim** — the database, not application timing, picks the one live builder:

   ```sql
   INSERT INTO build_tasks (image_key, status, holder_worker_id, lease_expires_at, ...)
   VALUES (:key, 'BUILDING', :worker, now() + :lease, ...)
   ON CONFLICT (image_key) DO NOTHING
   RETURNING id;
   ```

   Row returned → you won, you build. Nothing returned → the row already exists →
   `SELECT` it and either return its tag (`READY`) or wait (`PENDING`/`BUILDING`). You
   never build a duplicate. SQLite (subprocess mode) uses the same `ON CONFLICT DO
   NOTHING` shape; its serialized session closes the window anyway.

### Crashed-builder reclaim

A winner that dies mid-build leaves the row in `BUILDING`. The **lease** resolves it
without ever permitting a concurrent duplicate — reclaim is a single atomic, row-locked
conditional update:

```sql
UPDATE build_tasks
SET holder_worker_id = :worker, lease_expires_at = now() + :lease, attempts = attempts + 1
WHERE image_key = :key
  AND (lease_expires_at < now() OR (status = 'FAILED' AND attempts < max_retries))
RETURNING id;
```

Only one worker's `UPDATE` can match-and-win (the row lock serializes them); everyone
else affects zero rows and keeps waiting. Lease expiry is a reclaim, not a build failure.

## Submission-time injection removal

`create_built_job()` stops injecting the `docker_build` task and stops adding
`entry_task.depends_on(build_task)`. A docker/k8s job becomes `Job` + `entry_task`,
identical in shape to a subprocess job. Ordering is preserved: the entry task waits
*inside dispatch* via `ensure_image()` blocking until `READY`, instead of via a DAG
dependency edge.

## Job ↔ build linkage (visibility)

With the build decoupled from the DAG, visibility comes from the link:

- Each container task that calls `ensure_image()` stamps `build_task_id` on itself (the
  `BuildTask` it waited on). Creating-vs-attaching is decided purely by who won the atomic
  `INSERT` — never by a separate "does a build exist?" check that could race.
- A job view joins `tasks → build_tasks` (or `job → build_tasks` by `image_key`) to
  surface "this job's image was built by BuildTask N — status, log_path, timings."
- `BuildTask.log_path` holds the clone+build+push output, so build logs stay first-class
  and queryable — the visibility requirement served by the dedicated table rather than a
  `Task` row.

## Failure & retry semantics

- `BuildTask` owns its retries (`attempts` / `max_retries`, default 2).
- Build fails → row goes `FAILED` + `error`. The next dispatcher reclaims if
  `attempts < max_retries`; otherwise `ensure_image()` raises `BuildFailed` and the
  dispatching task fails with a clear "image build failed (BuildTask N)" error.
- A task that failed because its build failed is retryable per its own `max_retries`; on
  retry it re-enters `ensure_image()` and either rides a now-`READY` image or a fresh
  reclaim.

## Reused vs deleted

**Reused**

- `docker_build.build_image`'s clone → `docker build` → push body and its
  registry→local→build cache hierarchy — moved into `ensure_image()`'s won-the-claim
  branch.
- `effective_image_tag` / image-tag derivation; a new `image_key()` hash over
  `(git_remote, git_sha, dockerfile)`.

**Deleted**

- `BUILD_TASK_ENTRYPOINT` as a `Task` entrypoint.
- The dispatch special-case `if task.entrypoint == BUILD_TASK_ENTRYPOINT: return
  subprocess` (`aaiclick/orchestration/execution/dispatch.py:47-48`).
- The injection block in `create_built_job` (`aaiclick/orchestration/factories.py:311-321`).

**Migration**

- New `build_tasks` table, generated via the `generate-migration` skill (never
  hand-written).

## Open questions

None blocking. Lease duration and poll backoff values are implementation tuning, set
during the build phase.
