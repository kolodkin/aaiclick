Per-Task Image Requirement
---

Move the container image from a job-level setting to a **task-level
requirement** with a job-level fallback. A task's image is a property of the
unit of work (what code/environment it needs), not of the job that groups the
work — today's per-job image is a simplification that makes "task_B and
task_C need a different image than the entry task" inexpressible.

Status: **design — not implemented**. Builds directly on the injected
image-build task design (`docs/designs/orchestration.md`, "Image source");
the gating and dedup machinery there needs no changes.

# Model

Add one nullable JSON column to `tasks`:

| Column         | Type          | Meaning                                                       |
|----------------|---------------|---------------------------------------------------------------|
| `image_source` | `JSON`, null  | Serialized `ImageSource` (`build` / `prebuilt`); `NULL` = inherit the job's image |

- The job's `runner` config keeps its image as the **default** — the common
  case ("one codebase at one SHA") still declares the image once, per job.
- `runner_mode` (subprocess / docker / kubernetes) **stays per job**: where
  containers run is a deployment concern; which image a task runs in is a
  code concern. A per-task `image_source` is only valid on docker/kubernetes
  jobs (validated at task creation).
- Migration: single nullable column via the `generate-migration` skill.

# Build task per distinct image

Today `create_built_job` injects exactly one build task wired to the entry
task. Generalized rule, applied **wherever tasks are committed** (submission
in `create_built_job`, dynamic commits in `commit_tasks`):

1. Collect the distinct `image_key`s of the committed tasks' *effective*
   image sources (task override, else job default), `build` sources only —
   `prebuilt` needs no build task.
2. For each key with no build task in this job yet, inject one build task.
3. Wire each committed task to its image's build task
   (`task.depends_on(build_task)`).

Consequences:

- Distinct images build **in parallel** — independent root build tasks.
- Each task subtree unlocks as *its* image becomes `READY`, independently of
  the other images.
- Dynamic tasks whose image differs from their parent's are gated correctly:
  the dynamic commit injects/wires a build task for the new image. (Today
  dynamic tasks need no edge only because they always share the parent's
  image.)

The build task's kwargs change from `{"job_id": ...}` to carrying the image
source itself (`{"image": {...}}`): with several images per job,
`build_job_image` can no longer read "the" image off the job row.
Cross-job/cross-worker dedup is unchanged — `ensure_image` and the
`build_tasks` table are already keyed by image identity, not by job.

# Dispatch and launch

- `dispatch._resolve_dispatch` resolves the task's effective source (task
  override, else job default) into `JobDispatch.image_source` / `image_tag`.
- `resolve_image_tag` at container launch is untouched — it already takes the
  source per call and serves as the `READY`-row fast path / inline fallback.
- The build task keeps its host-runner pin.

# API surface

Python-first — tasks are defined in code:

```python
@task(image=ImagePrebuilt(image_tag="python:3.12"))
async def side_tool(...): ...

create_task("mod.fn", image_source=ImageBuild(git_remote=..., git_sha=...))
```

- `create_task` / `@task` accept the override; `run_job` / `register-job`
  keep declaring the job default only.
- Validation at creation: override present ⇒ job is docker/kubernetes mode.
- REST/UI: task detail shows the effective image; no write surface needed
  beyond job submission (tasks come from code).

# Out of scope

- Per-task `runner_mode` (mixing subprocess and docker tasks in one job
  beyond the existing host-pinned build task).
- Per-task kubernetes cluster config — stays on the job.

# Open questions

- Full per-task runner override vs. image-only (this spec: image-only).
- Whether the entry task itself may override the job image, or the job
  default exists precisely to serve the entry task.
