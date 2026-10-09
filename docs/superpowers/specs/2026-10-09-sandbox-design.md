Sandbox Design
---

A `/sandbox` page where any signed-in user writes one Python file of `@task`
and `@job` definitions, submits it under a name, and the platform commits the
file to a git repo and runs every job in it once on the docker runner. Git is
the durable, reviewable record of every submission; a `sandbox_files` table
records what was submitted and which jobs ran.

# Goals

- Try job code in the browser without a deploy, a registration, or a Dockerfile.
- Every submission is a commit in the sandbox repo, readable by anyone with
  repo access, independent of the aaiclick database.
- Reuse the existing run path end to end: `run_job` with a git-build image
  source, the injected build task, the default Dockerfile fallback.

Out of scope for v1: Java files (the Java SDK defines tasks only, inside a
user-built image), job kwargs, re-running a submission, extra Python packages
(see `docs/designs/future.md` — Default Build Image, Dependency Manifests).

# Configuration

| Variable           | Where      | Meaning                                                   |
|--------------------|------------|-----------------------------------------------------------|
| `AAICLICK_SANDBOX` | API server | Git remote URL of the sandbox repo. Unset: no sandbox.    |

Only the API server reads it. The background worker and the execution workers
learn the remote from data they already receive: the `sandbox_files` row and the
task's `image_source`. The execution worker that runs the build task must be
able to clone the remote, exactly as for any git-build job (credentials in the
URL or in the host's git configuration).

Container tasks need the distributed backends; `run_job` rejects a build source
under `is_local()`, so the sandbox is a distributed-mode feature. The worker
fleet needs `AAICLICK_RUNNER=docker` or `kubernetes` plus `AAICLICK_REGISTRY` or
`AAICLICK_LOCAL_BUILD`, as for every build job.

# Flow

1. **Submit** — `POST /api/v0/sandbox` with `{name, source}`. The server
   validates the name, parses the source with `ast` to list top-level
   `@job` functions, writes `YYYYMMDD/sb_<unix ts>_<name>.py` into its clone
   of the sandbox repo, commits, pushes, and inserts a `sandbox_files` row
   with status `"pending"`.
2. **Run** — the background worker, on each poll, picks pending rows oldest
   first and calls `run_job` once per job function. The call carries
   everything: `name`, `entrypoint`, `git_remote`, `git_sha`,
   `run_type="SANDBOX"`. The existing machinery stamps the build source on
   the entry task, injects the build task, clones the commit and builds with
   the default Dockerfile. The row moves to `"submitted"` with the job ids, or
   `"failed"` with the error.
3. **View** — `/sandbox` lists submissions with status and job links;
   `/sandbox/<id>` shows the source at its commit.

Jobs defined in the file may return tasks that declare their own `image=` or
`git_*` coordinates through the existing per-task declaration. The sandbox
itself never takes an image: the entry task is always built from the sandbox
repo at the submission commit.

# Data Model

`sandbox_files` (SQLModel in `aaiclick/orchestration/models.py`, Alembic
revision via the `generate-migration` skill):

| Column         | Type                  | Notes                                               |
|----------------|-----------------------|-----------------------------------------------------|
| `id`           | BigInteger PK         | snowflake                                           |
| `name`         | String                | user-given, `[A-Za-z][A-Za-z0-9_]*`, max 64         |
| `path`         | String, unique        | `YYYYMMDD/sb_<ts>_<name>.py`, relative to repo root |
| `git_remote`   | String                | the remote at submission time                       |
| `git_sha`      | String                | the submission commit                               |
| `job_names`    | JSON (list of str)    | `@job` functions found by `ast`, source order       |
| `job_ids`      | JSON (list of int), nullable | Job ids created by the worker, same order    |
| `status`       | String (`SandboxStatus`) | `"pending"`, `"submitted"`, `"failed"`           |
| `error`        | String, nullable      | `run_job` failure text                              |
| `submitted_by` | BigInteger FK `users.id` | the submitting user                              |
| `created_at`   | datetime (utc_field)  |                                                     |
| `updated_at`   | datetime (utc_field)  |                                                     |

`SandboxStatus = Literal["pending", "submitted", "failed"]` with module
constants, per the Literal convention. No CHECK constraint.

`RunType` gains `"SANDBOX"` (`RUN_SANDBOX`). Jobs carry no sandbox column; the
link is `sandbox_files.job_ids` → `jobs.id`. A sandbox Job is named
`sb_<ts>_<name>.<func>` and has `registered_job_id=None`.

# Git Repository

The sandbox repo holds only submission files. No Dockerfile, no package files:
the build task's default Dockerfile (a thin layer on the aaiclick base image,
`WORKDIR /src`) makes `YYYYMMDD.sb_<ts>_<name>` importable. A digits-only
directory name is a valid importlib namespace package.

Layout:

```text
20261009/sb_1760000000_hello.py
20261009/sb_1760000420_join_demo.py
20261010/sb_1760086400_hello.py
```

**Server-side clone** — `aaiclick/sandbox/repo.py`, class `SandboxRepo`:

- Persistent clone at `<AAICLICK_LOCAL_ROOT>/sandbox/repo`, created on first
  use with `git clone --depth=1 -- <remote>`.
- Each submission, under one process-wide `asyncio.Lock`: `fetch origin`,
  `reset --hard origin/<default branch>`, write the file, `add`, `commit`
  with the submitting user as author (`name <email>`, falling back to
  `<username>@sandbox` when no email) and `aaiclick <sandbox@aaiclick>` as
  committer, `push origin HEAD:<default branch>`.
- A rejected push (`non-fast-forward`) is retried once after a fresh fetch and
  reset; a second rejection raises `SandboxPushRejected`.
- **Empty remote**: a remote with no commits has no default branch. The first
  submission commits on `main` and pushes with `-u`.
- Git subprocesses run through `aaiclick/orchestration/execution/cli.py` —
  see `run()`, like `docker_config._git`.
- `read_file(path, sha)` returns the source via `git show <sha>:<path>` for
  the detail endpoint.

The default branch is read once from `git symbolic-ref refs/remotes/origin/HEAD`
after the clone.

# Source Validation

`aaiclick/sandbox/parse.py` — `find_job_functions(source) -> list[str]`:

- `ast.parse`; a `SyntaxError` becomes a 422 with the line and message.
- Top-level `FunctionDef` / `AsyncFunctionDef` nodes with a decorator that is
  `job`, `job(...)`, `aaiclick.orchestration.job`, or the same under an alias
  of the imported name. The decorated function's name is the entrypoint
  attribute, regardless of the `@job("display name")` argument.
- Zero results is a 422: "no @job function found".

The source is never imported on the API server.

# API

`aaiclick/server/routers/sandbox.py`, included only when `AAICLICK_SANDBOX`
is set, behind `require_principal` like the other routers. Every role may
submit; viewers included — the sandbox is a sandbox.

| Method | Path                      | Body / query         | Returns                                   |
|--------|---------------------------|----------------------|-------------------------------------------|
| `GET`  | `/api/v0/sandbox/config`  |                      | `{enabled: bool, remote: str \| null}`    |
| `POST` | `/api/v0/sandbox`         | `{name, source}`     | `SandboxFileView`, 201                    |
| `GET`  | `/api/v0/sandbox`         | `limit`, `offset`    | `Page[SandboxFileView]`, newest first     |
| `GET`  | `/api/v0/sandbox/{id}`    |                      | `SandboxFileDetailView` (view + `source`) |

`remote` in `config` is returned to admins only; other roles get `null`. The
`config` route exists even when the sandbox is disabled, so the SPA can hide
the nav entry (`enabled=false`).

Errors: 422 for name, syntax and no-job failures; 409 when the push is rejected
twice; 502 for any other git failure, with the git stderr as `detail`. No row
is written unless the push succeeded.

`SandboxFileView` (in `aaiclick/orchestration/view_models.py`): `id`, `name`,
`path`, `git_sha`, `job_names`, `job_ids`, `status`, `error`, `submitted_by`
(username), `created_at`, `updated_at`.

# Background Worker

`BackgroundWorker._run_sandbox_files()` is a new step in `_do_cleanup()`,
after `_check_schedules()`:

1. Select `sandbox_files` with `status = "pending"`, oldest first, limit 10.
2. For each row, in a short `orch_context` (the same way `_check_schedules`
   submits scheduled runs), call for each `job_name`:

   ```python
   await run_job(
       f"{module_name}.{job_name}",            # sb_<ts>_<name>.<func>
       f"{date_dir}.{module_name}.{job_name}", # entrypoint
       git_remote=row.git_remote,
       git_sha=row.git_sha,
       run_type=RUN_SANDBOX,
   )
   ```

3. Set `job_ids` and `status="submitted"`. If any call raises, record the
   error, set `status="failed"`, and keep the job ids created so far.

Rows are processed one at a time; a row never runs twice because the status
flips in the same transaction that records the outcome, and the worker runs
as a single process.

# Frontend

Routes in `src/prompt.ts`: `{ kind: "sandbox" }` and
`{ kind: "sandbox-file"; id: string }`. Views in `src/views/Sandbox.tsx` and
`src/views/SandboxFile.tsx`, exported from `src/views/index.tsx`.

**`/sandbox`**:

- Name input, source textarea seeded with a minimal example (one `@task`, one
  `@job` returning `TaskResult`), Submit button. Disabled while a request is in
  flight; errors render inline from the problem `detail`.
- Submissions table: name, submitted by, time, status chip, short SHA, one link
  per job to `/job/<name>`. Re-fetches every 5 s while any row is `pending`,
  through the existing hooks in `src/api/hooks.ts`.

**`/sandbox/<id>`**: read-only source, the same job links, the error when
failed.

`Header.tsx` shows a Sandbox entry when `config.enabled` is true. The SPA fetches
`/sandbox/config` once at load, next to the existing auth bootstrap.

# Error Handling

| Failure                             | Surface                                          |
|-------------------------------------|--------------------------------------------------|
| Bad name / syntax / no job          | 422 before any git operation                     |
| Clone or fetch failure              | 502, nothing written                             |
| Push rejected twice                 | 409, local clone reset, nothing in the DB        |
| `run_job` raises                    | row `failed` with the message, visible in the UI |
| Build or task failure inside a job  | ordinary job failure on the Jobs page            |

# Testing

- `aaiclick/server/routers/test_sandbox.py` — bare repo in `tmp_path` as the
  remote. Submit a valid file: row fields, commit path, SHA equals the bare
  repo's head, job names. Parametrized 422s: bad name, syntax error, no job.
  Routes absent when the variable is unset. `config` hides `remote` from
  non-admins.
- `aaiclick/sandbox/test_repo.py` — empty remote gets an initial commit on
  `main`; a second submission sees the first; a push rejected by a competing
  commit succeeds on the retry; `read_file` returns the committed text.
- `aaiclick/sandbox/test_parse.py` — parametrized decorator forms and
  rejections.
- `aaiclick/orchestration/background/test_sandbox_runs.py` — a pending row
  yields one `run_job` call per job name with the expected entrypoint, remote,
  SHA and run type; a raising `run_job` marks the row failed.
- `test_e2e/docker/` — submit through the API, let the real worker build and
  run on the docker runner, assert the job completes and `job_ids` are set.
- `src/` — Vitest for the new route parsing in `prompt.test.ts`.

# Documentation

- `docs/user_guide/sandbox.md` — enabling, writing a file, what runs where.
- `docs/user_guide/deployment.md` — `AAICLICK_SANDBOX` in the env table.
- `docs/designs/orchestration.md` — one line under Image source naming the
  sandbox as a caller of `run_job` with `git_remote`/`git_sha`.
- This spec is deleted once the feature lands; the user guide is the record.
