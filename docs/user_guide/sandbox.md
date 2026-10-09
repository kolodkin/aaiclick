Sandbox
---

Any signed-in user pastes one Python file of `@task` / `@job` definitions,
names it, and every job in it runs once. Each submission is a commit in a git
repo you own.

# Enabling

Set `AAICLICK_SANDBOX` on the API server to a git remote URL:

```bash
AAICLICK_SANDBOX=https://token@github.com/acme/aaiclick-sandbox.git
```

- The server clones the remote under `<AAICLICK_LOCAL_ROOT>/sandbox/repo` and
  pushes one commit per submission. An empty repo works.
- Only the server reads the variable: the workers get the remote and commit
  from the submission, as for any git-build job.
- Sandbox jobs are container jobs: distributed backends, a worker with
  `AAICLICK_RUNNER`, and `AAICLICK_REGISTRY` or `AAICLICK_LOCAL_BUILD` — see
  [Container Images](container_images.md). Execution workers must be able to
  clone the remote.

The `@sandbox` command appears on the home page once enabled.

# Writing a file

```python
from aaiclick.orchestration import TaskResult, job, task


@task
async def hello():
    return "hello"


@job
def hello_job():
    return TaskResult(tasks=[hello()])
```

- Every top-level `@job` function runs once, with no arguments; a file
  without one is rejected.
- Tasks may declare their own `image=` / `git_*` via `create_task`; the job
  itself is always built from the sandbox repo.
- The server parses the file and never imports it.

# What happens

1. The file is committed as `YYYYMMDD/sb_<unix ts>_<name>.py`; the row shows
   `pending` with the short SHA.
2. The background worker submits one job per `@job`, named
   `sb_<ts>_<name>.<function>` and pinned to that commit. The row turns
   `submitted` with links to the jobs, or `failed` with the message.
3. The build task clones the commit onto the default Dockerfile (Container
   Images), so the file imports as `YYYYMMDD.sb_<ts>_<name>`.

Click a submission's name to read its source.

# Limits

- The base image ships only `aaiclick[all]`; other imports fail in the
  container. Check a `Dockerfile` into the sandbox repo for dependencies
  (manifests are planned: `docs/designs/future.md`).
- A submission runs once; submit again to re-run.

**Implementation**: `aaiclick/sandbox/repo.py` — see `SandboxRepo`;
`aaiclick/sandbox/parse.py` — see `find_job_functions`;
`aaiclick/internal_api/sandbox.py` — see `submit_sandbox_file`;
`aaiclick/orchestration/background/background_worker.py` — see
`BackgroundWorker._run_sandbox_files`.
