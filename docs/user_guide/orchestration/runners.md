Runners
---

# Runners

Two independent dials control how a task runs:

- **Runner mode** (per *job*) — where the execution environment lives.
- **Entry type** (per *task*) — what runs inside it: `"module"` (import and
  run a Python entrypoint, the default) or `"shell"` (run a literal argv).

## Runner modes

| Mode                     | Where the task runs                      | Worker host requirements                               |
|--------------------------|------------------------------------------|--------------------------------------------------------|
| **subprocess** (default) | Child process on the worker host         | none                                                   |
| **docker**               | Container via the worker's Docker daemon | Docker daemon + CLI (`AAICLICK_DOCKER_BIN`)            |
| **kubernetes**           | Pod in a cluster                         | `kubectl` (+ Docker & `AAICLICK_REGISTRY` for `build`) |

<!-- req: container-runners-need-distributed -->
`docker` and `kubernetes` require the distributed backends: a container or Pod
reaches shared ClickHouse + PostgreSQL over the network, but embedded chdb and a
local SQLite file cannot be shared into it. `subprocess` works in either mode.

## Image source (docker / kubernetes)

The image is a **per-task** requirement. Each container task carries an
`image_source` from one of two kinds; a task with none runs as a host
subprocess even inside a docker/kubernetes job:

| Source     | How                                                  | When built                               |
|------------|------------------------------------------------------|------------------------------------------|
| `build`    | Your git repo, built into an image at a specific SHA | by a `build-image` task in the job graph |
| `prebuilt` | `image="python:3.12"` run verbatim                   | never                                    |

<!-- req: image-source -->
Pass `image=` (`run_job` / `run-job --image`, or `register-job --image` for a
default) to select a prebuilt image — mutually exclusive with the git build
fields (`git_remote` / `git_sha` / `git_branch` / `dockerfile`). `run_job`
stamps the resolved image on the job's entry task; dynamic child tasks inherit
their parent's image unless they declare their own
(`create_task(image=...)` or `create_task(git_remote=..., git_sha=...)`). A
shell or jvm task may only return children built from its own repo or a
prefix in the worker's `AAICLICK_CHILD_BUILD_REMOTES`.

<!-- req: build-flag-defaults -->
| Flag           | Default                          | Auto-sets |
|----------------|----------------------------------|-----------|
| `--image`      | none                             | —         |
| `--build`      | off                              | —         |
| `--git-remote` | registered, else local `origin`  | `--build` |
| `--git-sha`    | remote branch head               | `--build` |
| `--git-branch` | remote default branch            | `--build` |

Defaults resolve at submission, so the stored task always names a concrete
commit. The working tree is read only when no remote is given or registered.
A task with an image fails, naming `AAICLICK_RUNNER`, where no container
runner is set: a worker without `AAICLICK_RUNNER`, local mode, or `job_test`.

<!-- req: build-image-task -->
Each distinct image gets one `build-image` task that every task on that image
depends on; it is an ordinary task in the job graph (retries, logs, UI). The
worker running it picks one of two mutually exclusive build modes:

<!-- req: build-modes -->
| Env var                    | Mode     | What the build task does                                                              |
|----------------------------|----------|---------------------------------------------------------------------------------------|
| `AAICLICK_REGISTRY=<host>` | registry | Skips if the registry has the image, else builds and pushes; every worker pulls it    |
| `AAICLICK_LOCAL_BUILD=1`   | local    | Builds and loads into the worker's own Docker daemon, no push — single-host only      |

Either way the build runs on the buildkitd at `AAICLICK_BUILDKIT_ADDR`, not on
the worker's daemon, so Dockerfile steps never see the worker's credentials.

<!-- req: build-mode-required -->
Setting both, or neither, fails the build task with an error naming the two
variables. Kubernetes `build` sources need registry mode: the cluster cannot
pull from a worker's daemon.

<!-- req: github-app-clone -->
**Private GitHub repos.** Give the worker a
[GitHub App](https://docs.github.com/en/apps/creating-github-apps) key and its
clone authenticates on its own:

| Env var                        | Value                                            |
|--------------------------------|--------------------------------------------------|
| `AAICLICK_GITHUB_APP_ID`       | The App's ID                                     |
| `AAICLICK_GITHUB_APP_PEM_FILE` | Path to the App's private key, mounted read-only |

- The App needs the **Contents: read** repository permission and must be
  installed on each repo to build.
- Any `github.com` remote works: `https://github.com/org/repo`,
  `git@github.com:org/repo.git`, `ssh://git@github.com/org/repo`. An SSH
  remote is fetched over HTTPS.
- Each build mints its own one-hour token, limited to that repo, read-only.
- When GitHub will not issue a token (App not installed on the repo, missing
  permission, outage), the clone goes ahead without credentials and the build
  task's log says why: a public repo still builds, a private one fails in git.
- A remote that carries its own password (`https://user:token@github.com/...`)
  is left alone.
- Setting only one of the two fails every build that has to clone, naming
  the other.

Setup per deployment: [Docker Compose](../deployment.md#docker-compose-mode),
[Kubernetes](../deployment.md#kubernetes-mode).

!!! warning "Submit private-repo jobs from the CLI"
    The App key authenticates the worker's clone only. Resolving a branch to
    a commit happens where the job is submitted, with that process's own git
    access: the CLI on a machine that can reach the repo works as usual. The
    API server and the scheduler have no such access, so there pass
    `--git-sha`.

!!! warning "Tasks that run on the worker can read the key"
    A task with no image runs as a subprocess of the worker and can read the
    key file, then mint tokens with everything the App is allowed. Install
    the App only on the repos to build, grant it nothing beyond
    **Contents: read**, and keep such jobs off a worker that holds the key.

For the released `aaiclick` container images and their Docker/Kubernetes
runtime requirements, see [Container Images](../container_images.md).

## Resources

<!-- req: job-resources -->
`resources` is a per-job dict (`register-job` default, overridable per
`run_job`) shaped for the deployment's one runner; mixed docker / kubernetes
fleets are not supported.

```python
# AAICLICK_RUNNER=docker — docker run flags, verbatim
run_job(name, image="python:3.12", resources={"cpus": "1.5", "memory": "512m"})

# AAICLICK_RUNNER=kubernetes — Pod container resources, verbatim
run_job(name, image="python:3.12", resources={"limits": {"cpu": "500m", "memory": "512Mi"}})
```

<!-- req: docker-resource-flags -->
- **Docker** accepts resource flags only (`cpus`, `memory`, `gpus`, …); any
  other key fails the task with the allowed list. `0` means unlimited, and
  `memory` alone allows swap: set `memory-swap` to the same value to forbid it.
- A non-mapping is rejected at register / run; a value the runner rejects fails
  the task.

## Shell tasks

<!-- req: shell-tasks -->
A `shell` task runs a literal argv (`command`, a list) instead of importing a
Python entrypoint. Success is **exit code 0**; stdout/stderr land in the normal
per-task log. The command reads its kwargs
from `$AAICLICK_IO_DIR/in.json` and may write a result — or child tasks — to
`out.json`: see [Task Interface](../task_interface.md).

```python
run_job(name, entry_type="shell", command=["python", "main.py"], command_env={"K": "v"})
```

```bash
python -m aaiclick run-job <name> --entry-type shell --command 'python main.py' --command-env K=v
```

!!! warning "Shell tasks in containers see only `command_env`"
    <!-- req: shell-container-env -->
    In an isolated environment (container/Pod) a shell task receives **only**
    `command_env` on top of the image's env — not the aaiclick runner env — so
    no DB credentials leak into an arbitrary image. With the subprocess runner
    the command inherits the worker's process env with `command_env` overlaid.
