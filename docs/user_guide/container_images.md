Container Images
---

Every `vX.Y.Z` release publishes three container images to GHCR, built for
`linux/amd64` and `linux/arm64`. Each is tagged with the exact version and, for
non-pre-releases, `latest`.

| Image                             | Bundles              | Role                                              |
|-----------------------------------|----------------------|---------------------------------------------------|
| `ghcr.io/kolodkin/aaiclick`         | wheel only           | API server (default), task base, local/mp worker  |
| `ghcr.io/kolodkin/aaiclick-docker`  | `+ docker` CLI       | Docker-dispatching worker                         |
| `ghcr.io/kolodkin/aaiclick-kubectl` | `+ docker + kubectl` | Kubernetes-dispatching worker                     |

The images layer as a chain — `aaiclick` → `aaiclick-docker` → `aaiclick-kubectl`
— so each variant shares all the layers below it. Only the dispatching-worker
roles need the CLIs; the API server and task-leaf roles never shell out.

# API server

The base image serves the SPA + REST API by default:

```bash
docker run -p 5255:5255 ghcr.io/kolodkin/aaiclick:vX.Y.Z
# UI + API at http://localhost:5255  (health: /health, API: /api/v0)
```

Runs as non-root `aaiclick`, with a `HEALTHCHECK` on `/health`.

# Runner base

Base a job's task image on it so per-task builds skip reinstalling the framework:

```dockerfile
FROM ghcr.io/kolodkin/aaiclick:vX.Y.Z
COPY . /src
RUN pip install --no-cache-dir /src
```

A git build (`--build`) whose repo has no `Dockerfile` gets this layer by
default, pinned to the worker's aaiclick version, with `WORKDIR /src` instead of
`pip install` — entrypoints resolve from the copied tree, so a job in
`jobs/my_job.py` is `jobs.my_job.entry` and every directory on the path must be
an importable package name (`abc-123` or `2026-10-09` cannot be imported). Set
`AAICLICK_BASE_IMAGE` on the worker to use another base: an rc tag, or a locally
built image when the worker runs from a git checkout — a developer's machine, or
CI running the e2e from source (its version, `0.0.1.dev50+g...`, has no
published image; the tag rule drops the `+...` segment so the reference stays
valid). Check in a `Dockerfile` (`python -m aaiclick docker init` scaffolds one)
to customize; an explicit `dockerfile=` path that is missing still fails.

The default honours a dependency manifest at the repo root:

| Found at repo root                                   | Template | Install step                                                        |
|------------------------------------------------------|----------|---------------------------------------------------------------------|
| none of the below                                    | python   | none — the thin layer above                                         |
| `uv.lock` with `pyproject.toml`                      | python   | `uv export --frozen --no-dev` → `uv pip install`                    |
| `requirements.txt` (otherwise)                       | python   | `uv pip install -r requirements.txt`                                |
| `pyproject.toml` with a `[project]` table            | python   | `uv pip install -r pyproject.toml`                                  |
| `pom.xml`, no Python manifest                        | jvm      | `mvn package`, SDK shim as `ENTRYPOINT`                             |
| `pom.xml` **and** a Python manifest                  | error    | build fails: check in a `Dockerfile`                                |

`uv` runs on the worker's `PIP_*` index settings with the checkout mounted as its
working directory, so `-r` / `-c` includes, path and workspace dependencies
resolve. Packages the base image ships — aaiclick and everything it depends
on — keep the base image's version whatever the manifest pins, so a user lock
cannot downgrade aaiclick or swap the dependencies it was tested with. The
project itself is never `pip install .`-ed. Limits:

- Installs from the checkout must be non-editable: the mount is gone at
  runtime, so `-e .` points nowhere (a `uv.lock` export already skips the
  project). The mount has no `.git`, so a setuptools-scm project cannot build.
- Builds run on a buildkitd (`AAICLICK_BUILDKIT_ADDR`); a custom worker image
  needs the docker CLI with `buildx` like the shipped ones.
- A `.dockerignore` that excludes the manifest hides it from the install.
- Python code next to a `pom.xml` with no Python manifest gets the JVM image;
  a mixed repo adds a Python manifest.
- The image is cached per commit (`aaiclick-job:<sha>`, plus `-<dockerfile>`
  when one is named): a commit built before the worker learned manifests
  keeps its old image until a new commit lands.
- The JVM default is single-module, JDK/JRE 21 from Docker Hub, and expects
  `aaiclick-task-api` declared in the pom (`docs/designs/java-sdk.md`).

Beyond these, check in a `Dockerfile`.

!!! warning "Private index URLs are secrets, not build-args"
    `AAICLICK_PIP_INDEX_URL` / `AAICLICK_PIP_EXTRA_INDEX_URL` may embed a
    token, so a Dockerfile that declares `ARG PIP_INDEX_URL` no longer receives
    them. Mount them into the step that installs:

    ```dockerfile
    # syntax=docker/dockerfile:1
    RUN --mount=type=secret,id=pip_index_url,env=PIP_INDEX_URL \
        --mount=type=secret,id=pip_extra_index_url,env=PIP_EXTRA_INDEX_URL \
        pip install --index-url "${PIP_INDEX_URL:-https://pypi.org/simple/}" -r requirements.txt
    ```

# Kubernetes dispatching worker

Run `aaiclick-kubectl` as a pod with `AAICLICK_RUNNER=kubernetes`: container
tasks become Pods, tasks without an image run in-place. Pod namespace, service
account and imagePullSecret come from `AAICLICK_K8S_NAMESPACE`,
`AAICLICK_K8S_SERVICE_ACCOUNT`, `AAICLICK_K8S_IMAGE_PULL_SECRET`. Inside a pod,
`kubectl` uses **in-cluster
config** automatically — it reads the API address from the injected
`KUBERNETES_SERVICE_HOST` / `_PORT`, the token from
`/var/run/secrets/kubernetes.io/serviceaccount/token`, and the CA from the
sibling `ca.crt`. No kubeconfig mount is needed; the worker calls plain
`kubectl apply/delete/logs -n <namespace>` (see
`aaiclick/orchestration/execution/kubernetes_worker.py`).

!!! warning "A ServiceAccount + RBAC is required, or every `kubectl` call returns 403"
    `kubectl` connects fine via in-cluster config, but the API rejects it until
    the pod's ServiceAccount is bound to a Role granting exactly what the vehicle
    does: `pods` (`create`, `get`, `list`, `watch`, `delete`), `pods/log`
    (`get`), `pods/exec` (`create`, `get` — a shell task's `in.json` /
    `out.json` move through `kubectl cp` / `exec`) and `secrets` (`create`,
    `delete` — its `command_env` rides in a per-attempt Secret) in the target
    namespace.

```yaml
apiVersion: v1
kind: ServiceAccount
metadata: { name: aaiclick-worker, namespace: default }
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: aaiclick-task-runner, namespace: default }
rules:
  - apiGroups: [""]
    resources: [pods]
    verbs: [create, get, list, watch, delete]
  - apiGroups: [""]
    resources: [pods/log]
    verbs: [get]
  - apiGroups: [""]
    resources: [pods/exec]
    verbs: [create, get]
  - apiGroups: [""]
    resources: [secrets]
    verbs: [create, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: aaiclick-task-runner, namespace: default }
subjects: [{ kind: ServiceAccount, name: aaiclick-worker, namespace: default }]
roleRef: { kind: Role, name: aaiclick-task-runner, apiGroup: rbac.authorization.k8s.io }
```

The worker Deployment sets `serviceAccountName: aaiclick-worker` and runs
`ghcr.io/kolodkin/aaiclick-kubectl`. Task pods created in a *different* namespace,
or using a private image, need the Role widened to that namespace and
`AAICLICK_K8S_IMAGE_PULL_SECRET` set.

!!! warning "`build` sources need buildkitd and a registry"
    A host-pinned `build-image` task runs `docker buildx build --push` on the
    worker against `AAICLICK_BUILDKIT_ADDR` (the chart's `buildkitd`, enabled by
    default) — no Docker daemon. Set `env.registry` to a registry both
    buildkitd and the cluster reach. With prebuilt images, submit with `image=`
    and no build runs.

Out-of-cluster use (talking to a remote cluster) instead mounts a kubeconfig and
sets `KUBECONFIG`.

# Docker dispatching worker

`aaiclick-docker` carries the Docker **client** only. With
`AAICLICK_RUNNER=docker` it talks to a daemon over a mounted socket
(docker-out-of-docker); the containers it spawns are **siblings** on the host
daemon, not nested:

```bash
docker run \
  -e AAICLICK_RUNNER=docker \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ghcr.io/kolodkin/aaiclick-docker:vX.Y.Z \
  python -m aaiclick execution-worker start
```

A dispatching worker needs a ClickHouse server; with chdb it refuses to start.

The image runs as the non-root `aaiclick` user, so it needs access to the mounted
socket — add the container to the socket's group (or run with `--user root`).

!!! warning "Mounting the docker socket grants host-daemon control"
    A container with `/var/run/docker.sock` can start, stop, and inspect any
    container on the host and mount host paths. Treat it as host-root access.
