# Release container images — design

Publish container images as part of the existing PyPI release, so users can
`docker run` the API server, deploy it to Kubernetes, and use the images as a
runner base — without building their own from scratch.

## Goals

- Ship container images alongside every `vX.Y.Z` PyPI release.
- One shared base image plus two CLI-bearing variants for the dispatching-worker
  roles.
- Reuse the exact wheel that already passed the release gate.
- Zero new secrets: publish to GHCR with the workflow's built-in token.

## Non-goals

- Multi-arch. `linux/amd64` only for the first cut; `arm64` is deferred to
  `docs/future.md`.
- A runtime-default Dockerfile for *task* images. The docker/k8s runners still
  build per-task images from the user's checked-in Dockerfile at a git SHA —
  that model is unchanged (`aaiclick/orchestration/execution/docker_scaffold.py`).

## The three images

All published to GHCR under `ghcr.io/kolodkin/`, tagged `vX.Y.Z` always and
`latest` only on a non-pre-release.

| Image | Bundles | Roles it serves |
| --- | --- | --- |
| `aaiclick` | wheel only | API server (default), task base, local/mp worker |
| `aaiclick-kubectl` | `+ kubectl` | Kubernetes-dispatching worker (in-cluster) |
| `aaiclick-docker` | `+ docker` CLI | Docker-dispatching worker (host socket mount) |

The runners shell out to the real CLIs via `asyncio.create_subprocess_exec` —
`docker` (`AAICLICK_DOCKER_BIN`, default `"docker"`) and `kubectl`
(`AAICLICK_KUBECTL_BIN`, default `"kubectl"`). Only the *dispatching worker*
role invokes them; the API server and task-leaf roles never do. Hence the base
image is CLI-free and the two variants add exactly one binary each.

### Base image — `docker/Dockerfile`

- `FROM python:3.10-slim`
- `ARG AAICLICK_VERSION`; `pip install --no-cache-dir "aaiclick[all]==${AAICLICK_VERSION}"`
  from PyPI. `[all]` pulls `distributed,ai,server` — the server needs
  `fastapi`/`uvicorn`, the runner base needs `distributed`.
- Non-root: create `aaiclick` user, `USER aaiclick`, `WORKDIR /app`.
- `EXPOSE 5255`
- `HEALTHCHECK CMD` probing `http://127.0.0.1:5255/health`.
- `CMD ["uvicorn", "aaiclick.server.app:app", "--host", "0.0.0.0", "--port", "5255"]`
  — `docker run -p 5255:5255 ghcr.io/kolodkin/aaiclick` serves the SPA + `/api/v0`
  out of the box. Bundled SPA ships in the wheel already (`aaiclick/server/static`).

### Variant Dockerfiles — `docker/kubectl.Dockerfile`, `docker/docker.Dockerfile`

Each `FROM ghcr.io/kolodkin/aaiclick:<tag>` (the base built in the same run) and
installs one CLI, keeping the base layers shared:

- `kubectl.Dockerfile`: download the pinned `kubectl` binary, verify its
  sha256, drop it on `PATH`.
- `docker.Dockerfile`: install the Docker CLI (`docker-ce-cli` only — the client,
  not the daemon).

## Release-workflow integration

Extend `.github/workflows/publish.yaml` with one `publish-image` job:

- `needs: [publish]` — runs only after the wheel is live on PyPI and after the
  `test-package-local` / `test-package` / `test-package-docker-e2e` gates that
  `publish` already depends on. The images therefore install the same validated
  wheel a downstream user would.
- `permissions: { contents: read, packages: write }`.
- Matrix over the three images. Each entry names its Dockerfile and image name;
  the base builds first, variants `FROM` it. Because a matrix has no ordering
  guarantee, build the base in a non-matrix step (or a preceding job) and the two
  variants in the matrix — see "Open question: build ordering".
- Steps: `docker/setup-buildx-action` → `docker/login-action` to `ghcr.io` with
  `${{ github.actor }}` / `${{ secrets.GITHUB_TOKEN }}` → `docker/metadata-action`
  for tags → `docker/build-push-action` with `platforms: linux/amd64`,
  `build-args: AAICLICK_VERSION=${tag#v}`.
- Tags: `vX.Y.Z` always; `latest` added only when `inputs.pre-release == false`
  (reuse the existing `pre-release` input; `docker/metadata-action`'s
  `enable=${{ !inputs.pre-release }}` on the `latest` tag).

Trade-off accepted: installing from PyPI (not the release's local pypiserver)
means the job waits on PyPI propagation. This is simplest and yields a true
"what a user gets" image; the docker-e2e gate already proved the wheel installs
into a working container from an index, so the risk is low.

## Documentation

New `docs/container_images.md` (subject to `markdown-style`), linked from
`docs/getting_started.md`. Covers all three images and — the explicit ask — the
**k8s and docker runtime requirements**.

### API server (base image)

```bash
docker run -p 5255:5255 ghcr.io/kolodkin/aaiclick:vX.Y.Z
# UI + API at http://localhost:5255  (health: /health, API: /api/v0)
```

### Runner base

```dockerfile
FROM ghcr.io/kolodkin/aaiclick:vX.Y.Z
COPY . /src
RUN pip install --no-cache-dir /src
```

### Kubernetes dispatching worker — requirements

`kubectl` inside a pod uses **in-cluster config** automatically: it reads the
API address from the injected `KUBERNETES_SERVICE_HOST`/`_PORT`, the token from
`/var/run/secrets/kubernetes.io/serviceaccount/token`, and the CA from the
sibling `ca.crt`. No kubeconfig mount needed. The vehicle calls plain
`kubectl apply/delete/logs -n <namespace>` with no `--kubeconfig`/`--context`
(`aaiclick/orchestration/execution/kubernetes_worker.py`), so ambient in-cluster
resolution is what makes it work.

The gotcha is **RBAC, not connectivity** — `kubectl` connects but the API
returns `403` unless the pod's ServiceAccount is bound to a Role granting exactly
what the vehicle does. Ship this manifest in the docs:

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
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: aaiclick-task-runner, namespace: default }
subjects: [{ kind: ServiceAccount, name: aaiclick-worker, namespace: default }]
roleRef: { kind: Role, name: aaiclick-task-runner, apiGroup: rbac.authorization.k8s.io }
```

The worker Deployment sets `serviceAccountName: aaiclick-worker` and runs
`ghcr.io/kolodkin/aaiclick-kubectl`. Task pods created in a *different*
namespace, or using a private image, need the Role widened to that namespace and
`kubernetes_config.image_pull_secret` set.

Out-of-cluster use (talking to a remote cluster) instead mounts a kubeconfig and
sets `KUBECONFIG` — documented but not optimized for.

### Docker dispatching worker — requirements

The `aaiclick-docker` image carries only the Docker **client**. It talks to a
daemon over a mounted socket (docker-out-of-docker); the containers it spawns are
**siblings** on the host daemon, not nested:

```bash
docker run \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ghcr.io/kolodkin/aaiclick-docker:vX.Y.Z \
  python -m aaiclick worker start ...
```

Requirements to document: the socket mount; that the container user needs access
to the socket (socket group ownership / running the worker appropriately); and
the security note that mounting the docker socket grants host-daemon control.

## Open questions

1. **Build ordering.** The variants `FROM` the base, so the base image tag must
   exist before they build. Simplest: build+push the base in one job, then a
   second job (`needs`) matrixes the two variants. Alternative: single job,
   base as an ordered step before the variant matrix. Decide during
   implementation-plan writing.
2. **Docker CLI as non-root + socket group.** The base image runs non-root; the
   mounted socket is typically `root:docker`. Document running the docker
   variant with an appropriate group, or leave the docker variant root. Resolve
   when writing the docker variant Dockerfile.

## Files touched

- `docker/Dockerfile` (new) — base image.
- `docker/kubectl.Dockerfile`, `docker/docker.Dockerfile` (new) — variants.
- `.github/workflows/publish.yaml` — add `publish-image` job(s).
- `docs/container_images.md` (new); link from `docs/getting_started.md`.
- `docs/future.md` — record deferred arm64/multi-arch.
