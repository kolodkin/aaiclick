# Release container images — design

Publish container images as part of the existing PyPI release, so users can
`docker run` the API server, deploy it to Kubernetes, and use the images as a
runner base — without building their own from scratch.

## Goals

- Ship container images alongside every `vX.Y.Z` PyPI release.
- One shared base image plus two CLI-bearing variants for the dispatching-worker
  roles.
- Multi-arch: build `linux/amd64` + `linux/arm64` for every image via buildx.
- Reuse the exact wheel that already passed the release gate.
- Zero new secrets: publish to GHCR with the workflow's built-in token.

## Non-goals

- A runtime-default Dockerfile for *task* images. The docker/k8s runners still
  build per-task images from the user's checked-in Dockerfile at a git SHA —
  that model is unchanged (`aaiclick/orchestration/execution/docker_scaffold.py`).

## The three images

All published to GHCR under `ghcr.io/kolodkin/`, tagged `vX.Y.Z` always and
`latest` only on a non-pre-release. Each is built for `linux/amd64` and
`linux/arm64`.

| Image | Bundles | Roles it serves |
| --- | --- | --- |
| `aaiclick` | wheel only | API server (default), task base, local/mp worker |
| `aaiclick-docker` | `+ docker` CLI | Docker-dispatching worker (host socket mount) |
| `aaiclick-kubectl` | `+ docker + kubectl` | Kubernetes-dispatching worker (in-cluster) |

The runners shell out to the real CLIs via `asyncio.create_subprocess_exec` —
`docker` (`AAICLICK_DOCKER_BIN`, default `"docker"`) and `kubectl`
(`AAICLICK_KUBECTL_BIN`, default `"kubectl"`). Only the *dispatching worker*
role invokes them; the API server and task-leaf roles never do. Hence the base
image is CLI-free.

The Kubernetes variant carries **both** CLIs, not just `kubectl`: in k8s mode the
dispatching worker still builds the task image with `docker build`/`docker push`
before creating pods (`resolve_image_tag` → `build_image_to_tag`,
`image_builder.py` / `docker_build.py`), then `kubectl` launches pods that
reference the pushed tag. So the images layer as a strict chain — base →
`aaiclick-docker` (+docker) → `aaiclick-kubectl` (+kubectl) — sharing all lower
layers and giving a deterministic build order.

### Base image — `docker/Dockerfile`

- `FROM python:3.13-slim` (latest stable 3.13; `requires-python >=3.10` and the
  deps ship 3.13 wheels).
- `ARG AAICLICK_VERSION`; `pip install --no-cache-dir "aaiclick[all]==${AAICLICK_VERSION}"`
  from PyPI. `[all]` pulls `distributed,ai,server` — the server needs
  `fastapi`/`uvicorn`, the runner base needs `distributed`.
- Non-root: create `aaiclick` user, `USER aaiclick`, `WORKDIR /app`.
- `EXPOSE 5255`
- `HEALTHCHECK CMD` probing `http://127.0.0.1:5255/health`.
- `CMD ["uvicorn", "aaiclick.server.app:app", "--host", "0.0.0.0", "--port", "5255"]`
  — `docker run -p 5255:5255 ghcr.io/kolodkin/aaiclick` serves the SPA + `/api/v0`
  out of the box. Bundled SPA ships in the wheel already (`aaiclick/server/static`).

### Variant Dockerfiles — `docker/docker.Dockerfile`, `docker/kubectl.Dockerfile`

Built as a chain so each shares the layers below it:

- `docker.Dockerfile`: `FROM ghcr.io/kolodkin/aaiclick:<tag>`; install the Docker
  CLI (`docker-ce-cli` only — the client, not the daemon; apt resolves the arch).
- `kubectl.Dockerfile`: `FROM ghcr.io/kolodkin/aaiclick-docker:<tag>`; download the
  pinned `kubectl` binary and verify its sha256, then drop it on `PATH`. So it
  inherits docker from the layer below and adds kubectl.

Both the base and the variants are multi-arch. The Dockerfiles use buildx's
`TARGETARCH` build-arg to fetch the correct `kubectl` binary per platform
(`amd64` / `arm64`); the base and the apt-installed docker CLI are arch-agnostic.

## Release-workflow integration

Extend `.github/workflows/publish.yaml` with one `publish-image` job:

- `needs: [publish]` — runs only after the wheel is live on PyPI and after the
  `test-package-local` / `test-package` / `test-package-docker-e2e` gates that
  `publish` already depends on. The images therefore install the same validated
  wheel a downstream user would.
- `permissions: { contents: read, packages: write }`.
- The three images form a `FROM` chain (base → docker → kubectl), so build them
  sequentially in one job — each `build-push-action` step pushes its tag before
  the next step's `FROM` resolves it. (A matrix has no ordering guarantee and the
  chain needs one, so ordered steps, not a matrix.)
- Steps: `docker/setup-qemu-action` (arm64 emulation) → `docker/setup-buildx-action`
  → `docker/login-action` to `ghcr.io` with `${{ github.actor }}` /
  `${{ secrets.GITHUB_TOKEN }}` → three `docker/metadata-action` +
  `docker/build-push-action` pairs (base, docker, kubectl) with
  `platforms: linux/amd64,linux/arm64`, `build-args: AAICLICK_VERSION=${tag#v}`.
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

**The k8s worker also needs Docker to build the task image.** Before creating
pods it runs `docker build`/`docker push` (that is why `aaiclick-kubectl` bundles
the docker CLI). So the worker also needs a reachable Docker daemon — a mounted
host socket, a remote `DOCKER_HOST`, or a build sidecar — plus `AAICLICK_REGISTRY`
set to a registry the cluster can pull from. Same socket/security caveats as the
docker worker below. If task images are prebuilt and pushed out-of-band, set the
job's `image_source` to skip the build and the daemon is not needed.

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

1. **Docker CLI as non-root + socket group.** The base image runs non-root; the
   mounted socket is typically `root:docker`. Document running the docker/kubectl
   variants with an appropriate group, or leave those variants root. Resolve when
   writing the docker variant Dockerfile.

## Files touched

- `docker/Dockerfile` (new) — base image (`python:3.13-slim`).
- `docker/docker.Dockerfile`, `docker/kubectl.Dockerfile` (new) — chained variants.
- `.github/workflows/publish.yaml` — add the `publish-image` job.
- `docs/container_images.md` (new); link from `docs/getting_started.md`.
