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

# Kubernetes dispatching worker

Run `aaiclick-kubectl` as a pod. Inside a pod, `kubectl` uses **in-cluster
config** automatically — it reads the API address from the injected
`KUBERNETES_SERVICE_HOST` / `_PORT`, the token from
`/var/run/secrets/kubernetes.io/serviceaccount/token`, and the CA from the
sibling `ca.crt`. No kubeconfig mount is needed; the worker calls plain
`kubectl apply/delete/logs -n <namespace>` (see
`aaiclick/orchestration/execution/kubernetes_worker.py`).

!!! warning "A ServiceAccount + RBAC is required, or every `kubectl` call returns 403"
    `kubectl` connects fine via in-cluster config, but the API rejects it until
    the pod's ServiceAccount is bound to a Role granting exactly what the vehicle
    does: `pods` (`create`, `get`, `list`, `watch`, `delete`) and `pods/log`
    (`get`) in the target namespace.

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
`ghcr.io/kolodkin/aaiclick-kubectl`. Task pods created in a *different* namespace,
or using a private image, need the Role widened to that namespace and
`kubernetes_config.image_pull_secret` set.

!!! warning "The k8s worker also needs Docker to build the task image"
    For `build` image sources, a host-pinned `build-image` task runs
    `docker build` / `docker push` on the worker (that is why
    `aaiclick-kubectl` bundles the docker CLI). So it also needs a reachable
    Docker daemon — a mounted host socket, a remote `DOCKER_HOST`, or a build
    sidecar — plus `AAICLICK_REGISTRY` pointing at a registry the cluster can pull
    from. If task images are prebuilt and pushed out-of-band, submit with
    `image=` to skip the build and no daemon is needed.

Out-of-cluster use (talking to a remote cluster) instead mounts a kubeconfig and
sets `KUBECONFIG`.

# Docker dispatching worker

`aaiclick-docker` carries the Docker **client** only. It talks to a daemon over a
mounted socket (docker-out-of-docker); the containers it spawns are **siblings**
on the host daemon, not nested:

```bash
docker run \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ghcr.io/kolodkin/aaiclick-docker:vX.Y.Z \
  python -m aaiclick worker start ...
```

The image runs as the non-root `aaiclick` user, so it needs access to the mounted
socket — add the container to the socket's group (or run with `--user root`).

!!! warning "Mounting the docker socket grants host-daemon control"
    A container with `/var/run/docker.sock` can start, stop, and inspect any
    container on the host and mount host paths. Treat it as host-root access.
