# Kubernetes E2E: in-cluster services via a committed Helm chart

## Goal

Replace the Kubernetes-runner e2e suite's host-side dependency services with
in-cluster pods deployed by a Helm chart committed to the repo. This removes the
three different host↔pod networking mechanisms the current workflow threads
(kind gateway IP for the databases, a containerd mirror plus `docker network
connect` for the registry, `host.docker.internal` for the build's pip) and
replaces them with one consistent "everything runs in the cluster" topology.

**Scope:** CI test infrastructure only. The chart exists to stand up
dependencies for the k8s e2e suite — it is not a production deployment artifact.
It is ephemeral (no persistence), optimized for fast cold start.

## What changes

Today (`_kubernetes-e2e-reusable.yaml`) ClickHouse, Postgres, pypiserver, and the
registry run as **GitHub Actions service containers on the host**. Pods reach the
databases via the kind docker network's gateway IP; the registry is reached via a
containerd mirror wired with `docker network connect`; the image build reaches
pypi via `host.docker.internal`.

After this change, all four services run **as pods in the kind cluster**, deployed
by `helm install`. The host reaches each via a fixed NodePort bridged to a
standard `localhost` port through kind `extraPortMappings`. Pods reach the
databases by Service name through CoreDNS.

## Topology

| Service | NodePort | Host reaches via | Pod / node reaches via | Single address? |
|---|---|---|---|---|
| Postgres | 30432 | `aaiclick-postgres:5432` (`/etc/hosts`→127.0.0.1, extraPortMapping→30432) | `aaiclick-postgres:5432` (CoreDNS) | ✅ same DSN |
| ClickHouse | 30123 | `aaiclick-clickhouse:8123` (same) | `aaiclick-clickhouse:8123` (CoreDNS) | ✅ same DSN |
| registry | 30500 | `localhost:5000` (extraPortMapping→30500) | node containerd: mirror `localhost:5000`→`localhost:30500` | n/a (image plumbing) |
| pypiserver | 30080 | `localhost:8080` (upload) / `host.docker.internal:8080` (build) | — pods never touch it | n/a (build-time only) |

### Why the DBs need a single shared DSN

The orchestrator runs on the host (the pytest process) and passes its
`AAICLICK_SQL_URL` / `AAICLICK_CH_URL` straight through to the job pod via
`build_runner_env`. If the host value were `localhost:5432`, the pod would
inherit `localhost:5432`, which inside the pod is the pod itself.

The fix keeps **one DSN** that resolves correctly in both places, with **no
runner code change**:

- Name the Services `aaiclick-postgres` / `aaiclick-clickhouse`.
- On the GHA host, add `127.0.0.1 aaiclick-postgres` / `127.0.0.1
  aaiclick-clickhouse` to `/etc/hosts`. Combined with the extraPortMapping, the
  hostname resolves to the NodePort → the pod.
- Inside the pod, the same hostname resolves via CoreDNS to the Service.

So `AAICLICK_SQL_URL=postgresql+asyncpg://aaiclick:secret@aaiclick-postgres:5432/aaiclick`
works verbatim on the host (migrations + orchestrator) and in the pod.

### Why registry and pypi do NOT need aliases

- **registry**: the host pushes to `localhost:5000` (extraPortMapping → NodePort
  30500 → registry pod); the node's containerd resolves the job image's
  `localhost:5000/...` tag through the mirror `localhost:5000` →
  `localhost:30500` (the node's own NodePort). The pod's application code never
  references the registry. So `AAICLICK_REGISTRY=localhost:5000` is unchanged.
- **pypiserver**: consumed only at `docker build` time on the host. The build
  reaches it via `host.docker.internal:8080`
  (`--add-host=host.docker.internal:host-gateway`) and the upload step uses
  `localhost:8080`. Both unchanged.

## The Helm chart

Committed at `test_e2e/kubernetes/chart/`. Self-contained — no external chart
dependencies, so nothing is fetched from a chart repo at install time.

```
test_e2e/kubernetes/chart/
  Chart.yaml
  values.yaml
  templates/
    postgres.yaml        # Deployment + NodePort Service (aaiclick-postgres, 30432)
    clickhouse.yaml      # Deployment + NodePort Service (aaiclick-clickhouse, 30123)
    pypiserver.yaml      # Deployment + NodePort Service (30080)
    registry.yaml        # Deployment + NodePort Service (30500)
```

- **Ephemeral**: `emptyDir` volumes (or none); no PVCs.
- **Readiness probes** on each Deployment so `helm install --wait` gates on real
  readiness, replacing the GHA service-container health checks.
- **`values.yaml`** holds images, credentials, and the NodePort numbers:
  - Postgres `postgres:18.3`, `aaiclick` / `secret` / db `aaiclick`.
  - ClickHouse `clickhouse/clickhouse-server:26.3`, `default` / `click123`.
  - pypiserver `pypiserver/pypiserver:v2.3.2`, args `run -p 8080 -a . -P .
    --server gunicorn`.
  - registry `registry:2`.

## Workflow changes (`_kubernetes-e2e-reusable.yaml`)

- **Remove** the entire `services:` block (clickhouse, postgres, pypiserver,
  registry).
- **Remove** the "wire the registry service" `docker network connect` step.
- **kind config**: keep the containerd mirror (now `localhost:5000` →
  `localhost:30500`); **add** `extraPortMappings` for 5432→30432, 8123→30123,
  8080→30080, 5000→30500.
- **Add** steps: install Helm; `helm install aaiclick-e2e
  ./test_e2e/kubernetes/chart --wait --timeout 5m`; append the two `/etc/hosts`
  aliases.
- DSNs become static Service-name URLs:
  - `AAICLICK_SQL_URL=postgresql+asyncpg://aaiclick:secret@aaiclick-postgres:5432/aaiclick`
  - `AAICLICK_CH_URL=clickhouse://default:click123@aaiclick-clickhouse:8123/default`
- Build / push / migrate / pytest steps: **addressing unchanged**.

The temporary `test-k8s-nightly.yaml` push trigger is re-added for on-branch
validation, then reverted to nightly once green (a `schedule` cron only fires
from the default branch).

## Non-goals

- No change to the Kubernetes runner code (`kubernetes_worker.py`, `dispatch.py`,
  `runner_env.py`, `kubernetes_config.py`).
- No production deployment chart; no persistence, secret management, or HA.
- The docker e2e suite (`_docker-e2e-reusable.yaml`) is untouched.

## Validation

1. Re-add the push trigger on `test-k8s-nightly.yaml` scoped to the branch.
2. Confirm the run is green end-to-end: kind cluster + `helm install --wait`,
   migrations, image build/push, pod pull, e2e tests, and the on-failure
   diagnostics step stays skipped (a real pass, not a no-op).
3. Revert the trigger to nightly-only.
