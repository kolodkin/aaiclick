Deployment Scaffolds and RC-Gated Release
---

Scaffolds for deploying aaiclick from the published GHCR images — a docker compose file for
the docker-runner setup and a helm chart for the kubernetes setup — plus a restructured PyPI
release pipeline that builds release-candidate images, gates the release on end-to-end tests
against those exact images, and promotes the tested digests. The native (subprocess-runner)
setup needs no scaffold: it is `pip install` + `python -m aaiclick setup`.

# Goals

- One-command scaffolds, following the `python -m aaiclick docker init` precedent: the
  framework writes a sensible starter into the user's directory; from there the user owns it.
- Scaffolds reference the GHCR images (`ghcr.io/kolodkin/aaiclick`, `aaiclick-docker`,
  `aaiclick-kubectl`) pinned to the installed aaiclick version.
- The PyPI release gates on the scaffolds working against candidate builds of those images —
  the release tests dogfood the same artifacts users deploy with, replacing GitHub Actions
  `services:` blocks.
- The image digests that were tested are the digests that get published (`-rc` tag promoted
  to `vX.Y.Z` + `latest` — no post-publish rebuild).

# Scaffold package and CLI

New package `aaiclick/deploy/` with templates as package data. `docker_scaffold.py` keeps a
single Dockerfile as a Python string; a helm chart is many files, so these templates live as
real files shipped in the wheel and are copied out with `importlib.resources`.

```
aaiclick/deploy/
    compose_scaffold.py          # init_compose(), version rendering, ComposeFileExists
    k8s_scaffold.py              # init_helm(), HelmChartExists
    templates/
        compose/docker-compose.yaml
        helm/aaiclick/
            Chart.yaml
            values.yaml
            templates/*.yaml
```

CLI:

```bash
python -m aaiclick compose init [--path docker-compose.yaml] [--image-tag vX.Y.Z] [--force]
python -m aaiclick k8s init [--path ./aaiclick-chart] [--image-tag vX.Y.Z] [--force]
```

- One compose variant only: the docker-runner setup. It is the superset (native users can
  strip the socket mount and registry from their scaffolded copy), and one template means one
  artifact to maintain and test.
- Templates render on write: image tags default to `v{installed aaiclick version}` via
  `importlib.metadata`, overridable with `--image-tag`. Helm's `Chart.yaml` gets the same
  version as `appVersion`.
- Existing files are never silently overwritten — same `--force` semantics and error style as
  `init_dockerfile()` (`ComposeFileExists` / `HelmChartExists` mirroring `DockerfileExists`).

**Implementation**: aaiclick/deploy/compose_scaffold.py — see init_compose(); aaiclick/deploy/k8s_scaffold.py —
see init_helm(); CLI wiring in aaiclick/__main__.py — see _run_compose_init() / _run_k8s_init().

# Compose template (docker-runner setup)

**Implementation**: aaiclick/deploy/templates/compose/docker-compose.yaml.

Full stack — `docker compose up` yields a complete working docker-runner deployment:

| Service    | Image                             | Role                                                 |
|------------|-----------------------------------|------------------------------------------------------|
| clickhouse | clickhouse/clickhouse-server      | Data backend; healthcheck; named volume              |
| postgres   | postgres                          | Orchestration state; healthcheck; named volume       |
| registry   | registry:2                        | Task-image registry; host port published             |
| migrate    | ghcr.io/kolodkin/aaiclick         | One-shot `python -m aaiclick migrate upgrade head`   |
| server     | ghcr.io/kolodkin/aaiclick         | Image default CMD (uvicorn :5255); port published    |
| worker     | ghcr.io/kolodkin/aaiclick-docker  | `python -m aaiclick execution-worker start`; mounts `/var/run/docker.sock` |
| background | ghcr.io/kolodkin/aaiclick         | `python -m aaiclick background start` (cleanup)      |

- migrate runs after postgres is healthy; server/worker/background start after migrate
  completes successfully (`depends_on: condition: service_completed_successfully`).
- Task containers run as siblings on the host daemon, so clickhouse/postgres/registry ports
  are published to the host, `AAICLICK_REGISTRY` points at the host-published registry, and
  task-facing DSNs use `host.docker.internal` (`extra_hosts: host-gateway`) — sibling
  containers cannot resolve compose service names. The worker itself also addresses
  ClickHouse/Postgres/the registry via `host.docker.internal` (not the compose service name)
  so that spawned task containers inherit the exact same env var values verbatim; only
  server/background use compose service names in `AAICLICK_SQL_URL` / `AAICLICK_CH_URL`.
- No profiles: `docker compose up` always brings up the whole stack. CI jobs that only need
  the infra services still run the full stack — simpler than maintaining profile splits.

# Helm chart template (kubernetes setup)

**Implementation**: aaiclick/deploy/templates/helm/aaiclick/.

A user-facing chart — distinct from `test_e2e/kubernetes/chart`, which is CI-only dependency
infra and stays as it is.

Templates: server Deployment + Service, worker Deployment (`aaiclick-kubectl` image),
background cleanup Deployment (drives job completion, as in the compose stack),
ServiceAccount + Role/RoleBinding scoped to pod create/get/list/watch/delete (what the
kubernetes runner needs), and optional in-cluster ClickHouse + Postgres gated by
`devDependencies.enabled` for evaluation setups. Migrations run as an initContainer on the
server (not a helm hook — pre-install hooks execute before chart resources exist, so they can
never succeed against in-chart databases); the worker's initContainer waits for server health,
giving the same migrate → server → worker ordering as the compose stack.

`values.yaml` covers: image repository/tag per component, `AAICLICK_SQL_URL` /
`AAICLICK_CH_URL` (secret-ref or literal), registry URL, imagePullSecret, namespace-scoped
RBAC toggles, resources.

# Release pipeline restructure

**Implementation**: .github/workflows/publish.yaml.

```
build ──► build-rc-images ──► merge-rc-images ──► gates (parallel) ──► publish (PyPI) ──► promote-images
```

## build-rc-images — per-platform matrix

The three images are FROM-chained (`aaiclick` → `aaiclick-docker` → `aaiclick-kubectl`), so
they cannot build in parallel per-image. They parallelize per-platform instead, which is also
the larger win — a QEMU-emulated arm64 leg would otherwise dominate the build time:

- Matrix over `platform: [amd64, arm64]`. Each leg runs a local pypiserver serving the wheel
  artifact, builds the three images in FROM order natively (`PIP_INDEX_URL` build-arg already
  exists in `docker/Dockerfile`), and pushes per-arch digests to GHCR.
- A merge job assembles the multi-arch `vX.Y.Z-rc` manifest per image with
  `docker buildx imagetools create` (matrix over the three image names).
- arm64 leg runs on the `ubuntu-24.04-arm` native runner.

The wheel baked into the rc images is the same artifact file later uploaded to PyPI.

## Gates — all parallel, all required

All gates depend only on `build` + `merge-rc-images` (or `build` alone for the local-only
gate), so they run concurrently.

**Implementation**: .github/workflows/publish.yaml — see jobs test-package-local,
test-package, test-compose-e2e, test-helm-e2e, test-package-docker-e2e;
.github/workflows/_compose-e2e-reusable.yaml; .github/workflows/_helm-e2e-reusable.yaml.

| Gate                     | Shape                                                                        |
|--------------------------|-------------------------------------------------------------------------------|
| test-package-local       | Unchanged (chdb + SQLite, no infra)                                          |
| test-package             | Existing data/orch matrix; infra now comes from the scaffolded compose file (full stack up), waiting for the compose `migrate` service to exit 0, instead of `services:` blocks |
| test-compose-e2e         | Scaffold via `compose init`, point tags at `-rc`, `docker compose up`, wait for server health, then `docker compose exec` into the server container to register/run a job via the CLI and poll `job list --json` for completion |
| test-helm-e2e            | kind cluster with the `-rc` images side-loaded (`kind load docker-image`), `k8s init` scaffolded chart, `helm lint` + `helm install`, then a server-health port-forward smoke and a `kubectl exec` job round trip (`register-job` / `run-job` / `job list --json`) |
| test-package-docker-e2e  | Existing `_docker-e2e-reusable.yaml` release gate, unchanged                 |

## promote-images

Replaces the current `publish-image` job. After PyPI publish succeeds,
`docker buildx imagetools create -t vX.Y.Z -t latest <image>:vX.Y.Z-rc` per image (matrix
over the three names) — no rebuild, no wait-for-PyPI polling. The tested digests are the
published digests. A final step deletes the `-rc` tags on success; failed releases leave them
behind for debugging.

# Testing

**Implementation**: aaiclick/deploy/test_compose_scaffold.py; aaiclick/deploy/test_k8s_scaffold.py;
test_e2e/compose/ (marker `compose_e2e`, `AAICLICK_E2E_COMPOSE_DIR`).

- Unit tests adjacent to each scaffold module, mirroring `test_docker_scaffold.py`: file
  written, `--force` semantics, version/tag rendering, output parses as YAML with the expected
  services (compose) or the chart tree/`Chart.yaml`/`values.yaml` rendering (helm). Helm
  templates are not YAML-parseable pre-render; `helm lint` runs in the helm e2e gate, not in
  unit tests.
- `test_e2e/compose/` drives the scaffolded stack in CI: server health, an execution worker
  registered, and a job round trip driven via `docker compose exec` into the server container
  (register/run/poll through the CLI, not the HTTP API).

# Trade-offs and decisions

- **rc-promote over post-publish rebuild**: the current `publish-image` installs from PyPI
  after publishing, so published images were never tested. Promotion guarantees tested bits
  == published bits and removes the wait-for-PyPI loop. Cost: `-rc` tags exist transiently on
  GHCR and permanently for failed releases (kept deliberately for debugging).
- **Package-data templates over Python strings**: a chart is many files; real files also get
  editor syntax support and can be linted in CI.
- **Single compose variant (docker-runner)**: less is more — it is the superset of the native
  setup, and one template means one artifact to maintain, test, and gate on.
- **No compose profiles**: everything comes up on `docker compose up`. Running the app
  services during infra-only CI jobs costs a little, but one invocation with no modes is
  simpler for users and CI alike.
