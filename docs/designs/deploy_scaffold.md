Deployment Scaffolds and RC-Gated Release
---

Scaffolds for deploying aaiclick from the published GHCR images — docker compose for the
native (subprocess-runner) and docker-runner setups, a helm chart for the kubernetes setup —
plus a restructured PyPI release pipeline that builds release-candidate images, gates the
release on end-to-end tests against those exact images, and promotes the tested digests.

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
    scaffold.py                  # init_compose(), init_helm(), version rendering, *Exists errors
    templates/
        compose-native/docker-compose.yaml
        compose-docker/docker-compose.yaml
        helm/aaiclick/
            Chart.yaml
            values.yaml
            templates/*.yaml
```

CLI:

```bash
python -m aaiclick compose init [--runner subprocess|docker] [--path docker-compose.yaml] [--image-tag vX.Y.Z] [--force]
python -m aaiclick k8s init [--path ./aaiclick-chart] [--image-tag vX.Y.Z] [--force]
```

- `--runner` reuses the existing `RunnerMode` literals; `subprocess` (the native setup) is the
  default, matching `register-job --runner`.
- Templates render on write: image tags default to `v{installed aaiclick version}` via
  `importlib.metadata`, overridable with `--image-tag`. Helm's `Chart.yaml` gets the same
  version as `appVersion`.
- Existing files are never silently overwritten — same `--force` semantics and error style as
  `init_dockerfile()` (`ComposeFileExists` / `HelmChartExists` mirroring `DockerfileExists`).

**Implementation**: `aaiclick/deploy/scaffold.py`; CLI wiring in `aaiclick/__main__.py`
alongside `_run_docker_init()`.

# Native compose template

Full stack — `docker compose up` yields a complete working deployment:

| Service    | Image                          | Role                                                    |
|------------|--------------------------------|---------------------------------------------------------|
| clickhouse | clickhouse/clickhouse-server   | Data backend; healthcheck; named volume                 |
| postgres   | postgres                       | Orchestration state; healthcheck; named volume          |
| migrate    | ghcr.io/kolodkin/aaiclick      | One-shot `python -m aaiclick migrate upgrade head`      |
| server     | ghcr.io/kolodkin/aaiclick      | Image default CMD (uvicorn :5255); port published       |
| worker     | ghcr.io/kolodkin/aaiclick      | `python -m aaiclick execution-worker start`             |
| background | ghcr.io/kolodkin/aaiclick      | `python -m aaiclick background start` (cleanup)         |

- `AAICLICK_SQL_URL` / `AAICLICK_CH_URL` point at the compose service names.
- migrate runs after postgres is healthy; server/worker/background start after migrate
  completes successfully (`depends_on: condition: service_completed_successfully`).
- **Profiles**: clickhouse and postgres are profile-free; migrate/server/worker/background sit
  in an `app` profile. `docker compose up clickhouse postgres` gives infra-only — this is what
  lets CI reuse the user-facing compose file in place of GitHub Actions `services:` blocks,
  and lets developers run the test suite against compose-provided infra.

# Docker-runner compose template

Same skeleton with the docker-runner additions:

- worker image is `ghcr.io/kolodkin/aaiclick-docker` with `/var/run/docker.sock` mounted —
  task containers run as siblings on the host daemon.
- a `registry:2` service, published on a host port, with `AAICLICK_REGISTRY` pointing at it.
- clickhouse/postgres ports published to the host, and task-facing DSNs expressed via
  `host.docker.internal` (`extra_hosts: host-gateway`), because sibling task containers cannot
  resolve compose service names.

# Helm chart template (kubernetes setup)

A user-facing chart — distinct from `test_e2e/kubernetes/chart`, which is CI-only dependency
infra and stays as it is.

Templates: server Deployment + Service, worker Deployment (`aaiclick-kubectl` image), migrate
Job as a `pre-install,pre-upgrade` helm hook, ServiceAccount + Role/RoleBinding scoped to pod
create/get/list/watch/delete (what the kubernetes runner needs), and optional in-cluster
ClickHouse + Postgres gated by `devDependencies.enabled` for evaluation setups.

`values.yaml` covers: image repository/tag per component, `AAICLICK_SQL_URL` /
`AAICLICK_CH_URL` (secret-ref or literal), registry URL, imagePullSecret, namespace-scoped
RBAC toggles, resources.

# Release pipeline restructure

```
build ──► build-rc-images ──► gates (parallel) ──► publish (PyPI) ──► promote-images
```

## build-rc-images — per-platform matrix

The three images are FROM-chained (`aaiclick` → `aaiclick-docker` → `aaiclick-kubectl`), so
they cannot build in parallel per-image. They parallelize per-platform instead, which is also
the larger win — arm64 under QEMU emulation is the slow leg today:

- Matrix over `platform: [amd64, arm64]`. Each leg runs a local pypiserver serving the wheel
  artifact, builds the three images in FROM order natively (`PIP_INDEX_URL` build-arg already
  exists in `docker/Dockerfile`), and pushes per-arch digests to GHCR.
- A merge job assembles the multi-arch `vX.Y.Z-rc` manifest per image with
  `docker buildx imagetools create` (matrix over the three image names).
- arm64 leg runs on `ubuntu-24.04-arm` when available for the repo; fallback is QEMU on
  `ubuntu-latest` for that leg only — the matrix shape is identical either way.

The wheel baked into the rc images is the same artifact file later uploaded to PyPI.

## Gates — all parallel, all required

All gates depend only on `build` + `build-rc-images`, so they run concurrently:

| Gate                | Shape                                                                            |
|---------------------|----------------------------------------------------------------------------------|
| test-package-local  | Unchanged (chdb + SQLite, no infra)                                              |
| test-package        | Existing data/orch matrix; infra now comes from the scaffolded compose file (infra-only profile) instead of `services:` blocks |
| compose e2e         | New `_compose-e2e-reusable.yaml`, matrix over `setup: [native, docker]` — scaffold via `compose init`, point tags at `-rc`, `docker compose up`, wait for health, submit a job through the API, assert completion |
| helm e2e            | kind cluster + `k8s init` scaffolded chart installed with the `-rc` images, reusing the existing kubernetes e2e harness |
| docker e2e          | Existing `_docker-e2e-reusable.yaml` release gate, unchanged                     |

## promote-images

Replaces the current `publish-image` job. After PyPI publish succeeds,
`docker buildx imagetools create -t vX.Y.Z -t latest <image>:vX.Y.Z-rc` per image (matrix
over the three names) — no rebuild, no wait-for-PyPI polling. The tested digests are the
published digests. A final step deletes the `-rc` tags on success; failed releases leave them
behind for debugging.

# Testing

- Unit tests in `aaiclick/deploy/test_scaffold.py`, mirroring `test_docker_scaffold.py`:
  files written, `--force` semantics, version/tag rendering, compose output parses as YAML
  with the expected services and profiles, chart directory structure and `Chart.yaml`
  version present. Helm templates are not YAML-parseable pre-render; `helm lint` runs in the
  helm e2e gate, not in unit tests.
- `test_e2e/compose/` drives the scaffolded stack in CI: health endpoints up, job submission
  round-trip against the running server/worker.

# Trade-offs and decisions

- **rc-promote over post-publish rebuild**: the current `publish-image` installs from PyPI
  after publishing, so published images were never tested. Promotion guarantees tested bits
  == published bits and removes the wait-for-PyPI loop. Cost: `-rc` tags exist transiently on
  GHCR and permanently for failed releases (kept deliberately for debugging).
- **Package-data templates over Python strings**: a chart is many files; real files also get
  editor syntax support and can be linted in CI.
- **Compose profiles over separate infra/app files**: one file per setup keeps the scaffold a
  single artifact users reason about, while still letting CI start infra alone.
