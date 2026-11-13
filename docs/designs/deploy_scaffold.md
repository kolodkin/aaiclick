Deployment Scaffolds and RC-Gated Release
---

Scaffolds for deploying aaiclick from the published GHCR images — a docker
compose file for the docker-runner setup and a helm chart for the kubernetes
setup — plus a release pipeline that builds release-candidate images, gates
the release on end-to-end tests against those exact images, and promotes the
tested digests. The native (subprocess-runner) setup needs no scaffold:
`pip install` + `python -m aaiclick setup`.

# Goals

- One-command scaffolds, following `python -m aaiclick docker init`: the
  framework writes a starter into the user's directory; from there the user
  owns it.
- Scaffolds reference the GHCR images (`ghcr.io/kolodkin/aaiclick`,
  `aaiclick-docker`, `aaiclick-kubectl`) pinned to the installed aaiclick
  version.
- The PyPI release gates on the scaffolds working against candidate builds of
  those images — release tests dogfood the artifacts users deploy with,
  instead of GitHub Actions `services:` blocks.
- The tested image digests are the published ones (`-rc` promoted to
  `vX.Y.Z` + `latest`, no post-publish rebuild).

# Scaffold package and CLI

**Implementation**: `aaiclick/deploy/`

Templates ship as package data under `aaiclick/deploy/templates/` (`compose/`,
`helm/aaiclick/`) and are copied out with `importlib.resources`. Unlike the
single-Dockerfile scaffold, kept as a Python string, a chart is many files;
real files also get editor syntax support and can be linted in CI.

```bash
python -m aaiclick compose init [--path docker-compose.yaml] [--image-tag vX.Y.Z] [--force]
python -m aaiclick k8s init [--path ./aaiclick-chart] [--image-tag vX.Y.Z] [--force]
```

- **One compose variant**: the docker-runner setup. It is the superset
  (native users can strip the socket mount and registry from their copy), and
  one template means one artifact to maintain, test and gate on.
- **Render on write**: image tags default to `v{installed aaiclick version}`
  (`importlib.metadata`), overridable with `--image-tag`; `Chart.yaml` gets the
  same version as `appVersion`.
- **No silent overwrite**: same `--force` semantics and error style as
  `docker init`.

# Compose template (docker-runner setup)

**Implementation**: `aaiclick/deploy/templates/compose/`

`docker compose up` yields a complete docker-runner deployment:

| Service    | Image                            | Role                                                                       |
|------------|----------------------------------|----------------------------------------------------------------------------|
| clickhouse | clickhouse/clickhouse-server     | Data backend; healthcheck; named volume                                    |
| postgres   | postgres                         | Orchestration state; healthcheck; named volume                             |
| registry   | registry:2                       | Task-image registry; host port published                                   |
| migrate    | ghcr.io/kolodkin/aaiclick        | One-shot `python -m aaiclick migrate upgrade head`                         |
| server     | ghcr.io/kolodkin/aaiclick        | Image default CMD (uvicorn :5255); port published                          |
| worker     | ghcr.io/kolodkin/aaiclick-docker | `python -m aaiclick execution-worker start`; mounts `/var/run/docker.sock` |
| background | ghcr.io/kolodkin/aaiclick        | `python -m aaiclick background start` (cleanup)                            |

- migrate runs once postgres is healthy; server, worker and background start
  after it exits 0 (`depends_on: condition: service_completed_successfully`).
- Task containers run as siblings on the host daemon and cannot resolve
  compose service names. So clickhouse, postgres and registry publish host
  ports, `AAICLICK_REGISTRY` points at the host-published registry, and
  task-facing DSNs use `host.docker.internal` (`extra_hosts: host-gateway`).
  The worker itself uses `host.docker.internal` too, so spawned task
  containers inherit its env values verbatim; only server and background use
  service names in `AAICLICK_SQL_URL` / `AAICLICK_CH_URL`.
- Distributed URLs enforce auth, so the server carries starter
  `AAICLICK_JWT_SECRET` / `AAICLICK_ADMIN_PASSWORD` — without them it refuses
  to start or cannot be logged in to.
- Each secret is set once. `init` writes a `.env` beside the compose file
  whose `change-me-...` tokens become random values, and the compose file
  references them as `${...}` in both the database service and every URL;
  `${VAR:?...}` on the service side makes `up` without the `.env` fail naming
  the variable. The compose file then holds no secrets and can be committed.
  Cost: a password lands in a URL verbatim, so it must be URL-safe. The
  server refuses to start while any credential still carries the marker. The
  user-facing list is `docs/user_guide/deployment.md`.
- **No profiles**: `docker compose up` always brings up the whole stack. CI
  jobs that need only the infra services run the full stack too — a little
  extra cost, but one invocation with no modes is simpler for users and CI.

The worker ships the GitHub App key mount and its two `AAICLICK_GITHUB_APP_*`
settings commented out: private-repo builds are opt-in, and the key is mounted
from disk, never written into the scaffold.

# Helm chart template (kubernetes setup)

**Implementation**: `aaiclick/deploy/templates/helm/aaiclick/`

A user-facing chart, distinct from the CI-only dependency chart in
`test_e2e/kubernetes/chart`.

Templates: server Deployment + Service, worker Deployment (`aaiclick-kubectl`
image), background cleanup Deployment (drives job completion, as in the
compose stack), ServiceAccount + Role/RoleBinding scoped to pod
create/get/list/watch/delete (what the kubernetes runner needs), and optional
in-cluster ClickHouse + Postgres gated by `devDependencies.enabled` for
evaluation setups.

Migrations run as an initContainer on the server, not a helm hook:
pre-install hooks execute before chart resources exist, so they can never
succeed against in-chart databases. The worker's initContainer waits for
server health, giving the compose stack's migrate → server → worker order.

`values.yaml` covers: image repository/tag per component, `env.sqlUrl` /
`env.chUrl`, registry URL, the server's `auth.jwtSecret` /
`auth.adminPassword` (generated by `init`, like the database credentials),
imagePullSecret, namespace-scoped RBAC toggles, resources. Empty
`env.sqlUrl` / `env.chUrl` are built by `_helpers.tpl` from the
`devDependencies` user and password (URL-encoded), so each database password
is set in one place; a full URL overrides it for a managed database.

`githubApp.appId` + `githubApp.secretName` (both empty by default) give the
worker the same settings from a Secret the user creates, mounted `0440` with
`fsGroup: 1000` so the image's non-root user can read it. Setting one alone
fails the render.

# Release pipeline restructure

**Implementation**: `.github/workflows/publish.yaml`

```
build ──► build-rc-images ──► merge-rc-images ──► gates (parallel) ──► publish (PyPI) ──► promote-images
```

## build-rc-images — per-platform matrix

The three images are FROM-chained (`aaiclick` → `aaiclick-docker` →
`aaiclick-kubectl`), so they cannot build in parallel per image. They
parallelize per platform instead, the larger win — a QEMU-emulated arm64 leg
would otherwise dominate build time:

- Matrix over `platform: [amd64, arm64]`; arm64 runs on the native
  `ubuntu-24.04-arm` runner. Each leg serves the wheel artifact from a local
  pypiserver, builds the three images natively in FROM order (the base
  through `.github/actions/build-base-image`), and pushes per-arch digests to
  GHCR.
- A merge job (matrix over the three images) assembles each multi-arch
  `vX.Y.Z-rc` manifest with `docker buildx imagetools create`.

The wheel baked into the rc images is the same file later uploaded to PyPI.

## Gates — all parallel, all required

Every gate depends only on `build` + `merge-rc-images` (or `build` alone for
the local-only gate), so they run concurrently.

| Gate                    | Shape                                                                                                                                                                                                                                                 |
|-------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| test-package-local      | chdb + SQLite, no infra                                                                                                                                                                                                                               |
| test-package            | Data/orch matrix; infra from the scaffolded compose file (full stack up, waiting for `migrate` to exit 0) instead of `services:` blocks                                                                                                               |
| test-compose-e2e        | `compose init`, tags pointed at `-rc`, `docker compose up`, wait for server health, then `docker compose exec` into the server to register/run a job via the CLI and poll `job list --json`                                                           |
| test-helm-e2e           | kind cluster with the `-rc` images side-loaded (`kind load docker-image`), `k8s init` chart, `helm lint` + `helm install`, a port-forwarded server-health smoke, and a `kubectl exec` job round trip (`register-job` / `run-job` / `job list --json`) |
| test-package-docker-e2e | `_docker-e2e-reusable.yaml`; `base_image` is the `-rc` base, so git builds without a Dockerfile layer on the promoted digest                                                                                                                          |

## promote-images

After PyPI publish succeeds, `docker buildx imagetools create -t vX.Y.Z -t
latest <image>:vX.Y.Z-rc` per image (matrix over the three names) — no
rebuild, no wait-for-PyPI polling. A final step deletes the `-rc` tags on
success; failed releases keep them for debugging.

# Testing

- Unit tests beside each scaffold module: file written, `--force`
  semantics, version/tag rendering, output parses as YAML with the expected
  services (compose) or the chart tree / `Chart.yaml` / `values.yaml`
  rendering (helm). Helm templates are not YAML-parseable before rendering,
  so `helm lint` runs in the helm e2e gate, not in unit tests.
- `test_e2e/compose/` drives the scaffolded stack in CI: server health, an
  execution worker registered, and a job round trip via `docker compose exec`
  into the server (register/run/poll through the CLI, not the HTTP API).

# Trade-offs and decisions

- **rc-promote over post-publish rebuild**: the previous image job installed
  from PyPI after publishing, so published images were never tested.
  Promotion guarantees tested bits == published bits and removes the
  wait-for-PyPI loop. Cost: `-rc` tags exist transiently on GHCR, and
  permanently for failed releases.
- **Generated credentials over helm `required`**: `init` writes a random
  value for every credential, so a first install works unmodified with
  nothing public; `required` would fail every install and e2e gate instead.
  A placeholder that reaches the server (a template copied by hand) refuses
  startup rather than warning: a log line nobody reads leaves a public secret
  in use.
