Deployment Artifacts and RC-Gated Release
---

How aaiclick ships for distributed deployment: published GHCR images, a Helm
chart published to GHCR, and a reference compose stack in the repo, plus a
release pipeline that gates on end-to-end tests against release-candidate
images and promotes the tested digests. The native (subprocess-runner) setup
needs none of it: `pip install` + `python -m aaiclick setup`.

# Goals

- Ship deployment artifacts the way comparable tools do (see References):
  users run the chart and customize it through `values.yaml`, and copy the
  compose stack. The CLI writes no deployment files into a user's project.
- Both reference the GHCR images (`ghcr.io/kolodkin/aaiclick`,
  `aaiclick-docker`, `aaiclick-kubectl`).
- The PyPI release gates on chart and compose working against candidate
  builds of those images, from the same commit.
- The tested image digests are the published ones (`-rc` promoted to
  `vX.Y.Z` + `latest`, no post-publish rebuild).

# Chart

**Implementation**: `deploy/helm/aaiclick/`

A user-facing chart, distinct from the CI-only dependency chart in
`test_e2e/kubernetes/chart`.

Templates: server Deployment + Service, worker Deployment (`aaiclick-kubectl`
image), background cleanup Deployment, ServiceAccount + Role/RoleBinding
scoped to pod create/get/list/watch/delete (what the kubernetes runner
needs), the credentials Secret, and optional in-cluster ClickHouse + Postgres
gated by `devDependencies.enabled` for evaluation.

Migrations run as an initContainer on the server, not a helm hook:
pre-install hooks execute before chart resources exist, so they can never
succeed against in-chart databases. The worker's initContainer waits for
server health, giving the compose stack's migrate → server → worker order.

`githubApp.appId` + `githubApp.secretName` (both empty by default) give the
worker a GitHub App key from a Secret the user creates, mounted `0440` with
`fsGroup: 1000` so the image's non-root user can read it. Setting one alone
fails the render.

## Versioning

The chart version tracks aaiclick in lockstep (`1.4.2` ↔ `v1.4.2`). Git
carries `version: 0.0.0` / `appVersion: "0.0.0"`; the release sets both with
`helm package --version --app-version`. `images.*.tag` defaults to `""`,
which renders `.Chart.AppVersion`.

## Credentials

**Implementation**: `deploy/helm/aaiclick/templates/credentials.yaml`

Two paths, production first:

1. **`auth.existingSecret`**: a Secret managed outside Helm (External Secrets
   Operator, Sealed Secrets, Vault, `kubectl`) with keys `jwt-secret`,
   `admin-password`, `sql-url`, `ch-url` (plus `pg-password`, `ch-password`
   with `devDependencies`). The chart renders no credentials at all, so none
   sit in `values.yaml` or the release history.
2. **Generated**: otherwise the chart renders `<release>-credentials`. Each
   value is the one set in values, else the one stored in that Secret
   (`lookup`, so `helm upgrade` keeps it), else random (`randAlphaNum 48` for
   `jwt-secret`, 24 for passwords). `sql-url` / `ch-url` are `env.sqlUrl` /
   `env.chUrl`, else built from the generated passwords (URL-encoded).
   `helm.sh/resource-policy: keep` makes the Secret survive `helm uninstall`,
   so a reinstall keeps the database passwords.

Pods read every credential through `secretKeyRef`, and carry a
`checksum/credentials` annotation so a changed credential rolls them.
`NOTES.txt` prints how to read the admin password.

- **`lookup` over Airflow's `pre-install` hook Secret**: Argo CD runs Helm
  hooks on every sync, so a hook regenerates either way; `lookup` also covers
  keys added in a later chart version.
- **Known limit**: `lookup` is empty under `helm template` / Argo CD, so
  generated values rotate on every render there. GitOps installs must set
  `auth.existingSecret`.

`deploy/helm/check.sh` lints the chart and asserts on renders (generated
Secret, `existingSecret`, external URLs, image tags); PR CI runs it.

## Publishing

OCI artifact on GHCR, beside the images:

```bash
helm install aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version 1.4.2
```

# Compose reference

**Implementation**: `deploy/compose/`

`docker-compose.yaml`, `.env.template` and `env.init.sh`, fetched from the
release tag. `docker compose up` yields a complete docker-runner deployment:

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
- Every credential and `AAICLICK_IMAGE_TAG` is a `${VAR:?run sh env.init.sh}`
  reference to `.env`, used by both the database service and every URL, so
  each secret is set once and `up` without `.env` fails naming the variable.
  Cost: a password lands in a URL verbatim, so it must be URL-safe.
- **No profiles**: `docker compose up` always brings up the whole stack. CI
  jobs that need only the infra services run the full stack too.

The worker ships the GitHub App key mount and its two `AAICLICK_GITHUB_APP_*`
settings commented out: private-repo builds are opt-in, and the key is
mounted from disk.

## env.init.sh

Writes `.env` beside itself from `.env.template`, replacing each distinct
`change-me-...` token with `openssl rand -hex 24` (hex is URL-safe). POSIX
`sh`, so it runs on Linux, macOS and in CI without Python. It refuses to
overwrite an existing `.env`, whose passwords the database volumes already
hold, and writes it with `umask 077`.

# Startup guard

**Implementation**: `aaiclick/server/credential_guard.py`

The server refuses to start while `AAICLICK_JWT_SECRET`,
`AAICLICK_ADMIN_PASSWORD`, `AAICLICK_SQL_URL` or `AAICLICK_CH_URL` still
contains `change-me`: an `.env` copied from `.env.template` by hand and never
edited. It refuses rather than warns, because a log line nobody reads leaves
a public secret in use. Superset does the same for its shipped `CHANGE_ME`
secret key.

# Release pipeline restructure

**Implementation**: `.github/workflows/publish.yaml`

```
build ──► build-rc-images ──► merge-rc-images ──► gates (parallel) ──► publish (PyPI) ──► promote-images ──► publish-chart
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
| test-package            | Data/orch matrix; infra from `deploy/compose` (`env.init.sh`, full stack up, waiting for `migrate` to exit 0) instead of `services:` blocks                                                                                                           |
| test-compose-e2e        | `deploy/compose` + `env.init.sh`, `AAICLICK_IMAGE_TAG` set to `-rc`, `docker compose up`, wait for server health, then `docker compose exec` into the server to register/run a job via the CLI and poll `job list --json`                            |
| test-helm-e2e           | k3s cluster with the `-rc` images side-loaded (`k3s ctr images import`), `deploy/helm/check.sh`, `helm install` of `deploy/helm/aaiclick`, a port-forwarded server-health smoke, a `kubectl exec` job round trip (`register-job` / `run-job` / `job list --json`), then `helm upgrade` must leave the credentials Secret unchanged |
| test-package-docker-e2e | `_docker-e2e-reusable.yaml`; `base_image` is the `-rc` base, so git builds without a Dockerfile layer on the promoted digest                                                                                                                          |

## promote-images

After PyPI publish succeeds, `docker buildx imagetools create -t vX.Y.Z -t
latest <image>:vX.Y.Z-rc` per image (matrix over the three names) — no
rebuild, no wait-for-PyPI polling. A final step deletes the `-rc` tags on
success; failed releases keep them for debugging.

## publish-chart

After `promote-images`, so the chart never names an image tag that does not
exist yet: `helm package deploy/helm/aaiclick --version X.Y.Z --app-version
vX.Y.Z`, then `helm push` to `oci://ghcr.io/kolodkin/charts`.

# Testing

- `aaiclick/test_deploy_files.py` runs `env.init.sh` as a user would.
- `deploy/helm/check.sh` covers chart renders; helm templates are not YAML
  before rendering, so they get no Python tests.
- `test_e2e/compose/` drives the reference stack in CI: server health, an
  execution worker registered, and a job round trip via `docker compose exec`
  into the server.

# References

Checked 2026-11-14 against each project's default branch; versions are the
latest published at that date. Re-check before relying on a row.

| Tool            | Version checked              | Image                    | Compose                                       | Helm chart                                                | Chart secrets                                                   |
|-----------------|------------------------------|--------------------------|-----------------------------------------------|-----------------------------------------------------------|-----------------------------------------------------------------|
| Airflow         | 3.4.0, chart 2.0.0           | Official, in main repo   | Docs reference, fetched with `curl`, not for production | `chart/` in the main repo, served from a Helm repo on airflow.apache.org, version independent of Airflow | Fernet and API keys: random, in a `pre-install` hook Secret; admin `admin`/`admin`; Postgres `postgres` |
| Dagster         | 1.13.30, chart 1.13.30       | Official                 | Example in `examples/deploy_docker/`          | `helm/dagster` in the main repo, GitHub Pages; **lockstep** with Dagster | Bundled Postgres `test`                                         |
| Prefect         | 3.9.3, chart 2026.11.13192421 | Official, in main repo   | None in repo                                  | Separate `prefect-helm` repo, GitHub Pages                | Basic auth off by default; bring-your-own Secret                |
| Mage            | 0.9.79, chart 0.2.19         | `mageai/mageai`          | Repo root, for developing Mage itself         | Separate `mage-ai/helm-charts` repo, GitHub Pages         | `your_password` placeholder, "REQUIRED: override" comment       |
| Bruin           | `main` at check date         | CLI image                | None                                          | None (single CLI binary, no server)                       | n/a                                                             |
| Kafka           | 4.4.0                        | Official, `docker/` in main repo | Examples in `docker/examples/`         | None from Apache (Strimzi, Bitnami)                       | n/a                                                             |
| Spark           | 4.3.0                        | Separate `apache/spark-docker` repo | None                                | Separate `apache/spark-kubernetes-operator` repo          | n/a                                                             |
| Grafana         | 13.3.1, chart 14.4.1         | Official                 | n/a                                           | `grafana-community/helm-charts`                           | Admin password `randAlphaNum 40`, reused via `lookup`           |
| Bitnami Postgres | 17.6.0, chart 17.1.2        | Bitnami                  | n/a                                           | `bitnami/charts`                                          | Random, reused via `lookup`; `existingSecret` + `secretKeys.*`  |
| Superset        | 6.1.0, chart 0.22.8          | Official                 | In repo                                       | `helm/superset` in the main repo                          | Ships `CHANGE_ME` secret key; **the app refuses to start** on it; admin `admin` |
| Temporal        | 1.32.2, chart 1.8.0          | Official                 | n/a                                           | Separate `temporalio/helm-charts`                         | Bring-your-own Secret only                                      |
| Argo Workflows  | v4.1.6, chart 2.0.18         | Official                 | n/a                                           | `argoproj/argo-helm`                                      | Bring-your-own Secret only                                      |

What the table says:

- **No tool scaffolds deployment files** into the user's project. Every one with
  a server publishes an image and a chart in a Helm repo.
- **Secrets split three ways**: generate + `lookup` (Grafana, Bitnami), bring
  your own only (Temporal, Argo, Prefect), or weak defaults (Airflow, Dagster,
  Mage). aaiclick combines the first two and rejects the third.
- **Startup guard**: Superset is the precedent. It refuses to start on its
  shipped `CHANGE_ME` key, as aaiclick does on `change-me-...`.
- **Versioning**: Dagster is lockstep; Airflow and Superset version the chart
  independently.

# Trade-offs and decisions

- **rc-promote over post-publish rebuild**: published images were never
  tested when they were rebuilt from PyPI after publishing. Promotion
  guarantees tested bits == published bits. Cost: `-rc` tags exist
  transiently on GHCR, and permanently for failed releases.
- **Published chart over a scaffolded one**: a scaffolded chart is a fork
  from day one; upgrading aaiclick never upgrades it, and user edits drift
  from the tested templates. No comparable tool scaffolds deployment files.
- **OCI on GHCR over a GitHub Pages Helm repo**: one `helm push`, no
  `gh-pages` branch or `index.yaml` job. Cost: `helm search repo` doesn't list
  OCI charts.
- **Lockstep chart version**: one number to track, as Dagster does; Airflow
  and Superset version the chart independently. Cost: a chart-only fix needs
  an aaiclick release.
- **`existingSecret` first, generation as fallback, over `required` or weak
  defaults**: production keeps credentials out of Helm; a first evaluation
  install still works with no flags. `required` would fail every first
  install; weak defaults (`admin`, `postgres`) leave public credentials in use.
- **`env.init.sh` over a CLI command**: a one-command first run without
  shipping templates in the wheel or needing Python. Cost: `openssl` must be
  installed.
- **Chart and compose in the main repo**: the e2e gates test chart, compose
  and images from one commit.
