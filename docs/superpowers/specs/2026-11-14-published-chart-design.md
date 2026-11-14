Published Helm Chart, Reference Compose
---

Replace the `compose init` / `k8s init` scaffolds with a Helm chart published to
GHCR and a reference compose file kept in the repo. Users run the chart and
customize it through `values.yaml`. They copy the compose file and own that copy.

# Why

Airflow, Dagster, Prefect, Mage and Kafka all ship a published image, a chart in
a Helm repo, and at most an example compose file. None has its CLI write
deployment files into the user's project. A scaffolded chart is a fork from day
one: upgrading aaiclick never upgrades the chart, and user edits drift from the
templates we test.

| Today                                                 | After                                                       |
|-------------------------------------------------------|-------------------------------------------------------------|
| Templates ship as package data in `aaiclick/deploy/`  | Chart and compose live in the repo under `deploy/`          |
| `k8s init` copies the chart into the user's project   | `helm install aaiclick oci://ghcr.io/kolodkin/charts/aaiclick` |
| `compose init` writes `docker-compose.yaml` + `.env`  | `curl` three files, run `env.init.sh`                       |
| `init` fills `change-me-...` tokens with random values | The chart generates secrets; `env.init.sh` does it for compose |
| Image tag pinned by substitution at `init` time       | The chart defaults to `.Chart.AppVersion`; compose reads `AAICLICK_IMAGE_TAG` |

`docker init` (the user's task-image Dockerfile) is out of scope and stays. It
writes user code scaffolding, which is the user's file by nature, like
`FROM apache/airflow`.

# Layout

```
deploy/
  helm/aaiclick/          # moved from aaiclick/deploy/templates/helm/aaiclick
    Chart.yaml
    values.yaml
    templates/
      _helpers.tpl
      credentials.yaml    # new: generated Secret
      NOTES.txt           # new: how to read the admin password
      background.yaml  buildkitd.yaml  clickhouse.yaml  postgres.yaml
      rbac.yaml  server.yaml  worker.yaml
  compose/
    docker-compose.yaml   # moved from aaiclick/deploy/templates/compose
    .env.template         # renamed from compose.env
    env.init.sh           # new: writes .env from .env.template
```

Root `docker-compose.yml` (dev infra) and `test_e2e/kubernetes/chart/` (CI deps)
are unrelated and stay.

# Helm chart

## Versioning

The chart version tracks the aaiclick version in lockstep (`1.4.2` ↔ `v1.4.2`),
so users have one number to track. `Chart.yaml` in git carries `version: 0.0.0`
and `appVersion: "0.0.0"`. The release sets both values:

```bash
helm package deploy/helm/aaiclick --version 1.4.2 --app-version v1.4.2
```

`values.yaml` image tags default to empty, and the templates fall back to the
app version:

```yaml
images:
  server:
    repository: ghcr.io/kolodkin/aaiclick
    tag: ""   # empty → .Chart.AppVersion
```

```
image: "{{ .Values.images.server.repository }}:{{ .Values.images.server.tag | default .Chart.AppVersion }}"
```

## Credentials

**Implementation**: `deploy/helm/aaiclick/templates/credentials.yaml`

Two paths, in order of preference:

1. **Production: bring your own Secret.** Set `auth.existingSecret: <name>`
   and the chart renders no credentials at all. The Secret comes from outside
   Helm (External Secrets Operator, Sealed Secrets, Vault, or plain
   `kubectl create secret`), so no credential ever sits in `values.yaml` or the
   Helm release history. The user guide leads with this path.
2. **Evaluation: generated Secret.** With no `existingSecret`, the chart
   creates `<release>-credentials` itself. Each value is the one the user set,
   else the one already stored in the cluster, else a random one. Grafana and
   Bitnami work the same way.

Rules the chart follows on both paths:

- Pods read credentials through `secretKeyRef`, never as inline `value:`.
  That keeps them out of `kubectl describe pod` and the rendered manifests.
- `values.yaml` ships no credential values, only empty overrides. There is
  nothing a user could commit by accident.
- A `checksum/credentials` pod annotation, a hash of the Secret's data, makes a
  changed credential roll the Deployments.

The generated Secret holds these keys:

| Key              | Source when unset in values                                     |
|------------------|-----------------------------------------------------------------|
| `jwt-secret`     | `randAlphaNum 48`                                               |
| `admin-password` | `randAlphaNum 24`                                               |
| `pg-password`    | `randAlphaNum 24` (only with `devDependencies.enabled`)         |
| `ch-password`    | `randAlphaNum 24` (only with `devDependencies.enabled`)         |
| `sql-url`        | `env.sqlUrl`, else built from `pg-password` by `aaiclick.sqlUrl` |
| `ch-url`         | `env.chUrl`, else built from `ch-password` by `aaiclick.chUrl`   |

```
{{- $stored := (lookup "v1" "Secret" .Release.Namespace (printf "%s-credentials" .Release.Name)).data | default dict }}
jwt-secret: {{ .Values.auth.jwtSecret | default (index $stored "jwt-secret" | default "" | b64dec) | default (randAlphaNum 48) | b64enc }}
```

- **Upgrade-stable**: `lookup` returns the stored Secret, so `helm upgrade`
  keeps every value. The Secret also carries
  `helm.sh/resource-policy: keep`, so `helm uninstall` leaves it and a
  reinstall reuses the old database passwords.
- **Bring your own**: the Secret named by `auth.existingSecret` must hold
  `jwt-secret`, `admin-password`, `sql-url` and `ch-url`. The key names are
  fixed. Bitnami-style per-key overrides (`secretKeys.*`) are left out until
  someone needs them.
- **Why `lookup` over Airflow's `pre-install` hook**: Airflow generates its
  Fernet key in a hook Secret that renders once, at install. Argo CD runs Helm
  hooks on every sync, so it regenerates the key either way. `lookup` also
  covers values added in a later chart version, which an install-only hook
  never renders.
- **Consumers**: `server.yaml`, `worker.yaml`, `background.yaml` and the
  migrate init container read through `valueFrom.secretKeyRef` instead of
  inline `value:`. `postgres.yaml` and `clickhouse.yaml` read `pg-password` /
  `ch-password` the same way.
- **Values removed**: `auth.jwtSecret`, `auth.adminPassword`,
  `devDependencies.*.password` default to `""` (set them to override
  generation). No `change-me-...` token remains in the chart.

`NOTES.txt` prints how to read the admin password once:

```bash
kubectl get secret aaiclick-credentials -o jsonpath='{.data.admin-password}' | base64 -d
```

!!! warning "`lookup` is empty under `helm template`"
    ArgoCD and `helm template` render without cluster access, so every render
    generates new values and rotates the credentials. GitOps installs must set
    `auth.existingSecret`. The user guide says so in the Kubernetes section.

## Publishing

Push to GHCR as an OCI artifact: no `gh-pages` branch or `index.yaml` to
maintain, and it sits beside the images under the same login.

```bash
helm push aaiclick-1.4.2.tgz oci://ghcr.io/kolodkin/charts
```

User install:

```bash
helm install aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version 1.4.2 \
  --set devDependencies.enabled=true
helm upgrade aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version 1.4.3 --reuse-values
```

# Compose reference

Users fetch three files from the release tag, generate `.env`, and start:

```bash
V=v1.4.2
for f in docker-compose.yaml .env.template env.init.sh; do
  curl -fsSLO https://raw.githubusercontent.com/kolodkin/aaiclick/$V/deploy/compose/$f
done
sh env.init.sh && docker compose up
```

- **Image tag**: every aaiclick image reads
  `${AAICLICK_IMAGE_TAG:?set it in .env}`. `.env.template` sets
  `AAICLICK_IMAGE_TAG=latest` with a comment to pin a release.
- **Credentials**: `.env.template` keeps the `change-me-...` tokens. The
  compose file keeps its `${VAR:?...}` references, with the message changed
  from "run `compose init`" to "run `sh env.init.sh`".

## env.init.sh

Writes `.env` beside itself from `.env.template`, replacing each distinct
`change-me-...` token with one random value, the same rule as today's
`fill_placeholders`. POSIX `sh` plus `openssl`, so it runs on Linux, macOS and
in CI with no Python.

```sh
#!/bin/sh
# Write .env from .env.template: one random value per change-me-... token.
set -eu
cd "$(dirname "$0")"
[ -e .env ] && { echo ".env already exists; delete it to regenerate" >&2; exit 1; }
umask 077
cp .env.template .env.tmp
for token in $(grep -o 'change-me-[A-Za-z0-9-]*' .env.template | sort -u); do
  sed -i.bak "s/$token/$(openssl rand -hex 24)/g" .env.tmp
done
rm -f .env.tmp.bak
mv .env.tmp .env
echo "Wrote .env (keep it out of git)"
grep '^AAICLICK_ADMIN_PASSWORD=' .env | sed 's/^AAICLICK_ADMIN_PASSWORD=/Admin login: admin \/ /'
```

- **Never overwrites**: an existing `.env` holds the live database passwords;
  regenerating it would lock the stack out of its own volumes.
- **Hex values**: URL-safe, which the database passwords need, since they also
  go into the connection URLs.
- **`umask 077`**: `.env` is readable by its owner only.
- **`sed -i.bak`**: the one in-place form that GNU and BSD `sed` both accept.
- **Hand-editing still works**: users may copy `.env.template` to `.env` and
  set values themselves; the startup guard catches any token left in.

# Startup guard

Keep it. With the chart generating its own values, the guard now catches one
thing: a compose `.env` copied from `.env.template` by hand instead of
`env.init.sh`, and never edited. That is the same case Superset refuses to
start on.

- **Move** `require_no_placeholder_credentials`, `placeholder_env_vars`,
  `PLACEHOLDER_MARKER` and `CREDENTIAL_ENV_VARS` from
  `aaiclick/deploy/placeholders.py` to `aaiclick/server/credential_guard.py`.
  Its only caller is `aaiclick/server/app.py`.
- **Delete** `fill_placeholders`, `Scaffold` and `ADMIN_PASSWORD_PLACEHOLDER`;
  only `init` used them.
- **Error message**: `"... still in use: X. Run `sh env.init.sh` or replace
  them in .env (docs/user_guide/deployment.md)"`.

# Removed

| Path                                                     | Notes                                                    |
|----------------------------------------------------------|----------------------------------------------------------|
| `aaiclick/deploy/` (whole package)                       | Templates move to `deploy/`; the guard moves to `server/` |
| `compose init`, `k8s init` in `aaiclick/__main__.py`     | Parsers, `_run_compose_init`, `_run_k8s_init`, help lines |
| `pyproject.toml` `"aaiclick.deploy" = ["templates/**/*"]` | No package data left                                     |
| `publish.yaml` wheel check for `docker-compose.yaml`     | The file is no longer in the wheel                       |
| `docs/designs/deploy_scaffold.md`                        | Rewritten as `docs/designs/deployment.md` (below)         |

# CI

The gates test the repo files directly instead of `init` output. Each gate
already runs from a checkout.

| Workflow                          | Change                                                                                                   |
|-----------------------------------|----------------------------------------------------------------------------------------------------------|
| `_helm-e2e-reusable.yaml`         | Drop "Scaffold". Lint and install `deploy/helm/aaiclick` with `--set images.server.tag=<rc>` / `images.worker.tag=<rc>`. Add an upgrade step: `helm upgrade --reuse-values`, then assert the `jwt-secret` is unchanged |
| `_compose-e2e-reusable.yaml`      | Copy `deploy/compose/` to `$RUNNER_TEMP/stack`, run `sh env.init.sh`, then set `AAICLICK_IMAGE_TAG=<rc>` in `.env` |
| `publish.yaml` `test-package`     | Same `env.init.sh` step as compose e2e; reads the database passwords from that `.env`                     |
| `publish.yaml` new `publish-chart` | `needs: [promote-images]`, so the chart never points at an image tag that doesn't exist yet. Runs `helm package --version X.Y.Z --app-version vX.Y.Z`, then `helm push`. Needs `packages: write` |
| `test.yaml` (PR CI)               | Add `helm lint deploy/helm/aaiclick` and `helm template` with `devDependencies.enabled=true` so chart breakage shows before release |

Both gates run the same `env.init.sh` users run, so CI tests the real first-run path.

# Tests

- **Delete** `aaiclick/deploy/test_compose_scaffold.py`,
  `test_k8s_scaffold.py`, `test_placeholders.py`.
- **Keep** a unit test for the guard: `aaiclick/server/test_credential_guard.py`
  (the env var with the marker → `RuntimeError` naming it; none → no error).
  The existing `server/test_app.py` import switches to the new module.
- **Chart behaviour** (generation, `lookup` reuse, `existingSecret`) is tested
  by the helm e2e gate, not by Python tests. Helm templates don't parse as
  YAML before rendering.
- `test_e2e/compose/` docstrings stop saying "scaffolded"; the tests are
  otherwise unchanged.

# Docs

- `docs/user_guide/deployment.md`: rewrite "Docker Compose mode" around
  `curl` + `sh env.init.sh`, and "Kubernetes mode" around `helm install oci://...`,
  `NOTES.txt` and `auth.existingSecret`. The Configure tables stay; `auth.*`
  gains `existingSecret`.
- `docs/designs/deploy_scaffold.md` → `docs/designs/deployment.md`. Keep
  "Release pipeline restructure" (add `publish-chart`), replace the scaffold
  sections with "Chart" and "Compose reference", and replace the trade-off
  "Generated credentials over helm `required`" with the chart-side generation
  decision below.
- `docs/drafts/tenants.md`: its `aaiclick/deploy/` reference becomes
  `deploy/helm/aaiclick/`.
- Module docstrings that cite `docs/designs/deploy_scaffold.md` move to the
  new doc name.

# References

Checked 2026-11-14 against each project's default branch. Versions are the
latest published at that date. When this spec moves to
`docs/designs/deployment.md`, the table moves with it.

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
  Mage). This spec combines the first two and rejects the third.
- **Startup guard**: Superset is the precedent. It refuses to start on its
  shipped `CHANGE_ME` key, as aaiclick does on `change-me-...`.
- **Versioning**: Dagster is lockstep; Airflow and Superset version the chart
  independently.

# Trade-offs and decisions

- **OCI on GHCR over a GitHub Pages Helm repo**: Prefect, Mage and Dagster use
  Pages, but that needs a branch and an `index.yaml` job. OCI needs one
  `helm push`. Cost: `helm search repo` doesn't list OCI charts, and Artifact
  Hub needs a separate registration.
- **Lockstep chart version over an independent one**: one number to track,
  as Dagster does; Airflow and Superset version the chart independently.
  Cost: a chart-only fix needs an aaiclick release.
- **`existingSecret` first, generation as fallback, over `required` or weak
  defaults**: production keeps credentials out of Helm entirely. A first
  evaluation `helm install` still works with no flags, as it does with `init`
  today. `required` would fail every first install; weak defaults (`admin`,
  `postgres`) leave public credentials in use. Cost: generated credentials
  rotate under `helm template` / Argo CD, so GitOps users must set
  `existingSecret`.
- **`env.init.sh` over keeping `compose init`**: keeps a one-command first
  run without shipping templates in the wheel or needing Python. It lives next
  to the compose file, versioned with it. Airflow's quickstart does the same
  with a one-line `echo ... > .env`. Cost: one more file to fetch, and
  `openssl` must be installed.
- **Chart and compose in the main repo over a separate `helm-charts` repo**:
  the e2e gates test chart and images from one commit.

# Rollout

One PR, landing with the next release. `k8s init` users already own a forked
chart. The release notes tell them to upgrade onto the published chart with
their scaffolded `values.yaml`, after deleting its `images.*.tag` lines:

```bash
helm upgrade aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version X.Y.Z -f aaiclick-chart/values.yaml
```

The credentials `init` wrote into that file win over generation, so the
database keeps its passwords. Upgrading without it would generate new ones the
existing database doesn't know. The deleted tags let the chart's own app
version pick the image.
