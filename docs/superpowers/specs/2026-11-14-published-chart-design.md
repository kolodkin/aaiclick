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
| `compose init` writes `docker-compose.yaml` + `.env`  | `curl` the compose file and `.env.example`                  |
| `init` fills `change-me-...` tokens with random values | The chart generates secrets; for compose, the user fills `.env` |
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
    .env.example          # renamed from compose.env
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

A single Secret, `<release>-credentials`, holds every credential. The same
pattern as Grafana and Bitnami: use the value the user set, else reuse the
value already stored in the cluster, else generate one.

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
- **Bring your own**: `auth.existingSecret: <name>` skips the generated
  Secret. The named Secret must hold `jwt-secret`, `admin-password`,
  `sql-url` and `ch-url`.
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

Users fetch two files from the release tag:

```bash
V=v1.4.2
curl -fsSLO https://raw.githubusercontent.com/kolodkin/aaiclick/$V/deploy/compose/docker-compose.yaml
curl -fsSL  https://raw.githubusercontent.com/kolodkin/aaiclick/$V/deploy/compose/.env.example -o .env
# edit .env, then:
docker compose up
```

- **Image tag**: every aaiclick image reads
  `${AAICLICK_IMAGE_TAG:?set it in .env}`. `.env.example` sets
  `AAICLICK_IMAGE_TAG=latest` with a comment to pin a release.
- **Credentials**: `.env.example` keeps the `change-me-...` tokens, like
  Superset's `CHANGE_ME` secret key. The compose file keeps its `${VAR:?...}`
  references, with the message changed from "run `compose init`" to "set it in
  `.env`".
- **Generating values**: the user guide gives a one-liner per secret:

  ```bash
  openssl rand -hex 24
  ```

  Hex output is URL-safe, which the database passwords need.

# Startup guard

Keep it. With the chart generating its own values, the guard now catches one
thing: a compose `.env` copied from `.env.example` and never edited. That is the
same case Superset refuses to start on.

- **Move** `require_no_placeholder_credentials`, `placeholder_env_vars`,
  `PLACEHOLDER_MARKER` and `CREDENTIAL_ENV_VARS` from
  `aaiclick/deploy/placeholders.py` to `aaiclick/server/credential_guard.py`.
  Its only caller is `aaiclick/server/app.py`.
- **Delete** `fill_placeholders`, `Scaffold` and `ADMIN_PASSWORD_PLACEHOLDER`;
  only `init` used them.
- **Error message**: `"... still in use: X. Replace them in .env
  (docs/user_guide/deployment.md)"`.

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
| `_compose-e2e-reusable.yaml`      | Copy `deploy/compose/*` to `$RUNNER_TEMP/stack`, write `.env` from `.env.example` with `openssl rand` values and `AAICLICK_IMAGE_TAG=<rc>` |
| `publish.yaml` `test-package`     | Same `.env` step as compose e2e; reads the database passwords from that `.env`                            |
| `publish.yaml` new `publish-chart` | `needs: [promote-images]`, so the chart never points at an image tag that doesn't exist yet. Runs `helm package --version X.Y.Z --app-version vX.Y.Z`, then `helm push`. Needs `packages: write` |
| `test.yaml` (PR CI)               | Add `helm lint deploy/helm/aaiclick` and `helm template` with `devDependencies.enabled=true` so chart breakage shows before release |

The `.env` step is shared by two workflows; put it in
`.github/actions/compose-env/` (a composite action) rather than duplicating it.

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
  `curl` + edit `.env`, and "Kubernetes mode" around `helm install oci://...`,
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

# Trade-offs and decisions

- **OCI on GHCR over a GitHub Pages Helm repo**: Prefect, Mage and Dagster use
  Pages, but that needs a branch and an `index.yaml` job. OCI needs one
  `helm push`. Cost: `helm search repo` doesn't list OCI charts, and Artifact
  Hub needs a separate registration.
- **Lockstep chart version over an independent one (Airflow's model)**: one
  number to track. Cost: a chart-only fix needs an aaiclick release.
- **Chart-side generation over `required`**: a first `helm install` works with
  no flags, as it does with `init` today. Cost: credentials rotate under
  `helm template`, so GitOps users must set `existingSecret`.
- **Compose `.env` filled by hand over keeping `compose init`**: matches
  Airflow and Dagster, and drops a CLI command plus its package data. Cost:
  one copy-and-edit step before the first `up`; the startup guard catches it
  being skipped.
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
