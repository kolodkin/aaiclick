# Published Helm Chart Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task, inline in the current session. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `compose init` / `k8s init` with a chart published to `oci://ghcr.io/kolodkin/charts/aaiclick` and a reference compose stack under `deploy/compose/`.

**Architecture:** Templates move from package data (`aaiclick/deploy/templates/`) to repo-level `deploy/`. The chart generates its own credentials Secret (or uses `auth.existingSecret`); compose gets `.env` from `env.init.sh`. The startup guard stays and moves to `aaiclick/server/credential_guard.py`. Release gates test the repo files; a new `publish-chart` job pushes the chart after `promote-images`.

**Tech Stack:** Helm 3 (OCI), POSIX sh + openssl, GitHub Actions, pytest.

**Spec:** `docs/superpowers/specs/2026-11-14-published-chart-design.md`

## Global Constraints

- Chart version lockstep with aaiclick: `helm package --version X.Y.Z --app-version vX.Y.Z`; git carries `version: 0.0.0`, `appVersion: "0.0.0"`.
- Image tags default to `""` → `.Chart.AppVersion`.
- Secret name `<release>-credentials`; keys `jwt-secret`, `admin-password`, `pg-password`, `ch-password`, `sql-url`, `ch-url`; annotation `helm.sh/resource-policy: keep`.
- `auth.existingSecret` Secret must hold `jwt-secret`, `admin-password`, `sql-url`, `ch-url`; the chart then renders no credentials Secret.
- Generated lengths: `jwt-secret` `randAlphaNum 48`; passwords `randAlphaNum 24`.
- Pods read every credential via `secretKeyRef`; pods carry a `checksum/credentials` annotation.
- Compose images read `${AAICLICK_IMAGE_TAG:?run sh env.init.sh}`; `.env.template` sets `AAICLICK_IMAGE_TAG=latest`.
- `env.init.sh`: POSIX sh, `openssl rand -hex 24`, never overwrites `.env`, `umask 077`, `sed -i.bak`.
- Guard error: `scaffold placeholder credentials still in use: X. Run `sh env.init.sh` or replace them in .env (docs/user_guide/deployment.md)`.
- `docker init` and root `docker-compose.yml` are untouched.

## Review Focus

1. `helm upgrade` of an install with generated credentials keeps every value (else the DB locks out) — helm e2e upgrade step, Task 5.
2. `auth.existingSecret` set → no `<release>-credentials` Secret, pods reference the named Secret — render check, Task 3.
3. `env.sqlUrl` / `env.chUrl` set with `devDependencies.enabled=false` → `sql-url` is that URL and no `pg-password`/`ch-password` keys — render check, Task 3.
4. `env.init.sh` run from another directory writes `.env` beside itself, not in the caller's cwd — Task 2 test.
5. `env.init.sh` with an existing `.env` exits non-zero and leaves the file byte-identical — Task 2 test.

---

### Task 1: Move the startup guard

**Files:**
- Create: `aaiclick/server/credential_guard.py`
- Create: `aaiclick/server/test_credential_guard.py`
- Modify: `aaiclick/server/app.py:13`, `aaiclick/server/test_app.py:13,209`

**Interfaces:**
- Produces: `PLACEHOLDER_MARKER = "change-me"`, `CREDENTIAL_ENV_VARS: tuple[str, ...]` (same four names), `placeholder_env_vars() -> list[str]`, `require_no_placeholder_credentials() -> None` (raises `RuntimeError`).

- [ ] **Step 1: Write failing tests** in `test_credential_guard.py`:
  `test_placeholder_credential_refuses_start` — setenv `AAICLICK_JWT_SECRET=change-me-jwt`, `pytest.raises(RuntimeError, match="AAICLICK_JWT_SECRET")` and `match="env.init.sh"`.
  `test_real_credentials_pass` — delenv all four (`raising=False`), call, no raise.
- [ ] **Step 2:** `uv run pytest aaiclick/server/test_credential_guard.py` → FAIL (module missing).
- [ ] **Step 3:** Create `credential_guard.py` with the four names copied from `aaiclick/deploy/placeholders.py`, new error text from Global Constraints, module docstring pointing at `docs/designs/deployment.md — "Startup guard"`. Switch `app.py` and `test_app.py` imports.
- [ ] **Step 4:** `uv run pytest aaiclick/server/test_credential_guard.py aaiclick/server/test_app.py` → PASS.
- [ ] **Step 5:** Commit `feat: move credential startup guard to server`.

### Task 2: Compose reference stack

**Files:**
- Move: `aaiclick/deploy/templates/compose/docker-compose.yaml` → `deploy/compose/docker-compose.yaml`
- Move: `aaiclick/deploy/templates/compose/compose.env` → `deploy/compose/.env.template`
- Create: `deploy/compose/env.init.sh` (body: spec "env.init.sh", verbatim; `chmod +x`)
- Create: `aaiclick/test_deploy_files.py`

**Interfaces:**
- Produces: `deploy/compose/{docker-compose.yaml,.env.template,env.init.sh}`; `.env` keys `AAICLICK_IMAGE_TAG`, `AAICLICK_PG_PASSWORD`, `AAICLICK_CH_PASSWORD`, `AAICLICK_JWT_SECRET`, `AAICLICK_ADMIN_PASSWORD`.

- [ ] **Step 1: Write failing tests** in `aaiclick/test_deploy_files.py` (copy `deploy/compose/` into `tmp_path`, run `sh <tmp>/env.init.sh` via `subprocess.run` with `cwd` set to a *different* tmp dir):
  `test_env_init_fills_every_token` — `.env` exists beside the script, `"change-me" not in text`, four credential values match `^[0-9a-f]{48}$`, `AAICLICK_IMAGE_TAG=latest` present, `stat.S_IMODE(mode) == 0o600`, stdout contains `Admin login: admin / ` + the admin value.
  `test_env_init_refuses_existing_env` — write `.env` = `"keep\n"`, run → `returncode != 0`, file still `"keep\n"`.
- [ ] **Step 2:** `uv run pytest aaiclick/test_deploy_files.py` → FAIL (files missing).
- [ ] **Step 3:** `git mv` both files. In `.env.template`: header says "Copied to .env by `sh env.init.sh`", add `AAICLICK_IMAGE_TAG=latest` with a pin-a-release comment. In `docker-compose.yaml`: `__AAICLICK_IMAGE_TAG__` → `${AAICLICK_IMAGE_TAG:?run sh env.init.sh}`; every `:?run python -m aaiclick compose init` → `:?run sh env.init.sh`; header comment drops "scaffolded". Add `env.init.sh`.
- [ ] **Step 4:** `uv run pytest aaiclick/test_deploy_files.py` → PASS; `cp deploy/compose/.env.template /tmp/x.env && AAICLICK_IMAGE_TAG=t docker compose --env-file /tmp/x.env -f deploy/compose/docker-compose.yaml config -q` (skip if docker absent) → exit 0.
- [ ] **Step 5:** Commit `feat: reference compose stack with env.init.sh`.

### Task 3: Chart in `deploy/helm/aaiclick` with generated credentials

**Files:**
- Move: `aaiclick/deploy/templates/helm/aaiclick/` → `deploy/helm/aaiclick/`
- Create: `deploy/helm/aaiclick/templates/credentials.yaml`, `templates/NOTES.txt`
- Create: `deploy/helm/check.sh` (render assertions, used locally and by PR CI)
- Modify: `Chart.yaml`, `values.yaml`, `templates/_helpers.tpl`, `server.yaml`, `worker.yaml`, `background.yaml`, `postgres.yaml`, `clickhouse.yaml`
- Modify: `.github/workflows/test.yaml` (new `chart` job)

**Interfaces:**
- Produces: helpers `aaiclick.credentialsSecret` (→ `auth.existingSecret` or `<release>-credentials`), `aaiclick.image` (dict `repository`/`tag` + root → `repo:tag|default AppVersion`). Values `auth.existingSecret: ""`, `auth.jwtSecret: ""`, `auth.adminPassword: ""`, `devDependencies.*.password: ""`, `images.*.tag: ""`.

- [ ] **Step 1: Write `deploy/helm/check.sh`** (bash, `set -euo pipefail`, `helm lint` then `helm template t deploy/helm/aaiclick ...` piped to grep assertions). Cases:
  default + `devDependencies.enabled=true` → output has `name: t-credentials`, keys `jwt-secret` `admin-password` `pg-password` `ch-password` `sql-url` `ch-url`, `helm.sh/resource-policy: keep`, `secretKeyRef`, `checksum/credentials`, image `ghcr.io/kolodkin/aaiclick:0.0.0`; no `change-me`; no `value:` line under `AAICLICK_JWT_SECRET`.
  `auth.existingSecret=mine` → no `t-credentials`, `name: mine` appears in `secretKeyRef`.
  `env.sqlUrl=postgresql+asyncpg://u:p@db/x`, `env.chUrl=clickhouse://u:p@ch/x` → no `pg-password`, no `ch-password`.
  `--app-version v9.9.9` is not a `template` flag: instead check `--set images.server.tag=rc1` renders `aaiclick:rc1`.
- [ ] **Step 2:** Install helm locally (`curl -fsSL https://get.helm.sh/helm-v3.19.0-linux-amd64.tar.gz | tar -xz -C $SCRATCH && export PATH=$SCRATCH/linux-amd64:$PATH`), run `bash deploy/helm/check.sh` → FAIL (chart not moved).
- [ ] **Step 3:** `git mv` the chart. Implement:
  - `Chart.yaml`: `version: 0.0.0`, `appVersion: "0.0.0"`, description drops "Scaffolded by … you own this chart".
  - `credentials.yaml`: rendered only when `not .Values.auth.existingSecret`. `$stored := (lookup "v1" "Secret" .Release.Namespace <name>).data | default dict`; per key: values → `index $stored key | b64dec` → random. `pg-password`/`ch-password` only with `devDependencies.enabled`. `sql-url`/`ch-url` from `aaiclick.sqlUrl`/`aaiclick.chUrl`, which take the resolved passwords as input (change their signature to a dict `{root, password}`).
  - Every credential env (`AAICLICK_SQL_URL`, `AAICLICK_CH_URL`, `AAICLICK_JWT_SECRET`, `AAICLICK_ADMIN_PASSWORD`, `POSTGRES_PASSWORD`, `CLICKHOUSE_PASSWORD`) → `valueFrom.secretKeyRef` on `aaiclick.credentialsSecret`.
  - Pod template annotation `checksum/credentials: {{ include (print $.Template.BasePath "/credentials.yaml") . | sha256sum }}` on server, worker, background.
  - All `image:` lines → `aaiclick.image`.
  - `values.yaml`: credentials and tags `""`, `auth.existingSecret: ""` with a comment naming the four required keys; comments drop `k8s init` wording.
  - `NOTES.txt`: the spec's `kubectl get secret … admin-password` command, using `aaiclick.credentialsSecret`.
- [ ] **Step 4:** `bash deploy/helm/check.sh` → all cases pass.
- [ ] **Step 5:** Add `chart` job to `test.yaml`: checkout, `azure/setup-helm@v4`, `bash deploy/helm/check.sh`.
- [ ] **Step 6:** Commit `feat: chart under deploy/helm with generated credentials`.

### Task 4: Remove scaffolds

**Files:**
- Delete: `aaiclick/deploy/` (remaining: `__init__.py`, `compose_scaffold.py`, `k8s_scaffold.py`, `placeholders.py`, three tests)
- Modify: `aaiclick/__main__.py` (help lines 35-36, import line 61, `_run_compose_init`, `_run_k8s_init`, the `compose`/`k8s` parsers ~1256-1310, dispatch ~1564-1570)
- Modify: `pyproject.toml` (drop `"aaiclick.deploy" = ["templates/**/*"]`)
- Modify: `.github/workflows/publish.yaml` (wheel check drops `aaiclick/deploy/templates/compose/docker-compose.yaml`)

- [ ] **Step 1:** Delete and edit as listed. Check `ghcr.image_tag` still has another caller (`grep -rn "image_tag(" aaiclick`); keep it if so, else leave it — it is outside scope.
- [ ] **Step 2:** `uv run ruff check aaiclick && grep -rn "aaiclick.deploy\|compose_scaffold\|k8s_scaffold\|placeholders" aaiclick` → no hits; `uv run python -m aaiclick --help` lists no `compose`/`k8s`.
- [ ] **Step 3:** `uv run pytest aaiclick/server aaiclick/test_deploy_files.py -x` → PASS.
- [ ] **Step 4:** Commit `refactor: drop compose/k8s init scaffolds`.

### Task 5: Release workflows

**Files:**
- Modify: `.github/workflows/_compose-e2e-reusable.yaml`, `_helm-e2e-reusable.yaml`, `publish.yaml`
- Modify: `test_e2e/compose/conftest.py`, `test_e2e/compose/test_stack_e2e.py` (docstrings only)

- [ ] **Step 1: Compose gates.** In `_compose-e2e-reusable.yaml` and `publish.yaml` `test-package`, replace the init step with:
  ```bash
  cp -r deploy/compose "$RUNNER_TEMP/stack"
  sh "$RUNNER_TEMP/stack/env.init.sh"
  sed -i "s/^AAICLICK_IMAGE_TAG=.*/AAICLICK_IMAGE_TAG=<rc tag input>/" "$RUNNER_TEMP/stack/.env"
  ```
  Drop the "Install … wheel" step from `_compose-e2e-reusable.yaml` only if no later step runs Python (the test_e2e suite does — keep it).
- [ ] **Step 2: Helm gate.** Remove "Scaffold"; lint/install `deploy/helm/aaiclick` with `--set images.server.tag=<rc> --set images.worker.tag=<rc>`. After the job round trip, add "Upgrade keeps credentials": read `jwt-secret` from `aaiclick-credentials`, `helm upgrade aaiclick deploy/helm/aaiclick --reuse-values --wait`, read again, fail if different; re-run the health smoke.
- [ ] **Step 3: `publish-chart` job** in `publish.yaml`: `needs: [promote-images]`, `permissions: packages: write`, checkout, `azure/setup-helm@v4`, `helm registry login ghcr.io -u ${{ github.actor }} --password-stdin <<< "${{ secrets.GITHUB_TOKEN }}"`, `helm package deploy/helm/aaiclick --version "${TAG#v}" --app-version "$TAG"`, `helm push aaiclick-${TAG#v}.tgz oci://ghcr.io/kolodkin/charts`.
- [ ] **Step 4:** Update `publish.yaml` header link to `docs/designs/deployment.md`; e2e docstrings say "reference compose stack".
- [ ] **Step 5:** `uvx check-jsonschema --builtin-schema vendor.github-workflows .github/workflows/{publish,_compose-e2e-reusable,_helm-e2e-reusable,test}.yaml` → ok.
- [ ] **Step 6:** Commit `ci: release gates use deploy/, publish chart to GHCR`.

### Task 6: Docs

**Files:**
- Move: `docs/designs/deploy_scaffold.md` → `docs/designs/deployment.md`
- Modify: `docs/user_guide/deployment.md`, `docs/drafts/tenants.md:182`, `mkdocs.yml` (if it lists designs)
- Delete: `docs/superpowers/specs/2026-11-14-published-chart-design.md`, this plan

- [ ] **Step 1:** `docs/designs/deployment.md`: replace "Scaffold package and CLI", "Compose template", "Helm chart template" with "Chart" (versioning, credentials, publishing), "Compose reference" (incl. `env.init.sh`), "Startup guard", all from the spec; keep "Release pipeline restructure" + gates table, add `publish-chart`; replace Testing bullets; move spec "References" table and "Trade-offs and decisions" in. Follow `markdown-style`.
- [ ] **Step 2:** User guide: Compose mode → curl three files + `sh env.init.sh && docker compose up`; Kubernetes mode → `helm install aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version X.Y.Z --set devDependencies.enabled=true`, `NOTES.txt` admin password, a "Production credentials" paragraph leading with `auth.existingSecret`, and a `!!! warning` for `helm template`/Argo CD; Configure tables: `.env` keys gain `AAICLICK_IMAGE_TAG`, `auth.*` row becomes `auth.existingSecret`, `auth.jwtSecret`, `auth.adminPassword`. Private-repo `helm upgrade` example uses the OCI ref. Add the k8s-init migration note from spec "Rollout".
- [ ] **Step 3:** `tenants.md` reference → `deploy/helm/aaiclick/`, `docs/designs/deployment.md`. `grep -rn "deploy_scaffold\|compose init\|k8s init" docs aaiclick .github` → no hits.
- [ ] **Step 4:** `uv run --extra docs mkdocs build --strict` → success.
- [ ] **Step 5:** Delete spec and plan; commit `docs: deployment design and guide for published chart`; push; run `check-pr`.
