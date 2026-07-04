# Deployment Scaffolds and RC-Gated Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `python -m aaiclick compose init` (docker-runner compose scaffold) and `python -m aaiclick k8s init` (helm chart scaffold), and restructure `publish.yaml` so PyPI release gates on e2e tests against `-rc` GHCR images whose tested digests get promoted to `vX.Y.Z` + `latest`.

**Architecture:** New `aaiclick/deploy/` package holds templates as package data, copied out with `importlib.resources` and rendered by replacing the `__AAICLICK_IMAGE_TAG__` token. CI builds candidate images per-platform (matrix), merges multi-arch `-rc` manifests, runs all gates in parallel against them, publishes to PyPI, then promotes the exact tested manifests with `docker buildx imagetools create`.

**Tech Stack:** Python 3.10+ stdlib (`importlib.resources`, `importlib.metadata`, `argparse`), docker compose v2, helm v3, kind, GitHub Actions, GHCR.

**Spec:** `docs/designs/deploy_scaffold.md`

## Global Constraints

- ALL imports at top of file, three groups (stdlib / external / current package). No inline imports except the existing last-resort circular-dep pattern.
- No `__all__` in `__init__.py`; no `Any` typing shortcuts; `Literal` over enums for string sets.
- Tests: flat module-level functions next to the module under test; no test classes; no `@pytest.mark.asyncio`; don't test plain assignment/defaults. E2E suites live in `test_e2e/<suite>/`.
- No history comments (`# Removed: ...`).
- Docs in subdirectories follow the markdown-style skill (setext title, `#` sections, aligned tables, implementation refs by symbol name, not line number).
- GHCR namespace is `ghcr.io/kolodkin`; release tags match `^v[0-9]+\.[0-9]+\.[0-9]+$`.
- Image/credential defaults must match existing CI: ClickHouse `default`/`click123` on 8123/9000, Postgres `aaiclick`/`secret`/db `aaiclick` on 5432, registry on 5000, server on 5255.
- Commit after every green test cycle. Do NOT create a PR unless asked.

---

### Task 1: `aaiclick/deploy` package — compose template + `compose_scaffold.py`

**Files:**
- Create: `aaiclick/deploy/__init__.py`
- Create: `aaiclick/deploy/compose_scaffold.py`
- Create: `aaiclick/deploy/templates/compose/docker-compose.yaml` (no `__init__.py` anywhere under `templates/` — it is package data, not a package)
- Modify: `pyproject.toml` (`[tool.setuptools.package-data]`)
- Test: `aaiclick/deploy/test_compose_scaffold.py`

**Interfaces:**
- Produces: `init_compose(target: Path, *, image_tag: str | None = None, force: bool = False) -> Path`; `ComposeFileExists(FileExistsError)`; `default_image_tag() -> str` (returns `f"v{importlib.metadata.version('aaiclick')}"`); token constant `IMAGE_TAG_TOKEN = "__AAICLICK_IMAGE_TAG__"`.
- Consumes: nothing.

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/deploy/test_compose_scaffold.py`:

```python
"""Tests for the docker-compose scaffold command."""

from __future__ import annotations

import pytest
import yaml

from .compose_scaffold import (
    IMAGE_TAG_TOKEN,
    ComposeFileExists,
    default_image_tag,
    init_compose,
)

EXPECTED_SERVICES = {"clickhouse", "postgres", "registry", "migrate", "server", "worker", "background"}


def test_init_compose_writes_rendered_file(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    written = init_compose(target, image_tag="v9.9.9")
    assert written == target.resolve()
    content = target.read_text()
    assert IMAGE_TAG_TOKEN not in content
    assert "ghcr.io/kolodkin/aaiclick:v9.9.9" in content
    assert "ghcr.io/kolodkin/aaiclick-docker:v9.9.9" in content


def test_init_compose_output_is_valid_yaml_with_expected_services(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    init_compose(target, image_tag="v1.0.0")
    parsed = yaml.safe_load(target.read_text())
    assert set(parsed["services"]) == EXPECTED_SERVICES
    assert "/var/run/docker.sock:/var/run/docker.sock" in parsed["services"]["worker"]["volumes"]


def test_init_compose_defaults_tag_to_installed_version(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    init_compose(target)
    assert f"ghcr.io/kolodkin/aaiclick:{default_image_tag()}" in target.read_text()


def test_init_compose_refuses_overwrite(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    target.write_text("# user's own compose file\n")

    with pytest.raises(ComposeFileExists, match="already exists"):
        init_compose(target)

    assert target.read_text() == "# user's own compose file\n"


def test_init_compose_force_overwrites(tmp_path):
    target = tmp_path / "docker-compose.yaml"
    target.write_text("# stale\n")

    init_compose(target, image_tag="v1.0.0", force=True)

    assert "ghcr.io/kolodkin/aaiclick:v1.0.0" in target.read_text()
```

`yaml` is already a transitive dependency (`pyyaml`) — verify with `uv run python -c "import yaml"`. If it is not importable, add `pyyaml` to the `test` extra in `pyproject.toml`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/deploy/test_compose_scaffold.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'aaiclick.deploy'`

- [ ] **Step 3: Create the package and template**

Create `aaiclick/deploy/__init__.py`:

```python
from .compose_scaffold import ComposeFileExists, default_image_tag, init_compose
```

Create `aaiclick/deploy/templates/compose/docker-compose.yaml` (exact content):

```yaml
# Starter docker-compose stack for an aaiclick docker-runner deployment.
#
# Scaffolded by `python -m aaiclick compose init` — customize freely; you
# own this file. Credentials below are starter defaults: change them for
# anything beyond local evaluation.
#
# Task containers spawned by the docker runner run as SIBLINGS on the host
# docker daemon (the worker mounts /var/run/docker.sock). They cannot
# resolve compose service names, so everything a task needs (ClickHouse,
# Postgres, the registry) is published on host ports and addressed via
# host.docker.internal in the worker's environment.

name: aaiclick

services:
  clickhouse:
    image: clickhouse/clickhouse-server:26.3
    environment:
      CLICKHOUSE_USER: default
      CLICKHOUSE_PASSWORD: click123
      CLICKHOUSE_DB: default
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 0
    ports:
      - "8123:8123"
      - "9000:9000"
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "localhost:8123/ping"]
      interval: 5s
      timeout: 3s
      retries: 10

  postgres:
    image: postgres:18.3
    environment:
      POSTGRES_USER: aaiclick
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: aaiclick
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "aaiclick"]
      interval: 5s
      timeout: 3s
      retries: 10

  registry:
    image: registry:2
    ports:
      - "5000:5000"

  migrate:
    image: ghcr.io/kolodkin/aaiclick:__AAICLICK_IMAGE_TAG__
    command: python -m aaiclick migrate upgrade head
    environment:
      AAICLICK_SQL_URL: postgresql+asyncpg://aaiclick:secret@postgres:5432/aaiclick
    depends_on:
      postgres:
        condition: service_healthy
    restart: "no"

  server:
    image: ghcr.io/kolodkin/aaiclick:__AAICLICK_IMAGE_TAG__
    ports:
      - "5255:5255"
    environment:
      AAICLICK_SQL_URL: postgresql+asyncpg://aaiclick:secret@postgres:5432/aaiclick
      AAICLICK_CH_URL: clickhouse://default:click123@clickhouse:8123/default
    depends_on:
      migrate:
        condition: service_completed_successfully
      clickhouse:
        condition: service_healthy

  worker:
    image: ghcr.io/kolodkin/aaiclick-docker:__AAICLICK_IMAGE_TAG__
    command: python -m aaiclick execution-worker start
    # Root so the mounted docker socket is writable. Alternative: keep the
    # image's non-root user and add `group_add: ["<host docker gid>"]`.
    user: root
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    extra_hosts:
      - "host.docker.internal:host-gateway"
    environment:
      # host.docker.internal DSNs resolve BOTH here (extra_hosts above) and
      # inside spawned task containers (AAICLICK_DOCKER_RUN_ADD_HOST below),
      # so tasks inherit working connection strings verbatim.
      AAICLICK_SQL_URL: postgresql+asyncpg://aaiclick:secret@host.docker.internal:5432/aaiclick
      AAICLICK_CH_URL: clickhouse://default:click123@host.docker.internal:8123/default
      AAICLICK_REGISTRY: host.docker.internal:5000
      AAICLICK_DOCKER_BUILD_ADD_HOST: host.docker.internal:host-gateway
      AAICLICK_DOCKER_RUN_ADD_HOST: host.docker.internal:host-gateway
    depends_on:
      migrate:
        condition: service_completed_successfully
      clickhouse:
        condition: service_healthy

  background:
    image: ghcr.io/kolodkin/aaiclick:__AAICLICK_IMAGE_TAG__
    command: python -m aaiclick background start
    environment:
      AAICLICK_SQL_URL: postgresql+asyncpg://aaiclick:secret@postgres:5432/aaiclick
      AAICLICK_CH_URL: clickhouse://default:click123@clickhouse:8123/default
    depends_on:
      migrate:
        condition: service_completed_successfully

volumes:
  clickhouse_data:
  postgres_data:
```

Create `aaiclick/deploy/compose_scaffold.py`:

```python
"""Scaffold a starter docker-compose stack for docker-runner deployments.

Mirrors ``docker_scaffold.py``: the framework writes a sensible starter into
the user's directory and the user owns it from there. The template ships as
package data (``templates/compose/docker-compose.yaml``) and is rendered on
write by replacing the image-tag token with the installed aaiclick version,
so the scaffold pins to the GHCR images matching the wheel the user
installed.

Invoked via ``python -m aaiclick compose init``."""

from __future__ import annotations

from importlib.metadata import version
from importlib.resources import files
from pathlib import Path

IMAGE_TAG_TOKEN = "__AAICLICK_IMAGE_TAG__"

_TEMPLATES = files("aaiclick.deploy") / "templates"


class ComposeFileExists(FileExistsError):
    """Raised when the target path already exists and ``force`` is False."""


def default_image_tag() -> str:
    """GHCR image tag matching the installed aaiclick version."""
    return f"v{version('aaiclick')}"


def init_compose(target: Path, *, image_tag: str | None = None, force: bool = False) -> Path:
    """Write the starter docker-compose file to ``target``.

    Returns the resolved target path. Raises :class:`ComposeFileExists`
    when the file already exists and ``force`` is False."""
    if target.exists() and not force:
        raise ComposeFileExists(f"{target} already exists. Pass --force to overwrite.")
    template = (_TEMPLATES / "compose" / "docker-compose.yaml").read_text()
    target.write_text(template.replace(IMAGE_TAG_TOKEN, image_tag or default_image_tag()))
    return target.resolve()
```

Add to `pyproject.toml` under `[tool.setuptools.package-data]` (after the `"aaiclick.server"` line):

```toml
"aaiclick.deploy" = ["templates/**/*"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/deploy/test_compose_scaffold.py -v`
Expected: 5 PASS

- [ ] **Step 5: Verify the template ships in the wheel**

```bash
uv build && unzip -l dist/aaiclick-*.whl | grep "deploy/templates/compose/docker-compose.yaml"
```

Expected: the path is listed. Then `rm -rf dist/`.

- [ ] **Step 6: Commit**

```bash
git add aaiclick/deploy/ pyproject.toml
git commit -m "Add compose scaffold: aaiclick/deploy package with docker-runner template"
```

---

### Task 2: helm chart template + `k8s_scaffold.py`

**Files:**
- Create: `aaiclick/deploy/k8s_scaffold.py`
- Create: `aaiclick/deploy/templates/helm/aaiclick/Chart.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/values.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/templates/server.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/templates/worker.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/templates/migrate-job.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/templates/rbac.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/templates/clickhouse.yaml`
- Create: `aaiclick/deploy/templates/helm/aaiclick/templates/postgres.yaml`
- Modify: `aaiclick/deploy/__init__.py`
- Test: `aaiclick/deploy/test_k8s_scaffold.py`

**Interfaces:**
- Consumes: `IMAGE_TAG_TOKEN`, `default_image_tag` from `compose_scaffold` (Task 1).
- Produces: `init_helm(target_dir: Path, *, image_tag: str | None = None, force: bool = False) -> Path`; `HelmChartExists(FileExistsError)`.

- [ ] **Step 1: Write the failing tests**

Create `aaiclick/deploy/test_k8s_scaffold.py`:

```python
"""Tests for the helm chart scaffold command."""

from __future__ import annotations

import pytest
import yaml

from .compose_scaffold import IMAGE_TAG_TOKEN
from .k8s_scaffold import HelmChartExists, init_helm

EXPECTED_TEMPLATES = {
    "server.yaml",
    "worker.yaml",
    "migrate-job.yaml",
    "rbac.yaml",
    "clickhouse.yaml",
    "postgres.yaml",
}


def test_init_helm_writes_chart_tree(tmp_path):
    target = tmp_path / "aaiclick-chart"
    written = init_helm(target, image_tag="v9.9.9")
    assert written == target.resolve()
    assert (target / "Chart.yaml").is_file()
    assert (target / "values.yaml").is_file()
    assert {p.name for p in (target / "templates").iterdir()} == EXPECTED_TEMPLATES


def test_init_helm_renders_version_into_chart_and_values(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v9.9.9")

    chart = yaml.safe_load((target / "Chart.yaml").read_text())
    assert chart["appVersion"] == "v9.9.9"

    values = yaml.safe_load((target / "values.yaml").read_text())
    assert values["images"]["server"]["tag"] == "v9.9.9"
    assert values["images"]["worker"]["tag"] == "v9.9.9"
    assert values["images"]["worker"]["repository"] == "ghcr.io/kolodkin/aaiclick-kubectl"


def test_init_helm_leaves_no_token_anywhere(tmp_path):
    target = tmp_path / "aaiclick-chart"
    init_helm(target, image_tag="v1.0.0")
    for path in target.rglob("*.yaml"):
        assert IMAGE_TAG_TOKEN not in path.read_text(), path


def test_init_helm_refuses_overwrite(tmp_path):
    target = tmp_path / "aaiclick-chart"
    target.mkdir()
    (target / "Chart.yaml").write_text("# user's own chart\n")

    with pytest.raises(HelmChartExists, match="already exists"):
        init_helm(target)

    assert (target / "Chart.yaml").read_text() == "# user's own chart\n"


def test_init_helm_force_overwrites(tmp_path):
    target = tmp_path / "aaiclick-chart"
    target.mkdir()
    (target / "Chart.yaml").write_text("# stale\n")

    init_helm(target, image_tag="v1.0.0", force=True)

    chart = yaml.safe_load((target / "Chart.yaml").read_text())
    assert chart["name"] == "aaiclick"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/deploy/test_k8s_scaffold.py -v`
Expected: FAIL — `ImportError: cannot import name 'HelmChartExists'`

- [ ] **Step 3: Create the chart templates**

Create `aaiclick/deploy/templates/helm/aaiclick/Chart.yaml`:

```yaml
apiVersion: v2
name: aaiclick
description: >-
  aaiclick API server, kubernetes-runner execution worker, migrations, and
  optional in-cluster ClickHouse/Postgres for evaluation. Scaffolded by
  `python -m aaiclick k8s init` — customize freely; you own this chart.
type: application
version: 0.1.0
appVersion: "__AAICLICK_IMAGE_TAG__"
```

Create `aaiclick/deploy/templates/helm/aaiclick/values.yaml`:

```yaml
images:
  server:
    repository: ghcr.io/kolodkin/aaiclick
    tag: __AAICLICK_IMAGE_TAG__
  worker:
    repository: ghcr.io/kolodkin/aaiclick-kubectl
    tag: __AAICLICK_IMAGE_TAG__
imagePullSecrets: []

# Connection URLs consumed by every aaiclick component. The defaults point
# at the in-cluster dev dependencies below; production installs override
# them (ideally via --set-file / an external secret manager).
env:
  sqlUrl: postgresql+asyncpg://aaiclick:secret@aaiclick-postgres:5432/aaiclick
  chUrl: clickhouse://default:click123@aaiclick-clickhouse:8123/default
  # Registry the docker/kubernetes runner pushes task images to. Empty
  # disables the env var (subprocess and prebuilt-image jobs need none).
  registry: ""

serviceAccount:
  name: aaiclick-worker
rbac:
  create: true

server:
  replicas: 1
  service:
    type: ClusterIP
    port: 5255
  resources: {}

worker:
  replicas: 1
  resources: {}

# Deploy in-cluster ClickHouse + Postgres. Evaluation only — ephemeral,
# no persistence. Disable and point env.* at managed databases for real use.
devDependencies:
  enabled: false
  postgres:
    image: postgres:18.3
    user: aaiclick
    password: secret
    db: aaiclick
  clickhouse:
    image: clickhouse/clickhouse-server:26.3
    user: default
    password: click123
```

Create `aaiclick/deploy/templates/helm/aaiclick/templates/server.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aaiclick-server
  labels:
    app: aaiclick-server
spec:
  replicas: {{ .Values.server.replicas }}
  selector:
    matchLabels:
      app: aaiclick-server
  template:
    metadata:
      labels:
        app: aaiclick-server
    spec:
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: server
          image: "{{ .Values.images.server.repository }}:{{ .Values.images.server.tag }}"
          ports:
            - containerPort: 5255
          env:
            - name: AAICLICK_SQL_URL
              value: {{ .Values.env.sqlUrl | quote }}
            - name: AAICLICK_CH_URL
              value: {{ .Values.env.chUrl | quote }}
          readinessProbe:
            httpGet:
              path: /health
              port: 5255
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 12
          resources:
            {{- toYaml .Values.server.resources | nindent 12 }}
---
apiVersion: v1
kind: Service
metadata:
  name: aaiclick-server
  labels:
    app: aaiclick-server
spec:
  type: {{ .Values.server.service.type }}
  selector:
    app: aaiclick-server
  ports:
    - name: http
      port: {{ .Values.server.service.port }}
      targetPort: 5255
```

Create `aaiclick/deploy/templates/helm/aaiclick/templates/worker.yaml`:

```yaml
# Execution worker. The aaiclick-kubectl image carries kubectl for the
# kubernetes runner (task pods created via the ServiceAccount below) and
# runs subprocess-runner tasks in-place. Building task images from git
# requires a reachable docker daemon (DOCKER_HOST) — not provided here;
# use prebuilt-image jobs in-cluster.
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aaiclick-worker
  labels:
    app: aaiclick-worker
spec:
  replicas: {{ .Values.worker.replicas }}
  selector:
    matchLabels:
      app: aaiclick-worker
  template:
    metadata:
      labels:
        app: aaiclick-worker
    spec:
      serviceAccountName: {{ .Values.serviceAccount.name }}
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: worker
          image: "{{ .Values.images.worker.repository }}:{{ .Values.images.worker.tag }}"
          command: ["python", "-m", "aaiclick", "execution-worker", "start"]
          env:
            - name: AAICLICK_SQL_URL
              value: {{ .Values.env.sqlUrl | quote }}
            - name: AAICLICK_CH_URL
              value: {{ .Values.env.chUrl | quote }}
            - name: AAICLICK_K8S_NAMESPACE
              value: {{ .Release.Namespace | quote }}
            - name: AAICLICK_K8S_SERVICE_ACCOUNT
              value: {{ .Values.serviceAccount.name | quote }}
            {{- if .Values.env.registry }}
            - name: AAICLICK_REGISTRY
              value: {{ .Values.env.registry | quote }}
            {{- end }}
          resources:
            {{- toYaml .Values.worker.resources | nindent 12 }}
```

Create `aaiclick/deploy/templates/helm/aaiclick/templates/migrate-job.yaml`:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: aaiclick-migrate
  annotations:
    "helm.sh/hook": pre-install,pre-upgrade
    "helm.sh/hook-weight": "1"
    "helm.sh/hook-delete-policy": before-hook-creation,hook-succeeded
spec:
  backoffLimit: 4
  template:
    metadata:
      labels:
        app: aaiclick-migrate
    spec:
      restartPolicy: OnFailure
      {{- with .Values.imagePullSecrets }}
      imagePullSecrets:
        {{- toYaml . | nindent 8 }}
      {{- end }}
      containers:
        - name: migrate
          image: "{{ .Values.images.server.repository }}:{{ .Values.images.server.tag }}"
          command: ["python", "-m", "aaiclick", "migrate", "upgrade", "head"]
          env:
            - name: AAICLICK_SQL_URL
              value: {{ .Values.env.sqlUrl | quote }}
```

Create `aaiclick/deploy/templates/helm/aaiclick/templates/rbac.yaml`:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ .Values.serviceAccount.name }}
{{- if .Values.rbac.create }}
---
# The kubernetes runner creates one pod per task via kubectl and reads its
# logs, so the worker needs exactly these verbs in its own namespace.
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: aaiclick-worker
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: [""]
    resources: ["pods/log"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: aaiclick-worker
subjects:
  - kind: ServiceAccount
    name: {{ .Values.serviceAccount.name }}
roleRef:
  kind: Role
  name: aaiclick-worker
  apiGroup: rbac.authorization.k8s.io
{{- end }}
```

Create `aaiclick/deploy/templates/helm/aaiclick/templates/clickhouse.yaml`:

```yaml
{{- if .Values.devDependencies.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aaiclick-clickhouse
  labels:
    app: aaiclick-clickhouse
spec:
  replicas: 1
  selector:
    matchLabels:
      app: aaiclick-clickhouse
  template:
    metadata:
      labels:
        app: aaiclick-clickhouse
    spec:
      containers:
        - name: clickhouse
          image: {{ .Values.devDependencies.clickhouse.image }}
          env:
            - name: CLICKHOUSE_USER
              value: {{ .Values.devDependencies.clickhouse.user | quote }}
            - name: CLICKHOUSE_PASSWORD
              value: {{ .Values.devDependencies.clickhouse.password | quote }}
            - name: CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT
              value: "0"
          ports:
            - containerPort: 8123
            - containerPort: 9000
          readinessProbe:
            httpGet:
              path: /ping
              port: 8123
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 12
---
apiVersion: v1
kind: Service
metadata:
  name: aaiclick-clickhouse
  labels:
    app: aaiclick-clickhouse
spec:
  selector:
    app: aaiclick-clickhouse
  ports:
    - name: http
      port: 8123
      targetPort: 8123
    - name: native
      port: 9000
      targetPort: 9000
{{- end }}
```

Create `aaiclick/deploy/templates/helm/aaiclick/templates/postgres.yaml`:

```yaml
{{- if .Values.devDependencies.enabled }}
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aaiclick-postgres
  labels:
    app: aaiclick-postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: aaiclick-postgres
  template:
    metadata:
      labels:
        app: aaiclick-postgres
    spec:
      containers:
        - name: postgres
          image: {{ .Values.devDependencies.postgres.image }}
          env:
            - name: POSTGRES_USER
              value: {{ .Values.devDependencies.postgres.user | quote }}
            - name: POSTGRES_PASSWORD
              value: {{ .Values.devDependencies.postgres.password | quote }}
            - name: POSTGRES_DB
              value: {{ .Values.devDependencies.postgres.db | quote }}
          ports:
            - containerPort: 5432
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "{{ .Values.devDependencies.postgres.user }}"]
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 12
---
apiVersion: v1
kind: Service
metadata:
  name: aaiclick-postgres
  labels:
    app: aaiclick-postgres
spec:
  selector:
    app: aaiclick-postgres
  ports:
    - name: pg
      port: 5432
      targetPort: 5432
{{- end }}
```

- [ ] **Step 4: Create `k8s_scaffold.py`**

```python
"""Scaffold a starter helm chart for kubernetes deployments.

Mirrors ``compose_scaffold.py``: the chart ships as package data
(``templates/helm/aaiclick``) and every file is rendered on write by
replacing the image-tag token, so ``Chart.yaml``'s ``appVersion`` and the
``values.yaml`` image tags pin to the installed aaiclick version.

Invoked via ``python -m aaiclick k8s init``."""

from __future__ import annotations

from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path

from .compose_scaffold import IMAGE_TAG_TOKEN, default_image_tag

_CHART_ROOT = files("aaiclick.deploy") / "templates" / "helm" / "aaiclick"


class HelmChartExists(FileExistsError):
    """Raised when the target directory already exists and ``force`` is False."""


def _render_tree(src: Traversable, dst: Path, image_tag: str) -> None:
    dst.mkdir(parents=True, exist_ok=True)
    for entry in src.iterdir():
        if entry.is_dir():
            _render_tree(entry, dst / entry.name, image_tag)
        else:
            (dst / entry.name).write_text(entry.read_text().replace(IMAGE_TAG_TOKEN, image_tag))


def init_helm(target_dir: Path, *, image_tag: str | None = None, force: bool = False) -> Path:
    """Write the starter helm chart into ``target_dir``.

    Returns the resolved target path. Raises :class:`HelmChartExists`
    when the directory already exists and ``force`` is False."""
    if target_dir.exists() and not force:
        raise HelmChartExists(f"{target_dir} already exists. Pass --force to overwrite.")
    _render_tree(_CHART_ROOT, target_dir, image_tag or default_image_tag())
    return target_dir.resolve()
```

If `importlib.resources.abc` is unavailable on Python 3.10 (it landed in 3.11), import `Traversable` from `importlib.abc` instead — verify with `uv run --python 3.10 python -c "from importlib.abc import Traversable"`.

Update `aaiclick/deploy/__init__.py`:

```python
from .compose_scaffold import ComposeFileExists, default_image_tag, init_compose
from .k8s_scaffold import HelmChartExists, init_helm
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `uv run pytest aaiclick/deploy/ -v`
Expected: 10 PASS (both scaffold suites)

- [ ] **Step 6: Lint the chart if helm is available locally**

```bash
command -v helm >/dev/null && { d=$(mktemp -d); uv run python -c "
from pathlib import Path
from aaiclick.deploy import init_helm
init_helm(Path('$d/chart'), image_tag='v0.0.0')
"; helm lint "$d/chart"; } || echo "helm not installed locally — chart is linted in the helm e2e gate"
```

Expected: `1 chart(s) linted, 0 chart(s) failed` (or the skip message).

- [ ] **Step 7: Commit**

```bash
git add aaiclick/deploy/
git commit -m "Add helm chart scaffold: k8s_scaffold.py with user-facing chart template"
```

---

### Task 3: CLI wiring — `compose init` and `k8s init`

**Files:**
- Modify: `aaiclick/__main__.py` (usage epilog near line 27, handlers near `_run_docker_init`, parsers after the `docker` subcommand block, dispatch after `args.command == "docker"`)
- Test: `aaiclick/deploy/test_compose_scaffold.py`, `aaiclick/deploy/test_k8s_scaffold.py` (append CLI round-trip tests)

**Interfaces:**
- Consumes: `init_compose`, `init_helm`, `ComposeFileExists`, `HelmChartExists` from `aaiclick.deploy` (Tasks 1–2).
- Produces: `python -m aaiclick compose init [--path PATH] [--image-tag TAG] [--force]` and `python -m aaiclick k8s init [--path PATH] [--image-tag TAG] [--force]`; handlers `_run_compose_init(args)`, `_run_k8s_init(args)`.

- [ ] **Step 1: Write the failing CLI tests**

Append to `aaiclick/deploy/test_compose_scaffold.py` (add `import subprocess`, `import sys` to the stdlib import group):

```python
def test_cli_compose_init_writes_file(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "aaiclick", "compose", "init", "--path", "stack.yaml", "--image-tag", "v1.2.3"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Wrote" in result.stdout
    assert "ghcr.io/kolodkin/aaiclick:v1.2.3" in (tmp_path / "stack.yaml").read_text()


def test_cli_compose_init_existing_file_exits_nonzero(tmp_path):
    (tmp_path / "docker-compose.yaml").write_text("# mine\n")
    result = subprocess.run(
        [sys.executable, "-m", "aaiclick", "compose", "init"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "already exists" in result.stderr
```

Append to `aaiclick/deploy/test_k8s_scaffold.py` (same imports):

```python
def test_cli_k8s_init_writes_chart(tmp_path):
    result = subprocess.run(
        [sys.executable, "-m", "aaiclick", "k8s", "init", "--path", "chart", "--image-tag", "v1.2.3"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert (tmp_path / "chart" / "Chart.yaml").is_file()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest aaiclick/deploy/ -k cli -v`
Expected: FAIL — argparse error `invalid choice: 'compose'` surfaces as non-zero returncode with different stderr / missing file.

- [ ] **Step 3: Wire the CLI**

In `aaiclick/__main__.py`:

(a) Usage epilog — after the `docker init` example line, add:

```
    python -m aaiclick compose init             # Scaffold a docker-runner compose stack
    python -m aaiclick k8s init                 # Scaffold a helm chart
```

(b) Handlers — directly below `_run_docker_init`:

```python
def _run_compose_init(args: argparse.Namespace) -> None:
    from pathlib import Path

    from aaiclick.deploy import ComposeFileExists, init_compose

    target = Path(args.path)
    try:
        written = init_compose(target, image_tag=args.image_tag, force=args.force)
    except ComposeFileExists as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    print(f"Wrote {written}")


def _run_k8s_init(args: argparse.Namespace) -> None:
    from pathlib import Path

    from aaiclick.deploy import HelmChartExists, init_helm

    target = Path(args.path)
    try:
        written = init_helm(target, image_tag=args.image_tag, force=args.force)
    except HelmChartExists as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
    print(f"Wrote {written}")
```

(`_run_docker_init` already uses this deferred-import shape; keep the file's local convention.)

(c) Parsers — directly after the `docker init` parser block:

```python
    # Add compose subcommand
    compose_parser = subparsers.add_parser(
        "compose",
        help="Docker-compose deployment helpers",
    )
    compose_subparsers = compose_parser.add_subparsers(
        dest="compose_command",
        help="Compose commands",
    )

    # compose init
    compose_init_parser = compose_subparsers.add_parser(
        "init",
        help="Scaffold a docker-runner compose stack in the current directory",
    )
    compose_init_parser.add_argument(
        "--path",
        default="docker-compose.yaml",
        help="Output path (default: ./docker-compose.yaml)",
    )
    compose_init_parser.add_argument(
        "--image-tag",
        default=None,
        help="GHCR image tag to pin (default: v<installed aaiclick version>)",
    )
    compose_init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing file",
    )

    # Add k8s subcommand
    k8s_parser = subparsers.add_parser(
        "k8s",
        help="Kubernetes deployment helpers",
    )
    k8s_subparsers = k8s_parser.add_subparsers(
        dest="k8s_command",
        help="Kubernetes commands",
    )

    # k8s init
    k8s_init_parser = k8s_subparsers.add_parser(
        "init",
        help="Scaffold a helm chart in the current directory",
    )
    k8s_init_parser.add_argument(
        "--path",
        default="aaiclick-chart",
        help="Output directory (default: ./aaiclick-chart)",
    )
    k8s_init_parser.add_argument(
        "--image-tag",
        default=None,
        help="GHCR image tag to pin (default: v<installed aaiclick version>)",
    )
    k8s_init_parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing directory",
    )
```

(d) Dispatch — after the `elif args.command == "docker":` block:

```python
    elif args.command == "compose":
        if args.compose_command == "init":
            _run_compose_init(args)
        else:
            subcommands["compose"].print_help()

    elif args.command == "k8s":
        if args.k8s_command == "init":
            _run_k8s_init(args)
        else:
            subcommands["k8s"].print_help()
```

Check how `subcommands` is populated for `docker` (a dict near the parser definitions) and register `compose` and `k8s` the same way.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest aaiclick/deploy/ -v && uv run pytest aaiclick/test_cli.py -v`
Expected: all PASS (test_cli.py guards against regressions in the parser).

- [ ] **Step 5: Commit**

```bash
git add aaiclick/__main__.py aaiclick/deploy/
git commit -m "Wire compose init and k8s init CLI commands"
```

---

### Task 4: compose stack e2e test suite

**Files:**
- Create: `test_e2e/compose/conftest.py`
- Create: `test_e2e/compose/test_stack_e2e.py`

**Interfaces:**
- Consumes: a running scaffolded compose stack (started by the workflow in Task 6); env vars `AAICLICK_E2E_COMPOSE_DIR` (directory containing the scaffolded `docker-compose.yaml`) and optional `AAICLICK_E2E_SERVER_URL` (default `http://localhost:5255`).
- Produces: pytest marker `compose_e2e`; the suite the `_compose-e2e-reusable.yaml` workflow runs.

- [ ] **Step 1: Check how the docker e2e registers its marker**

Read `test_e2e/docker/conftest.py` — mirror its marker-registration mechanism exactly (it is run with `--strict-markers`). If it uses `pytest_configure` + `config.addinivalue_line`, do the same; if the marker comes from `pyproject.toml`, add `compose_e2e` there instead.

- [ ] **Step 2: Write the suite**

Create `test_e2e/compose/conftest.py`:

```python
"""Fixtures for the compose-stack e2e suite.

The suite assumes the scaffolded stack is ALREADY RUNNING — CI scaffolds
via ``python -m aaiclick compose init`` and ``docker compose up -d`` before
invoking pytest (see ``_compose-e2e-reusable.yaml``). Tests drive the stack
from outside: HTTP against the published server port, ``docker compose
exec`` for CLI round-trips."""

from __future__ import annotations

import os
from pathlib import Path

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "compose_e2e: end-to-end tests against the scaffolded docker-compose stack",
    )


@pytest.fixture(scope="session")
def compose_dir() -> Path:
    raw = os.environ.get("AAICLICK_E2E_COMPOSE_DIR")
    if not raw:
        pytest.skip("AAICLICK_E2E_COMPOSE_DIR not set — compose stack not running")
    return Path(raw)


@pytest.fixture(scope="session")
def server_url() -> str:
    return os.environ.get("AAICLICK_E2E_SERVER_URL", "http://localhost:5255")
```

Create `test_e2e/compose/test_stack_e2e.py`:

```python
"""End-to-end smoke of the scaffolded compose stack.

Covers the release-gate contract: the stack the user gets from
``compose init`` comes up healthy, migrations applied, and a job
round-trips through the server container's CLI to the worker container."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.compose_e2e

JOB_TIMEOUT_S = 300


def _compose_exec(compose_dir: Path, service: str, *args: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", service, *args],
        cwd=compose_dir,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"exec {service} {args} failed:\n{result.stdout}\n{result.stderr}"
    return result.stdout


def _job_by_name(compose_dir: Path, name: str) -> dict | None:
    out = _compose_exec(compose_dir, "server", "python", "-m", "aaiclick", "job", "list", "--json")
    data = json.loads(out)
    jobs = data if isinstance(data, list) else data.get("jobs", [])
    matches = [j for j in jobs if j.get("name") == name]
    return matches[0] if matches else None


def test_server_healthy(server_url):
    response = httpx.get(f"{server_url}/health", timeout=10)
    assert response.status_code == 200


def test_worker_registered(compose_dir):
    out = _compose_exec(compose_dir, "server", "python", "-m", "aaiclick", "execution-worker", "list", "--json")
    workers = json.loads(out)
    entries = workers if isinstance(workers, list) else workers.get("workers", [])
    assert entries, "no execution worker registered — worker service did not come up"


def test_shell_job_round_trip(compose_dir):
    _compose_exec(
        compose_dir,
        "server",
        "python", "-m", "aaiclick", "register-job", "aaiclick.testing",
        "--name", "compose-smoke",
        "--runner", "subprocess",
    )
    _compose_exec(
        compose_dir,
        "server",
        "python", "-m", "aaiclick", "run-job", "compose-smoke",
        "--entry-type", "shell",
        "--command", "python -c 'print(42)'",
    )

    deadline = time.monotonic() + JOB_TIMEOUT_S
    while time.monotonic() < deadline:
        job = _job_by_name(compose_dir, "compose-smoke")
        if job and job.get("status") == "COMPLETED":
            return
        if job and job.get("status") in ("FAILED", "CANCELLED"):
            pytest.fail(f"compose-smoke ended {job['status']}: {job}")
        time.sleep(5)
    pytest.fail(f"compose-smoke did not complete within {JOB_TIMEOUT_S}s")
```

Verification note for the implementer: before finalizing, run `uv run python -m aaiclick job list --json` and `execution-worker list --json` against a local backend to confirm the JSON shape (list vs wrapped object, exact status/name keys) and adjust `_job_by_name` / `test_worker_registered` to the actual shape — the defensive both-shapes parsing above is a starting point, not a license to skip checking. Same for `register-job`: confirm `aaiclick.testing` is accepted as an entrypoint for a subprocess job whose runs use `--entry-type shell` (the entrypoint is not imported for shell entries); if registration validates importability in the server container, `aaiclick.testing` ships in the wheel so it resolves.

- [ ] **Step 3: Verify collection (suite skips without a stack)**

Run: `uv run pytest test_e2e/compose/ -m compose_e2e --collect-only -q`
Expected: 3 tests collected, no errors.

Run: `uv run pytest test_e2e/compose/ -m compose_e2e -v`
Expected: 1 pass or fail depending on local server, 2 SKIP (no `AAICLICK_E2E_COMPOSE_DIR`) — the point is no import/collection errors. If `test_server_healthy` fails locally because nothing listens on 5255, that is expected outside CI; gate it on the same env var by moving the URL default behind a skip if desired — simplest: add `pytest.importorskip` is NOT needed; instead make `server_url` also skip when `AAICLICK_E2E_COMPOSE_DIR` is unset:

```python
@pytest.fixture(scope="session")
def server_url(compose_dir) -> str:
    return os.environ.get("AAICLICK_E2E_SERVER_URL", "http://localhost:5255")
```

(depending on `compose_dir` makes every test skip together outside CI — use this version).

- [ ] **Step 4: Commit**

```bash
git add test_e2e/compose/
git commit -m "Add compose stack e2e suite"
```

---

### Task 5: publish.yaml — build-rc-images + merge-rc-images

**Files:**
- Modify: `.github/workflows/publish.yaml`

**Interfaces:**
- Consumes: `dist` artifact from the existing `build` job.
- Produces: GHCR manifests `ghcr.io/kolodkin/{aaiclick,aaiclick-docker,aaiclick-kubectl}:<tag>-rc` (multi-arch) plus per-arch tags `<tag>-rc-amd64` / `<tag>-rc-arm64`. Downstream gates and `promote-images` reference the `-rc` tag.

- [ ] **Step 1: Add the two jobs after `build`**

```yaml
  build-rc-images:
    needs: build
    runs-on: ${{ matrix.runner }}
    timeout-minutes: 45
    permissions:
      contents: read
      packages: write
    strategy:
      fail-fast: true
      matrix:
        include:
          - platform: amd64
            runner: ubuntu-latest
          - platform: arm64
            runner: ubuntu-24.04-arm
    env:
      IMAGE_PREFIX: ghcr.io/kolodkin
    steps:
      - name: Checkout code
        uses: actions/checkout@v5

      - name: Download dist artifacts
        uses: actions/download-artifact@v7
        with:
          name: dist
          path: dist/

      # The images pip-install aaiclick==<version>. The wheel is not on
      # PyPI yet (publish is gated on these very images), so serve the
      # artifact from a local pypiserver; it redirects unknown packages
      # to pypi.org, so dependencies resolve through the same index URL.
      - name: Serve wheel via local pypiserver
        run: |
          docker run -d --name pypiserver -p 8080:8080 \
            pypiserver/pypiserver:v2.3.2 run -p 8080 -a . -P . --server gunicorn
          for i in $(seq 1 30); do
            curl -fsS http://localhost:8080/simple/ >/dev/null 2>&1 && break
            sleep 2
          done
          for f in dist/*.whl dist/*.tar.gz; do
            curl -fsSL -F "content=@${f}" -F ":action=file_upload" http://localhost:8080/
          done

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      # Native per-arch builds (no QEMU). The three images are FROM-chained,
      # so they build sequentially within each platform leg. --network=host
      # lets the build reach the pypiserver on localhost; pip trusts
      # localhost for plain HTTP by default.
      - name: Build & push per-arch rc images
        run: |
          VERSION="${{ inputs.tag }}"
          PEP440="${VERSION#v}"
          ARCH_TAG="${VERSION}-rc-${{ matrix.platform }}"
          docker build --network=host -f docker/Dockerfile \
            --build-arg AAICLICK_VERSION="${PEP440}" \
            --build-arg PIP_INDEX_URL="http://localhost:8080/simple/" \
            -t "${IMAGE_PREFIX}/aaiclick:${ARCH_TAG}" .
          docker build -f docker/docker.Dockerfile \
            --build-arg BASE_REF="${IMAGE_PREFIX}/aaiclick:${ARCH_TAG}" \
            --build-arg TARGETARCH="${{ matrix.platform }}" \
            -t "${IMAGE_PREFIX}/aaiclick-docker:${ARCH_TAG}" .
          docker build -f docker/kubectl.Dockerfile \
            --build-arg BASE_REF="${IMAGE_PREFIX}/aaiclick-docker:${ARCH_TAG}" \
            --build-arg TARGETARCH="${{ matrix.platform }}" \
            -t "${IMAGE_PREFIX}/aaiclick-kubectl:${ARCH_TAG}" .
          docker push "${IMAGE_PREFIX}/aaiclick:${ARCH_TAG}"
          docker push "${IMAGE_PREFIX}/aaiclick-docker:${ARCH_TAG}"
          docker push "${IMAGE_PREFIX}/aaiclick-kubectl:${ARCH_TAG}"

  merge-rc-images:
    needs: build-rc-images
    runs-on: ubuntu-latest
    permissions:
      packages: write
    strategy:
      matrix:
        image: [aaiclick, aaiclick-docker, aaiclick-kubectl]
    steps:
      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Assemble multi-arch rc manifest
        run: |
          REF="ghcr.io/kolodkin/${{ matrix.image }}"
          TAG="${{ inputs.tag }}-rc"
          docker buildx imagetools create -t "${REF}:${TAG}" \
            "${REF}:${TAG}-amd64" "${REF}:${TAG}-arm64"
```

If `ubuntu-24.04-arm` is unavailable for this repo (job stays queued — it is free for public repos only), replace that leg with `runner: ubuntu-latest` plus a `docker/setup-qemu-action@v3` step before the build; the matrix shape stays identical.

- [ ] **Step 2: Validate workflow syntax**

Run: `uv run --with pyyaml python -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yaml'))"`
Expected: no output (valid YAML).

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/publish.yaml
git commit -m "publish: build multi-arch rc images from wheel artifact via local pypiserver"
```

---

### Task 6: `_compose-e2e-reusable.yaml` + gate wiring

**Files:**
- Create: `.github/workflows/_compose-e2e-reusable.yaml`
- Modify: `.github/workflows/publish.yaml` (add `test-compose-e2e` job)

**Interfaces:**
- Consumes: `dist` + `requirements` artifacts; `-rc` GHCR manifests (Task 5); `test_e2e/compose/` suite (Task 4).
- Produces: release gate `test-compose-e2e` referenced by `publish.needs` (Task 9).

- [ ] **Step 1: Create the reusable workflow**

`.github/workflows/_compose-e2e-reusable.yaml`:

```yaml
name: Compose Stack E2E (reusable)

# Release gate for the scaffolded docker-compose stack: installs the wheel
# artifact, scaffolds via `python -m aaiclick compose init` (dogfooding the
# exact artifact users get), points the image tags at the -rc candidates,
# brings the stack up, and runs test_e2e/compose/.

on:
  workflow_call:
    inputs:
      version_pin:
        description: "vX.Y.Z of the wheel under test"
        required: true
        type: string
      image_tag:
        description: "GHCR image tag to deploy (e.g. v1.2.3-rc)"
        required: true
        type: string

jobs:
  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 30
    permissions:
      contents: read
      packages: read

    steps:
      - name: Checkout code
        uses: actions/checkout@v5

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Download dist artifacts
        uses: actions/download-artifact@v7
        with:
          name: dist
          path: dist/

      - name: Download requirements artifacts
        uses: actions/download-artifact@v7
        with:
          name: requirements

      - name: Install pinned dependencies and built wheel
        run: |
          uv venv --python 3.10
          uv pip install -r requirements-dist.txt
          uv pip install --no-deps --no-index --find-links dist/ "aaiclick[distributed]"

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Scaffold compose stack
        run: |
          mkdir -p "$RUNNER_TEMP/stack"
          uv run --no-project python -m aaiclick compose init \
            --path "$RUNNER_TEMP/stack/docker-compose.yaml" \
            --image-tag "${{ inputs.image_tag }}"
          cat "$RUNNER_TEMP/stack/docker-compose.yaml"

      - name: Start stack
        working-directory: ${{ runner.temp }}/stack
        run: docker compose up -d --quiet-pull

      - name: Wait for server health
        run: |
          for i in $(seq 1 60); do
            if curl -fsS http://localhost:5255/health >/dev/null 2>&1; then
              echo "server healthy"; exit 0
            fi
            sleep 5
          done
          echo "server never became healthy" >&2
          exit 1

      - name: Run compose e2e tests
        env:
          VIRTUAL_ENV: ${{ github.workspace }}/.venv
          AAICLICK_E2E_COMPOSE_DIR: ${{ runner.temp }}/stack
        run: |
          mkdir -p tmp
          uv run --no-project pytest test_e2e/compose/ -m compose_e2e -n 0 -v \
            -o asyncio_mode=auto \
            -o asyncio_default_fixture_loop_scope=module \
            -o asyncio_default_test_loop_scope=module \
            -o "filterwarnings=error" \
            --strict-markers \
            --junitxml=tmp/pytest-report.xml

      - name: Publish test results
        uses: dorny/test-reporter@v2
        if: always()
        with:
          name: "Compose Stack E2E"
          path: tmp/pytest-report.xml
          reporter: java-junit

      - name: Dump compose logs on failure
        if: failure()
        working-directory: ${{ runner.temp }}/stack
        run: docker compose logs --tail 200

      - name: Tear down stack
        if: always()
        working-directory: ${{ runner.temp }}/stack
        run: docker compose down -v
```

- [ ] **Step 2: Wire into publish.yaml**

Add after `test-package-docker-e2e`:

```yaml
  test-compose-e2e:
    needs: [build, merge-rc-images]
    uses: ./.github/workflows/_compose-e2e-reusable.yaml
    with:
      version_pin: ${{ inputs.tag }}
      image_tag: ${{ inputs.tag }}-rc
```

- [ ] **Step 3: Validate YAML, commit**

```bash
uv run --with pyyaml python -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/publish.yaml', '.github/workflows/_compose-e2e-reusable.yaml']]"
git add .github/workflows/
git commit -m "publish: gate release on compose stack e2e against rc images"
```

---

### Task 7: test-package on compose-provided infra

**Files:**
- Modify: `.github/workflows/publish.yaml` (`test-package` job)

**Interfaces:**
- Consumes: `-rc` manifests (Task 5); `compose init` CLI (Task 3).
- Produces: `test-package` gate running against the scaffolded stack instead of GHA `services:` blocks.

- [ ] **Step 1: Rework the job**

In `test-package`: change `needs: build` → `needs: [build, merge-rc-images]`, delete the entire `services:` block, and insert these steps after "Install pinned dependencies and built wheel" (before "Run database migrations"):

```yaml
      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      # Infra comes from the same compose file users scaffold — the release
      # dogfoods it instead of GHA `services:` blocks. The whole stack comes
      # up (no profiles); the extra app services are harmless to the suite.
      - name: Scaffold and start compose stack
        run: |
          mkdir -p "$RUNNER_TEMP/stack"
          uv run --no-project python -m aaiclick compose init \
            --path "$RUNNER_TEMP/stack/docker-compose.yaml" \
            --image-tag "${{ inputs.tag }}-rc"
          docker compose -f "$RUNNER_TEMP/stack/docker-compose.yaml" up -d --quiet-pull

      - name: Wait for infra health
        run: |
          for i in $(seq 1 60); do
            if curl -fsS http://localhost:8123/ping >/dev/null 2>&1 \
               && docker compose -f "$RUNNER_TEMP/stack/docker-compose.yaml" exec -T postgres pg_isready -U aaiclick >/dev/null 2>&1; then
              echo "infra healthy"; exit 0
            fi
            sleep 5
          done
          echo "infra never became healthy" >&2
          exit 1
```

Add a permissions block to the job (`contents: read`, `packages: read`). Keep migrations, env, and the pytest step unchanged — the scaffold's credentials/ports match the old `services:` values by design. Add teardown at the end:

```yaml
      - name: Tear down stack
        if: always()
        run: docker compose -f "$RUNNER_TEMP/stack/docker-compose.yaml" down -v
```

Note the known risk (accepted in the spec): the stack's live worker polls the same Postgres the suite writes to. If a suite test registers tasks the compose worker claims, tests get flaky — the fix is test-side isolation (separate database), not compose changes. Watch the first release run.

- [ ] **Step 2: Validate YAML, commit**

```bash
uv run --with pyyaml python -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yaml'))"
git add .github/workflows/publish.yaml
git commit -m "publish: test-package infra via scaffolded compose instead of services blocks"
```

---

### Task 8: `_helm-e2e-reusable.yaml` + gate wiring

**Files:**
- Create: `.github/workflows/_helm-e2e-reusable.yaml`
- Modify: `.github/workflows/publish.yaml` (add `test-helm-e2e` job)

**Interfaces:**
- Consumes: `dist` + `requirements` artifacts; `-rc` manifests; `k8s init` CLI (Task 3).
- Produces: release gate `test-helm-e2e` referenced by `publish.needs` (Task 9).

- [ ] **Step 1: Create the reusable workflow**

`.github/workflows/_helm-e2e-reusable.yaml`:

```yaml
name: Helm Chart E2E (reusable)

# Release gate for the scaffolded helm chart: installs the wheel artifact,
# scaffolds via `python -m aaiclick k8s init`, lints the chart, installs it
# on a kind cluster with devDependencies enabled and the -rc images, then
# smoke-tests server health and a shell-job round trip.

on:
  workflow_call:
    inputs:
      version_pin:
        description: "vX.Y.Z of the wheel under test"
        required: true
        type: string
      image_tag:
        description: "GHCR image tag to deploy (e.g. v1.2.3-rc)"
        required: true
        type: string

jobs:
  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 45
    permissions:
      contents: read
      packages: read

    steps:
      - name: Checkout code
        uses: actions/checkout@v5

      - name: Install uv
        uses: astral-sh/setup-uv@v7

      - name: Install kind + kubectl
        run: |
          curl -Lo kind https://kind.sigs.k8s.io/dl/v0.27.0/kind-linux-amd64
          sudo install kind /usr/local/bin/
          curl -Lo kubectl "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
          sudo install kubectl /usr/local/bin/

      - name: Install Helm
        uses: azure/setup-helm@v4
        with:
          version: v3.16.2

      - name: Download dist artifacts
        uses: actions/download-artifact@v7
        with:
          name: dist
          path: dist/

      - name: Download requirements artifacts
        uses: actions/download-artifact@v7
        with:
          name: requirements

      - name: Install pinned dependencies and built wheel
        run: |
          uv venv --python 3.10
          uv pip install -r requirements-dist.txt
          uv pip install --no-deps --no-index --find-links dist/ "aaiclick[distributed]"

      - name: Scaffold helm chart
        run: |
          uv run --no-project python -m aaiclick k8s init \
            --path "$RUNNER_TEMP/chart" \
            --image-tag "${{ inputs.image_tag }}"

      - name: Lint chart
        run: |
          helm lint "$RUNNER_TEMP/chart"
          helm template smoke "$RUNNER_TEMP/chart" --set devDependencies.enabled=true >/dev/null

      - name: Create kind cluster
        run: kind create cluster --name aaiclick-helm-e2e

      - name: Wait for cluster DNS (CoreDNS) ready
        run: kubectl -n kube-system rollout status deploy/coredns --timeout=180s

      - name: Install chart
        run: |
          helm install aaiclick "$RUNNER_TEMP/chart" \
            --set devDependencies.enabled=true \
            --wait --timeout 15m

      - name: Smoke: server health via port-forward
        run: |
          kubectl port-forward svc/aaiclick-server 5255:5255 &
          PF_PID=$!
          trap "kill $PF_PID" EXIT
          for i in $(seq 1 30); do
            if curl -fsS http://localhost:5255/health >/dev/null 2>&1; then
              echo "server healthy"; exit 0
            fi
            sleep 2
          done
          echo "server never became healthy" >&2
          exit 1

      - name: Smoke: shell job round trip
        run: |
          kubectl exec deploy/aaiclick-server -- \
            python -m aaiclick register-job aaiclick.testing --name helm-smoke --runner subprocess
          kubectl exec deploy/aaiclick-server -- \
            python -m aaiclick run-job helm-smoke --entry-type shell --command "python -c 'print(42)'"
          for i in $(seq 1 60); do
            STATUS=$(kubectl exec deploy/aaiclick-server -- \
              python -m aaiclick job list --json \
              | python3 -c "import json,sys; d=json.load(sys.stdin); jobs=d if isinstance(d,list) else d.get('jobs',[]); m=[j for j in jobs if j.get('name')=='helm-smoke']; print(m[0]['status'] if m else 'MISSING')")
            echo "helm-smoke status: $STATUS"
            case "$STATUS" in
              COMPLETED) exit 0 ;;
              FAILED|CANCELLED) echo "job ended $STATUS" >&2; exit 1 ;;
            esac
            sleep 5
          done
          echo "job did not complete in time" >&2
          exit 1

      - name: Dump cluster diagnostics on failure
        if: failure()
        run: |
          kubectl get pods -A -o wide || true
          kubectl get events --sort-by=.lastTimestamp || true
          for p in $(kubectl get pods -o name 2>/dev/null); do
            echo "===== describe $p ====="; kubectl describe "$p" || true
            echo "===== logs $p ====="; kubectl logs "$p" --tail=80 || true
          done
```

Same JSON-shape caveat as Task 4: verify `job list --json` output shape before finalizing the status-poll one-liner.

- [ ] **Step 2: Wire into publish.yaml**

```yaml
  test-helm-e2e:
    needs: [build, merge-rc-images]
    uses: ./.github/workflows/_helm-e2e-reusable.yaml
    with:
      version_pin: ${{ inputs.tag }}
      image_tag: ${{ inputs.tag }}-rc
```

- [ ] **Step 3: Validate YAML, commit**

```bash
uv run --with pyyaml python -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/publish.yaml', '.github/workflows/_helm-e2e-reusable.yaml']]"
git add .github/workflows/
git commit -m "publish: gate release on helm chart e2e against rc images on kind"
```

---

### Task 9: promote-images + final gate wiring

**Files:**
- Modify: `.github/workflows/publish.yaml` (`publish` needs/if; replace `publish-image` with `promote-images`)

**Interfaces:**
- Consumes: `-rc` manifests (tested by all gates); successful `publish` job.
- Produces: `ghcr.io/kolodkin/*:vX.Y.Z` (+ `latest` unless pre-release) — byte-identical manifests to the tested `-rc`.

- [ ] **Step 1: Update the publish job's needs and condition**

```yaml
  publish:
    needs: [test-package-local, test-package, test-package-docker-e2e, test-compose-e2e, test-helm-e2e]
    if: >-
      always()
      && needs.test-package-local.result == 'success'
      && needs.test-package.result == 'success'
      && needs.test-compose-e2e.result == 'success'
      && needs.test-helm-e2e.result == 'success'
      && (needs.test-package-docker-e2e.result == 'success'
          || needs.test-package-docker-e2e.result == 'skipped')
```

- [ ] **Step 2: Replace `publish-image` with `promote-images`**

Delete the entire `publish-image` job (QEMU/buildx/wait-for-PyPI included — promotion makes all of it dead) and add:

```yaml
  promote-images:
    needs: [publish]
    runs-on: ubuntu-latest
    permissions:
      packages: write
    strategy:
      matrix:
        image: [aaiclick, aaiclick-docker, aaiclick-kubectl]
    steps:
      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      # Retag the exact multi-arch manifests every gate tested — no rebuild,
      # no wait-for-PyPI. Tested digests ARE the published digests.
      - name: Promote rc manifest
        run: |
          REF="ghcr.io/kolodkin/${{ matrix.image }}"
          TAGS=(-t "${REF}:${{ inputs.tag }}")
          if [ "${{ inputs.pre-release }}" != "true" ]; then
            TAGS+=(-t "${REF}:latest")
          fi
          docker buildx imagetools create "${TAGS[@]}" "${REF}:${{ inputs.tag }}-rc"

      # Best-effort: GITHUB_TOKEN may lack package-version delete rights;
      # leftover -rc tags are harmless and useful for debugging.
      - name: Clean up rc tags
        continue-on-error: true
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        run: |
          for suffix in rc rc-amd64 rc-arm64; do
            TAG="${{ inputs.tag }}-${suffix}"
            VERSION_ID=$(gh api "/user/packages/container/${{ matrix.image }}/versions" --paginate \
              --jq ".[] | select(.metadata.container.tags[]? == \"${TAG}\") | .id" | head -n1)
            if [ -n "$VERSION_ID" ]; then
              gh api -X DELETE "/user/packages/container/${{ matrix.image }}/versions/${VERSION_ID}" || true
            fi
          done
```

- [ ] **Step 3: Validate YAML, review the full pipeline shape**

```bash
uv run --with pyyaml python -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yaml'))"
```

Then re-read `publish.yaml` end to end and confirm the dependency graph is exactly:
`build` → `build-rc-images` → `merge-rc-images` → {`test-package`, `test-compose-e2e`, `test-helm-e2e`} and `build` → {`test-package-local`, `test-package-docker-e2e`} → `publish` → `promote-images`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/publish.yaml
git commit -m "publish: promote tested rc manifests instead of post-publish rebuild"
```

---

### Task 10: docs — implementation references + shortify

**Files:**
- Modify: `docs/designs/deploy_scaffold.md`

- [ ] **Step 1: Update the spec with implementation references**

Per project convention (reference by symbol, no status icons):
- Scaffold section: `**Implementation**: aaiclick/deploy/compose_scaffold.py — see init_compose(); aaiclick/deploy/k8s_scaffold.py — see init_helm(); CLI wiring in aaiclick/__main__.py — see _run_compose_init() / _run_k8s_init().`
- Compose template section: reference `aaiclick/deploy/templates/compose/docker-compose.yaml` and remove the now-duplicated service table details that the template itself documents (keep the table, drop anything that drifted).
- Helm section: reference `aaiclick/deploy/templates/helm/aaiclick/`.
- Release pipeline section: reference `.github/workflows/publish.yaml`, `_compose-e2e-reusable.yaml`, `_helm-e2e-reusable.yaml`.
- Fix anything that drifted during implementation (flag names, service names, job names).

- [ ] **Step 2: Run the shortify skill on the edited doc, then commit**

```bash
git add docs/designs/deploy_scaffold.md
git commit -m "docs: point deploy scaffold spec at implementation"
```

---

### Task 11: full verification + push

- [ ] **Step 1: Run the affected suites**

```bash
uv run pytest aaiclick/deploy/ aaiclick/test_cli.py -v
uv run pytest test_e2e/compose/ --collect-only -q
uv run --with pyyaml python -c "import yaml; [yaml.safe_load(open(f)) for f in ['.github/workflows/publish.yaml', '.github/workflows/_compose-e2e-reusable.yaml', '.github/workflows/_helm-e2e-reusable.yaml']]"
```

Expected: all green / no collection errors / valid YAML.

- [ ] **Step 2: Run the default unit suite to catch regressions**

Run: `uv run pytest aaiclick/ -n auto -q`
Expected: no new failures vs. main.

- [ ] **Step 3: Push and check CI**

```bash
git push -u origin claude/scaffold-dockerfile-lii344
```

Then use the `check-pr` skill (project convention after every push).
