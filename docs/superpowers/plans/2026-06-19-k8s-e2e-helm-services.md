# Kubernetes E2E In-Cluster Services (Helm) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the Kubernetes e2e suite's host-side GitHub Actions service containers (Postgres, ClickHouse, pypiserver, registry) with in-cluster pods deployed by a committed Helm chart, reached from the host via NodePort + kind `extraPortMappings` and a single shared DSN.

**Architecture:** A self-contained Helm chart at `test_e2e/kubernetes/chart/` defines four `Deployment` + NodePort `Service` pairs. The kind cluster config maps each NodePort to a standard `localhost` port on the GHA host; `/etc/hosts` aliases let the two database DSNs resolve identically on the host (migrations/orchestrator) and inside job pods (CoreDNS). No runner code changes.

**Tech Stack:** Helm 3, kind, kubectl, GitHub Actions reusable workflow, pytest e2e suite.

**Reference spec:** `docs/superpowers/specs/2026-06-19-k8s-e2e-helm-services-design.md`

---

## Validation model (read before starting)

These deliverables are YAML/config, not Python — there is no pytest TDD loop. The per-task verification is **offline Helm rendering** (`helm lint` + `helm template`), and the final integration test is a **real CI run** on the branch.

Install the tools once at the start (the remote container does not have them):

```bash
curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
helm version   # expect: version.BuildInfo{Version:"v3...."}
```

If `get-helm-3` cannot reach the network, fall back to the tarball:
`curl -fsSL https://get.helm.sh/helm-v3.16.2-linux-amd64.tar.gz | tar xz && sudo install linux-amd64/helm /usr/local/bin/helm`.

`kind`/`kubectl` are NOT needed locally — cluster behavior is validated in CI only.

---

## File structure

| File | Responsibility |
|---|---|
| `test_e2e/kubernetes/chart/Chart.yaml` | Helm chart metadata |
| `test_e2e/kubernetes/chart/values.yaml` | Images, credentials, NodePort numbers |
| `test_e2e/kubernetes/chart/templates/postgres.yaml` | Postgres Deployment + NodePort Service |
| `test_e2e/kubernetes/chart/templates/clickhouse.yaml` | ClickHouse Deployment + NodePort Service |
| `test_e2e/kubernetes/chart/templates/pypiserver.yaml` | pypiserver Deployment + NodePort Service |
| `test_e2e/kubernetes/chart/templates/registry.yaml` | registry Deployment + NodePort Service |
| `.github/workflows/_kubernetes-e2e-reusable.yaml` | Rewritten: no service containers; install Helm; deploy chart; static DSNs |
| `.github/workflows/test-k8s-nightly.yaml` | Temporary push trigger for on-branch validation, then reverted |

---

## Task 1: Chart skeleton (Chart.yaml + values.yaml)

**Files:**
- Create: `test_e2e/kubernetes/chart/Chart.yaml`
- Create: `test_e2e/kubernetes/chart/values.yaml`

- [ ] **Step 1: Write `Chart.yaml`**

```yaml
apiVersion: v2
name: aaiclick-e2e-deps
description: >-
  In-cluster dependency services (Postgres, ClickHouse, pypiserver, registry)
  for the aaiclick Kubernetes e2e suite. CI test infrastructure only — ephemeral,
  no persistence, not a production deployment artifact.
type: application
version: 0.1.0
appVersion: "1.0"
```

- [ ] **Step 2: Write `values.yaml`**

```yaml
postgres:
  image: postgres:18.3
  user: aaiclick
  password: secret
  db: aaiclick
  nodePort: 30432
clickhouse:
  image: clickhouse/clickhouse-server:26.3
  user: default
  password: click123
  nodePort: 30123
pypiserver:
  image: pypiserver/pypiserver:v2.3.2
  nodePort: 30080
registry:
  image: registry:2
  nodePort: 30500
```

- [ ] **Step 3: Verify the chart lints (templates dir is empty, so expect a "no templates" note but no error)**

Run: `helm lint test_e2e/kubernetes/chart`
Expected: `1 chart(s) linted, 0 chart(s) failed` (an `[INFO]` about no templates is fine).

- [ ] **Step 4: Commit**

```bash
git add test_e2e/kubernetes/chart/Chart.yaml test_e2e/kubernetes/chart/values.yaml
git commit -m "test(k8s-e2e): add Helm chart skeleton for in-cluster deps"
```

---

## Task 2: Postgres template

**Files:**
- Create: `test_e2e/kubernetes/chart/templates/postgres.yaml`

- [ ] **Step 1: Write the template**

```yaml
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
          image: {{ .Values.postgres.image }}
          env:
            - name: POSTGRES_USER
              value: {{ .Values.postgres.user | quote }}
            - name: POSTGRES_PASSWORD
              value: {{ .Values.postgres.password | quote }}
            - name: POSTGRES_DB
              value: {{ .Values.postgres.db | quote }}
            # Point PGDATA at a subdir so the emptyDir mount (not a PVC) backs it.
            - name: PGDATA
              value: /var/lib/postgresql/data/pgdata
          ports:
            - containerPort: 5432
          volumeMounts:
            - name: data
              mountPath: /var/lib/postgresql/data
          readinessProbe:
            exec:
              command: ["pg_isready", "-U", "{{ .Values.postgres.user }}"]
            initialDelaySeconds: 5
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 12
      volumes:
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: aaiclick-postgres
  labels:
    app: aaiclick-postgres
spec:
  type: NodePort
  selector:
    app: aaiclick-postgres
  ports:
    - port: 5432
      targetPort: 5432
      nodePort: {{ .Values.postgres.nodePort }}
```

- [ ] **Step 2: Verify it renders**

Run: `helm template test_e2e/kubernetes/chart -s templates/postgres.yaml`
Expected: valid YAML printed; Service shows `nodePort: 30432`; probe command shows `pg_isready -U aaiclick`.

- [ ] **Step 3: Commit**

```bash
git add test_e2e/kubernetes/chart/templates/postgres.yaml
git commit -m "test(k8s-e2e): add Postgres Deployment+NodePort to e2e chart"
```

---

## Task 3: ClickHouse template

**Files:**
- Create: `test_e2e/kubernetes/chart/templates/clickhouse.yaml`

- [ ] **Step 1: Write the template**

```yaml
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
          image: {{ .Values.clickhouse.image }}
          env:
            - name: CLICKHOUSE_USER
              value: {{ .Values.clickhouse.user | quote }}
            - name: CLICKHOUSE_PASSWORD
              value: {{ .Values.clickhouse.password | quote }}
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
  type: NodePort
  selector:
    app: aaiclick-clickhouse
  ports:
    - name: http
      port: 8123
      targetPort: 8123
      nodePort: {{ .Values.clickhouse.nodePort }}
```

- [ ] **Step 2: Verify it renders**

Run: `helm template test_e2e/kubernetes/chart -s templates/clickhouse.yaml`
Expected: Service `nodePort: 30123`; readiness `httpGet /ping` on 8123.

- [ ] **Step 3: Commit**

```bash
git add test_e2e/kubernetes/chart/templates/clickhouse.yaml
git commit -m "test(k8s-e2e): add ClickHouse Deployment+NodePort to e2e chart"
```

---

## Task 4: pypiserver template

**Files:**
- Create: `test_e2e/kubernetes/chart/templates/pypiserver.yaml`

- [ ] **Step 1: Write the template**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aaiclick-pypiserver
  labels:
    app: aaiclick-pypiserver
spec:
  replicas: 1
  selector:
    matchLabels:
      app: aaiclick-pypiserver
  template:
    metadata:
      labels:
        app: aaiclick-pypiserver
    spec:
      containers:
        - name: pypiserver
          image: {{ .Values.pypiserver.image }}
          # Auth disabled (-a . -P .) — CI-only local index.
          args: ["run", "-p", "8080", "-a", ".", "-P", ".", "--server", "gunicorn"]
          ports:
            - containerPort: 8080
          readinessProbe:
            httpGet:
              path: /
              port: 8080
            initialDelaySeconds: 3
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 12
---
apiVersion: v1
kind: Service
metadata:
  name: aaiclick-pypiserver
  labels:
    app: aaiclick-pypiserver
spec:
  type: NodePort
  selector:
    app: aaiclick-pypiserver
  ports:
    - port: 8080
      targetPort: 8080
      nodePort: {{ .Values.pypiserver.nodePort }}
```

- [ ] **Step 2: Verify it renders**

Run: `helm template test_e2e/kubernetes/chart -s templates/pypiserver.yaml`
Expected: Service `nodePort: 30080`; args list matches the old service-container `command`.

- [ ] **Step 3: Commit**

```bash
git add test_e2e/kubernetes/chart/templates/pypiserver.yaml
git commit -m "test(k8s-e2e): add pypiserver Deployment+NodePort to e2e chart"
```

---

## Task 5: registry template

**Files:**
- Create: `test_e2e/kubernetes/chart/templates/registry.yaml`

- [ ] **Step 1: Write the template**

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: aaiclick-registry
  labels:
    app: aaiclick-registry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: aaiclick-registry
  template:
    metadata:
      labels:
        app: aaiclick-registry
    spec:
      containers:
        - name: registry
          image: {{ .Values.registry.image }}
          ports:
            - containerPort: 5000
          readinessProbe:
            httpGet:
              path: /v2/
              port: 5000
            initialDelaySeconds: 3
            periodSeconds: 5
            timeoutSeconds: 5
            failureThreshold: 12
---
apiVersion: v1
kind: Service
metadata:
  name: aaiclick-registry
  labels:
    app: aaiclick-registry
spec:
  type: NodePort
  selector:
    app: aaiclick-registry
  ports:
    - port: 5000
      targetPort: 5000
      nodePort: {{ .Values.registry.nodePort }}
```

- [ ] **Step 2: Verify the WHOLE chart renders and lints**

Run: `helm lint test_e2e/kubernetes/chart && helm template aaiclick-e2e test_e2e/kubernetes/chart | grep -c "^kind:"`
Expected: `0 chart(s) failed`; the grep count is `8` (4 Deployments + 4 Services).

- [ ] **Step 3: Commit**

```bash
git add test_e2e/kubernetes/chart/templates/registry.yaml
git commit -m "test(k8s-e2e): add registry Deployment+NodePort to e2e chart"
```

---

## Task 6: Rewrite the reusable workflow

**Files:**
- Modify (full rewrite): `.github/workflows/_kubernetes-e2e-reusable.yaml`

This task replaces the `services:` block and the registry-wiring step with a Helm
deployment, updates the kind config (containerd mirror endpoint + four
`extraPortMappings`), adds `/etc/hosts` aliases, and switches the DSNs to the
Service-name form. The wheel/migrate/pytest/diagnostics logic is preserved.

- [ ] **Step 1: Replace the file with the new content**

```yaml
name: Kubernetes Runner E2E (reusable)

# Shared body for the kubernetes-runner end-to-end suite. A kind cluster runs
# ALL dependency services (Postgres, ClickHouse, pypiserver, registry) as pods,
# deployed by the committed Helm chart in test_e2e/kubernetes/chart. There are
# no GitHub Actions service containers.
#
# Networking (see docs/superpowers/specs/2026-06-19-k8s-e2e-helm-services-design.md):
#   - Each service is a NodePort Service. kind extraPortMappings bridge each
#     NodePort to a standard localhost port on the runner:
#       postgres   30432 -> 127.0.0.1:5432
#       clickhouse 30123 -> 127.0.0.1:8123
#       pypiserver 30080 -> 127.0.0.1:8080
#       registry   30500 -> 127.0.0.1:5000
#   - The two databases use ONE DSN that resolves in both places: on the runner
#     via /etc/hosts (aaiclick-postgres/-clickhouse -> 127.0.0.1 -> extraPortMapping
#     -> NodePort), and inside job pods via CoreDNS (Service name). The pod
#     inherits the host DSN verbatim through build_runner_env, so no runner change.
#   - The job image tag stays localhost:5000/aaiclick-job:<sha>: the host pushes
#     to localhost:5000 (-> NodePort 30500 -> registry pod) and the node's
#     containerd resolves it via the mirror localhost:5000 -> localhost:30500.
#   - The host-side image build reaches the in-cluster pypi via
#     host.docker.internal:8080 (--add-host=...:host-gateway), unchanged.

on:
  workflow_call:
    inputs:
      wheel_source:
        description: "Where the wheel comes from: 'source' or 'artifact'"
        required: true
        type: string
      version_pin:
        description: "vX.Y.Z to install in artifact mode; empty in source mode means auto-detect"
        required: false
        default: ""
        type: string

jobs:
  e2e:
    runs-on: ubuntu-latest
    timeout-minutes: 60

    steps:
      - name: Checkout code
        uses: actions/checkout@v5
        with:
          fetch-depth: 0

      - name: Install uv
        uses: astral-sh/setup-uv@v7
        with:
          enable-cache: true
          cache-dependency-glob: "pyproject.toml"

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

      # kind config wires the registry mirror to the node's own NodePort and
      # bridges every service NodePort to a standard localhost port on the host.
      - name: Create kind cluster
        run: |
          cat > /tmp/kind.yaml <<'EOF'
          kind: Cluster
          apiVersion: kind.x-k8s.io/v1alpha4
          containerdConfigPatches:
            - |-
              [plugins."io.containerd.grpc.v1.cri".registry.mirrors."localhost:5000"]
                endpoint = ["http://localhost:30500"]
          nodes:
            - role: control-plane
              extraPortMappings:
                - containerPort: 30432
                  hostPort: 5432
                  listenAddress: "127.0.0.1"
                - containerPort: 30123
                  hostPort: 8123
                  listenAddress: "127.0.0.1"
                - containerPort: 30080
                  hostPort: 8080
                  listenAddress: "127.0.0.1"
                - containerPort: 30500
                  hostPort: 5000
                  listenAddress: "127.0.0.1"
          EOF
          kind create cluster --name aaiclick-e2e --config /tmp/kind.yaml

      - name: Wait for cluster DNS (CoreDNS) ready
        run: kubectl -n kube-system rollout status deploy/coredns --timeout=180s

      - name: Deploy dependency services (Helm)
        run: |
          helm install aaiclick-e2e ./test_e2e/kubernetes/chart \
            --wait --timeout 5m
          kubectl get pods -o wide

      # Single shared DSNs + /etc/hosts aliases so the names resolve on the host
      # (migrations + orchestrator) exactly as they do inside pods (CoreDNS).
      - name: Wire host DSNs
        run: |
          echo "127.0.0.1 aaiclick-postgres aaiclick-clickhouse" | sudo tee -a /etc/hosts
          echo "AAICLICK_SQL_URL=postgresql+asyncpg://aaiclick:secret@aaiclick-postgres:5432/aaiclick" >> "$GITHUB_ENV"
          echo "AAICLICK_CH_URL=clickhouse://default:click123@aaiclick-clickhouse:8123/default" >> "$GITHUB_ENV"

      - name: Start git daemon
        run: |
          GIT_DAEMON_BASE="$RUNNER_TEMP/gitsrv"
          mkdir -p "$GIT_DAEMON_BASE"
          echo "AAICLICK_E2E_GIT_DAEMON_BASE=$GIT_DAEMON_BASE" >> "$GITHUB_ENV"
          echo "AAICLICK_E2E_GIT_DAEMON_PORT=9418" >> "$GITHUB_ENV"
          git daemon --reuseaddr --listen=127.0.0.1 --port=9418 \
            --base-path="$GIT_DAEMON_BASE" --export-all --detach
          for i in $(seq 1 30); do
            if (exec 3<>/dev/tcp/127.0.0.1/9418) 2>/dev/null; then exec 3>&-; echo ready; exit 0; fi
            sleep 1
          done
          echo "git daemon failed to become ready" >&2
          exit 1

      # --- Wheel acquisition (branches on wheel_source) -------------------

      - name: Build wheel from source
        if: inputs.wheel_source == 'source'
        run: |
          uv sync --frozen --extra distributed --extra test --python 3.10
          uv build

      - name: Download dist artifact
        if: inputs.wheel_source == 'artifact'
        uses: actions/download-artifact@v7
        with:
          name: dist
          path: dist/

      - name: Download requirements artifact
        if: inputs.wheel_source == 'artifact'
        uses: actions/download-artifact@v7
        with:
          name: requirements

      - name: Upload wheel to test pypi
        run: |
          for f in dist/*.whl dist/*.tar.gz; do
            curl -fsSL -F "content=@${f}" -F ":action=file_upload" http://localhost:8080/
          done

      - name: Install aaiclick from test pypi (artifact mode)
        if: inputs.wheel_source == 'artifact'
        env:
          UV_INDEX_URL: http://localhost:8080/simple/
          UV_EXTRA_INDEX_URL: https://pypi.org/simple/
          UV_INDEX_STRATEGY: unsafe-best-match
        run: |
          uv venv --python 3.10
          uv pip install -r requirements-dist.txt
          VERSION="${{ inputs.version_pin }}"
          uv pip install "aaiclick[distributed]==${VERSION#v}"

      # --- Common: migrations, test ---------------------------------------

      - name: Run database migrations
        run: uv run --no-project python -m aaiclick migrate upgrade head

      - name: Run kubernetes e2e tests
        env:
          VIRTUAL_ENV: ${{ github.workspace }}/.venv
          # AAICLICK_SQL_URL / AAICLICK_CH_URL come from the "Wire host DSNs" step.
          AAICLICK_REGISTRY: "localhost:5000"
          AAICLICK_PIP_INDEX_URL: "http://host.docker.internal:8080/simple/"
          AAICLICK_PIP_TRUSTED_HOST: "host.docker.internal"
          AAICLICK_DOCKER_BUILD_ADD_HOST: "host.docker.internal:host-gateway"
          AAICLICK_LOG_DIR: "${{ github.workspace }}/tmp/aaiclick-logs"
        run: |
          mkdir -p tmp
          uv run --no-project pytest test_e2e/kubernetes/ -m kubernetes_e2e -n 0 -v -s \
            -o asyncio_mode=auto \
            -o asyncio_default_fixture_loop_scope=module \
            -o asyncio_default_test_loop_scope=module \
            -o log_cli=true \
            -o log_cli_level=INFO \
            -o "filterwarnings=error" \
            --strict-markers \
            --junitxml=tmp/pytest-report.xml

      - name: Publish test results
        uses: dorny/test-reporter@v2
        if: always()
        with:
          name: "Kubernetes Runner E2E (${{ inputs.wheel_source }})"
          path: tmp/pytest-report.xml
          reporter: java-junit

      - name: Dump cluster diagnostics on failure
        if: failure()
        run: |
          echo "===== pods / events ====="
          kubectl get pods -A -o wide || true
          kubectl get events --sort-by=.lastTimestamp || true
          for p in $(kubectl get pods -o name 2>/dev/null); do
            echo "===== describe $p ====="; kubectl describe "$p" || true
            echo "===== logs $p ====="; kubectl logs "$p" --tail=80 || true
          done
          echo "===== captured pod task logs (host side) ====="
          find "${{ github.workspace }}/tmp/aaiclick-logs" -name '*.log' 2>/dev/null | while read -r f; do
            echo "----- $f -----"; cat "$f" || true
          done
          echo "===== tasks / jobs table ====="
          uv run --no-project python - <<'PY' || true
          import asyncio, asyncpg
          async def main():
              c = await asyncpg.connect("postgresql://aaiclick:secret@aaiclick-postgres:5432/aaiclick")
              for r in await c.fetch("select id,name,status,error from jobs order by id"):
                  print("JOB", dict(r))
              for r in await c.fetch("select id,entrypoint,status,error from tasks order by id"):
                  print("TASK", dict(r))
              for r in await c.fetch("select task_id,run_epoch,success,error from task_run_results order by task_id"):
                  print("RESULT", dict(r))
              await c.close()
          asyncio.run(main())
          PY
```

- [ ] **Step 2: Verify the workflow is valid YAML**

Run: `uv run --no-project python -c "import yaml,sys; yaml.safe_load(open('.github/workflows/_kubernetes-e2e-reusable.yaml')); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Sanity-check the diff against the old file**

Run: `git diff --stat .github/workflows/_kubernetes-e2e-reusable.yaml`
Expected: the `services:` block and `docker network connect` step are gone; the Helm steps and `extraPortMappings` are present. Confirm `AAICLICK_REGISTRY: "localhost:5000"` and the `host.docker.internal` pip vars are still present (unchanged).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/_kubernetes-e2e-reusable.yaml
git commit -m "test(k8s-e2e): deploy deps via Helm chart, drop GHA service containers"
```

---

## Task 7: Add temporary on-branch validation trigger

**Files:**
- Modify: `.github/workflows/test-k8s-nightly.yaml`

A `schedule` cron only fires from the default branch, so to validate on this
branch we temporarily add a `push` trigger scoped to it. This is reverted in
Task 9.

- [ ] **Step 1: Add the push trigger**

Change the `on:` block from:

```yaml
on:
  schedule:
    - cron: "0 7 * * *"
  workflow_dispatch:
```

to:

```yaml
on:
  push:
    branches:
      - claude/kubernetes-runner-support-rxv9hm
  schedule:
    - cron: "0 7 * * *"
  workflow_dispatch:
```

- [ ] **Step 2: Verify valid YAML**

Run: `uv run --no-project python -c "import yaml; yaml.safe_load(open('.github/workflows/test-k8s-nightly.yaml')); print('ok')"`
Expected: `ok`

- [ ] **Step 3: Commit and push**

```bash
git add .github/workflows/test-k8s-nightly.yaml
git commit -m "ci: temporarily trigger k8s e2e on branch for validation"
git push -u origin claude/kubernetes-runner-support-rxv9hm
```

---

## Task 8: Validate the CI run end-to-end

**Files:** none (observation only)

- [ ] **Step 1: Watch the run via the GitHub MCP tools**

Use the `check-pr` skill, or `mcp__github__actions_list` / `mcp__github__get_job_logs`
to follow the `Kubernetes Runner E2E (nightly)` run triggered by the Task 7 push.

- [ ] **Step 2: Confirm a real pass**

Expected, in order: kind cluster created → CoreDNS ready → `helm install --wait`
succeeds with all 4 pods Ready → migrations apply → wheel build/upload →
e2e tests pass → "Dump cluster diagnostics on failure" step is **skipped** (not
run). A skipped diagnostics step is the signal of a genuine pass, not a no-op.

- [ ] **Step 3: If it fails, debug before proceeding**

Use `superpowers:systematic-debugging`. Likely first-look spots:
- Registry pull fails in-pod → check the containerd mirror endpoint
  (`localhost:30500`) and that the registry pod is Ready.
- DB connection refused on the host → check the `/etc/hosts` aliases and that
  the `extraPortMappings` bound (`listenAddress: 127.0.0.1`).
- `helm install` times out → `kubectl describe`/`logs` the not-ready pod (image
  pull, readiness probe path/port).

Fix, commit, push, and re-watch. Do not proceed to Task 9 until the run is green.

---

## Task 9: Revert the validation trigger

**Files:**
- Modify: `.github/workflows/test-k8s-nightly.yaml`

- [ ] **Step 1: Remove the push trigger** (restore the original `on:` block)

```yaml
on:
  schedule:
    - cron: "0 7 * * *"
  workflow_dispatch:
```

- [ ] **Step 2: Commit and push**

```bash
git add .github/workflows/test-k8s-nightly.yaml
git commit -m "ci: revert temporary k8s e2e branch trigger"
git push -u origin claude/kubernetes-runner-support-rxv9hm
```

---

## Self-review (completed during plan authoring)

**Spec coverage:**
- Chart at `test_e2e/kubernetes/chart/`, self-contained, ephemeral → Tasks 1–5. ✅
- 4 Deployments + 4 NodePort Services, readiness probes, values.yaml creds/ports → Tasks 1–5. ✅
- Remove `services:` block + `docker network connect` step → Task 6. ✅
- kind containerd mirror `localhost:5000`→`localhost:30500` + four `extraPortMappings` → Task 6. ✅
- Install Helm, `helm install --wait`, `/etc/hosts` aliases, static Service-name DSNs → Task 6. ✅
- `AAICLICK_REGISTRY`/pip vars and build/push/migrate/pytest unchanged → Task 6 (verified in Step 3). ✅
- Temporary push trigger, green validation, revert to nightly → Tasks 7–9. ✅
- Non-goal (no runner code change) honored — no task touches `aaiclick/`. ✅

**Placeholder scan:** none — every template and the full workflow are spelled out.

**Type/name consistency:** Service names (`aaiclick-postgres`, `aaiclick-clickhouse`),
NodePorts (30432/30123/30080/30500 → 5432/8123/8080/5000), and the mirror endpoint
(`localhost:30500`) match across the chart templates, the kind config, the
`/etc/hosts`/DSN step, and the diagnostics connection string.
