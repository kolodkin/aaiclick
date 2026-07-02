# Release Container Images Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish three GHCR container images (`aaiclick`, `aaiclick-docker`, `aaiclick-kubectl`) as part of the existing PyPI release workflow.

**Architecture:** A CLI-free base image (`FROM python:3.13-slim`, installs the released wheel) serves the API server and doubles as a runner base. Two variants chain off it — `aaiclick-docker` adds the Docker CLI, `aaiclick-kubectl` adds `kubectl` on top of docker — for the dispatching-worker roles. A new `publish-image` job in `publish.yaml` builds all three multi-arch (amd64+arm64) after the wheel is on PyPI.

**Tech Stack:** Docker Buildx, GitHub Actions (`docker/*-action`), GHCR, FastAPI/uvicorn (existing server), setuptools_scm versioning.

## Global Constraints

- Registry: `ghcr.io/kolodkin/` — auth via the workflow's built-in `GITHUB_TOKEN` (no new secrets).
- Platforms: `linux/amd64` + `linux/arm64` for every image.
- Base image: `FROM python:3.13-slim` (latest stable 3.13; `requires-python >=3.10`).
- Image chain (strict `FROM` order): `aaiclick` → `aaiclick-docker` → `aaiclick-kubectl`.
- Tags per image: `vX.Y.Z` always; `latest` only when `inputs.pre-release == false`.
- Server: health endpoint `GET /health`; default port `5255`; run via `uvicorn aaiclick.server.app:app`.
- Non-root by default: base image runs as user `aaiclick`.
- Task images (docker/k8s runner) are still built per-SHA from the user's repo — NOT changed here (`aaiclick/orchestration/execution/docker_scaffold.py`).
- Release iteration: real `0.0.x` patch releases, no dry-run. First tag = **`v0.0.17`** (v0.0.16 is the current max on both git tags and PyPI). Bump patch on each failed attempt.
- Commit identity: `git config user.email noreply@anthropic.com && git config user.name Claude`.

---

### Task 1: Base image Dockerfile

**Files:**
- Create: `docker/Dockerfile`

**Interfaces:**
- Consumes: nothing (first task).
- Produces: an image whose default `CMD` serves the API on `0.0.0.0:5255`, runs as user `aaiclick`, and accepts build-arg `AAICLICK_VERSION` (PEP 440, e.g. `0.0.17`). Later tasks base their variants on this image via a `BASE_REF` build-arg.

Local testing installs the already-published `0.0.16` wheel from PyPI, so the exact production Dockerfile is exercised with no modifications.

- [ ] **Step 1: Write `docker/Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
# Base aaiclick image: API server + SPA, also usable as a runner base.
# Build-arg AAICLICK_VERSION pins the wheel to install from PyPI.
FROM python:3.13-slim

ARG AAICLICK_VERSION
ARG PIP_INDEX_URL=https://pypi.org/simple/

LABEL org.opencontainers.image.source="https://github.com/kolodkin/aaiclick"
LABEL org.opencontainers.image.description="aaiclick API server + runner base"

# Install the released wheel with all extras (distributed + ai + server).
RUN pip install --no-cache-dir \
    --index-url "${PIP_INDEX_URL}" \
    "aaiclick[all]==${AAICLICK_VERSION}"

# Run as a non-root user.
RUN useradd --create-home --uid 1000 aaiclick
WORKDIR /home/aaiclick
USER aaiclick

EXPOSE 5255

# Health probe uses stdlib (slim image has no curl).
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:5255/health').status==200 else 1)"

CMD ["uvicorn", "aaiclick.server.app:app", "--host", "0.0.0.0", "--port", "5255"]
```

- [ ] **Step 2: Build the base image locally against the published 0.0.16 wheel**

Run:
```bash
docker build -f docker/Dockerfile --build-arg AAICLICK_VERSION=0.0.16 -t aaiclick:test .
```
Expected: build succeeds. If `pip install` fails on `python:3.13-slim`, a dependency lacks a 3.13 wheel — stop and report before proceeding (this is exactly the compatibility check we front-loaded).

- [ ] **Step 3: Smoke-test the server and the non-root user**

Run:
```bash
docker run -d --name aaiclick-smoke -p 5255:5255 aaiclick:test
sleep 8
curl -fsS http://127.0.0.1:5255/health
echo
docker exec aaiclick-smoke whoami
docker rm -f aaiclick-smoke
```
Expected: `curl` prints `{"status":"ok"}` and `whoami` prints `aaiclick`.

- [ ] **Step 4: Commit**

```bash
git add docker/Dockerfile
git commit -m "feat(release): add base aaiclick container image"
```

---

### Task 2: Docker-CLI variant Dockerfile

**Files:**
- Create: `docker/docker.Dockerfile`

**Interfaces:**
- Consumes: the base image via build-arg `BASE_REF` (default `ghcr.io/kolodkin/aaiclick:latest`).
- Produces: an image identical to the base plus a working `docker` CLI on `PATH`, for the docker-dispatching worker.

The Docker CLI is extracted from the official static tarball (client only — no daemon), chosen for a pinned version and clean multi-arch mapping.

- [ ] **Step 1: Write `docker/docker.Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
# aaiclick + Docker CLI (client only). For the docker-dispatching worker,
# which runs `docker build`/`docker run` against a mounted host daemon.
ARG BASE_REF=ghcr.io/kolodkin/aaiclick:latest
FROM ${BASE_REF}

ARG DOCKER_CLI_VERSION=27.5.1
# Buildx sets TARGETARCH to amd64 / arm64; the static tarball uses x86_64 / aarch64.
ARG TARGETARCH

USER root
RUN set -eux; \
    case "${TARGETARCH}" in \
      amd64) dockerarch=x86_64 ;; \
      arm64) dockerarch=aarch64 ;; \
      *) echo "unsupported arch ${TARGETARCH}" >&2; exit 1 ;; \
    esac; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL "https://download.docker.com/linux/static/stable/${dockerarch}/docker-${DOCKER_CLI_VERSION}.tgz" -o /tmp/docker.tgz; \
    tar -xzf /tmp/docker.tgz -C /tmp; \
    install -m 0755 /tmp/docker/docker /usr/local/bin/docker; \
    rm -rf /tmp/docker /tmp/docker.tgz; \
    docker --version
USER aaiclick
```

- [ ] **Step 2: Build the docker variant on top of the local base**

Run:
```bash
docker build -f docker/docker.Dockerfile --build-arg BASE_REF=aaiclick:test -t aaiclick-docker:test .
```
Expected: build succeeds and the final `docker --version` line prints during build.

- [ ] **Step 3: Verify the docker CLI is on PATH and the server still works**

Run:
```bash
docker run --rm aaiclick-docker:test docker --version
docker run --rm --entrypoint python aaiclick-docker:test -c "import aaiclick; print('aaiclick ok')"
```
Expected: prints `Docker version 27.5.1, ...` and `aaiclick ok`.

- [ ] **Step 4: Commit**

```bash
git add docker/docker.Dockerfile
git commit -m "feat(release): add aaiclick-docker CLI variant image"
```

---

### Task 3: kubectl variant Dockerfile

**Files:**
- Create: `docker/kubectl.Dockerfile`

**Interfaces:**
- Consumes: the docker variant via build-arg `BASE_REF` (default `ghcr.io/kolodkin/aaiclick-docker:latest`), so it inherits the Docker CLI.
- Produces: an image with both `docker` and `kubectl` on `PATH`, for the Kubernetes-dispatching worker.

- [ ] **Step 1: Write `docker/kubectl.Dockerfile`**

```dockerfile
# syntax=docker/dockerfile:1
# aaiclick + docker CLI (inherited) + kubectl. For the k8s-dispatching worker,
# which builds/pushes the task image with docker, then creates pods with kubectl.
ARG BASE_REF=ghcr.io/kolodkin/aaiclick-docker:latest
FROM ${BASE_REF}

ARG KUBECTL_VERSION=v1.32.2
ARG TARGETARCH

USER root
RUN set -eux; \
    apt-get update; apt-get install -y --no-install-recommends curl ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl" -o /usr/local/bin/kubectl; \
    curl -fsSL "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${TARGETARCH}/kubectl.sha256" -o /tmp/kubectl.sha256; \
    echo "$(cat /tmp/kubectl.sha256)  /usr/local/bin/kubectl" | sha256sum -c -; \
    chmod 0755 /usr/local/bin/kubectl; \
    rm -f /tmp/kubectl.sha256; \
    kubectl version --client
USER aaiclick
```

- [ ] **Step 2: Build the kubectl variant on top of the local docker variant**

Run:
```bash
docker build -f docker/kubectl.Dockerfile --build-arg BASE_REF=aaiclick-docker:test -t aaiclick-kubectl:test .
```
Expected: build succeeds; the sha256 check passes and `kubectl version --client` prints during build.

- [ ] **Step 3: Verify both CLIs are present**

Run:
```bash
docker run --rm aaiclick-kubectl:test sh -c "docker --version && kubectl version --client --output=yaml | head -3"
```
Expected: prints the Docker version and the kubectl client version.

- [ ] **Step 4: Commit**

```bash
git add docker/kubectl.Dockerfile
git commit -m "feat(release): add aaiclick-kubectl variant image"
```

---

### Task 4: Add the `publish-image` job to the release workflow

**Files:**
- Modify: `.github/workflows/publish.yaml` (append a new job after `publish`)

**Interfaces:**
- Consumes: `inputs.tag` (`vX.Y.Z`) and `inputs.pre-release` (existing workflow inputs); the `publish` job having uploaded the wheel to PyPI.
- Produces: three multi-arch images pushed to GHCR, tagged `vX.Y.Z` (+ `latest` on non-pre-release).

- [ ] **Step 1: Append the job to `.github/workflows/publish.yaml`**

Add at the end of the file (sibling to `build`, `publish`, etc.):

```yaml
  publish-image:
    needs: [publish]
    runs-on: ubuntu-latest
    permissions:
      contents: read
      packages: write

    steps:
      - name: Checkout code
        uses: actions/checkout@v5

      - name: Compute PEP 440 version
        id: vars
        run: |
          VERSION="${{ inputs.tag }}"
          echo "pep440=${VERSION#v}" >> "$GITHUB_OUTPUT"

      # The image installs aaiclick==<version> from PyPI, so wait for the
      # just-published wheel to become resolvable before building.
      - name: Wait for version on PyPI
        run: |
          VERSION="${{ steps.vars.outputs.pep440 }}"
          for i in $(seq 1 30); do
            if curl -fsSL "https://pypi.org/pypi/aaiclick/${VERSION}/json" >/dev/null 2>&1; then
              echo "aaiclick ${VERSION} is on PyPI"; exit 0
            fi
            echo "waiting for aaiclick ${VERSION} on PyPI (attempt $i)"; sleep 10
          done
          echo "timed out waiting for aaiclick ${VERSION} on PyPI" >&2; exit 1

      - name: Set up QEMU
        uses: docker/setup-qemu-action@v3

      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3

      - name: Log in to GHCR
        uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - name: Metadata (base)
        id: meta-base
        uses: docker/metadata-action@v5
        with:
          images: ghcr.io/kolodkin/aaiclick
          tags: |
            type=raw,value=${{ inputs.tag }}
            type=raw,value=latest,enable=${{ !inputs.pre-release }}

      - name: Build & push base
        uses: docker/build-push-action@v6
        with:
          context: .
          file: docker/Dockerfile
          platforms: linux/amd64,linux/arm64
          push: true
          build-args: |
            AAICLICK_VERSION=${{ steps.vars.outputs.pep440 }}
          tags: ${{ steps.meta-base.outputs.tags }}
          labels: ${{ steps.meta-base.outputs.labels }}

      - name: Metadata (docker variant)
        id: meta-docker
        uses: docker/metadata-action@v5
        with:
          images: ghcr.io/kolodkin/aaiclick-docker
          tags: |
            type=raw,value=${{ inputs.tag }}
            type=raw,value=latest,enable=${{ !inputs.pre-release }}

      - name: Build & push docker variant
        uses: docker/build-push-action@v6
        with:
          context: .
          file: docker/docker.Dockerfile
          platforms: linux/amd64,linux/arm64
          push: true
          build-args: |
            BASE_REF=ghcr.io/kolodkin/aaiclick:${{ inputs.tag }}
          tags: ${{ steps.meta-docker.outputs.tags }}
          labels: ${{ steps.meta-docker.outputs.labels }}

      - name: Metadata (kubectl variant)
        id: meta-kubectl
        uses: docker/metadata-action@v5
        with:
          images: ghcr.io/kolodkin/aaiclick-kubectl
          tags: |
            type=raw,value=${{ inputs.tag }}
            type=raw,value=latest,enable=${{ !inputs.pre-release }}

      - name: Build & push kubectl variant
        uses: docker/build-push-action@v6
        with:
          context: .
          file: docker/kubectl.Dockerfile
          platforms: linux/amd64,linux/arm64
          push: true
          build-args: |
            BASE_REF=ghcr.io/kolodkin/aaiclick-docker:${{ inputs.tag }}
          tags: ${{ steps.meta-kubectl.outputs.tags }}
          labels: ${{ steps.meta-kubectl.outputs.labels }}
```

- [ ] **Step 2: Validate the workflow YAML parses**

Run:
```bash
python3 -c "import yaml,sys; yaml.safe_load(open('.github/workflows/publish.yaml')); print('yaml ok')"
```
Expected: prints `yaml ok`.

- [ ] **Step 3: Sanity-check job wiring**

Run:
```bash
python3 - <<'PY'
import yaml
w = yaml.safe_load(open('.github/workflows/publish.yaml'))
job = w['jobs']['publish-image']
assert job['needs'] == ['publish'], job['needs']
assert job['permissions']['packages'] == 'write'
steps = [s.get('name') for s in job['steps']]
for required in ['Log in to GHCR', 'Build & push base', 'Build & push docker variant', 'Build & push kubectl variant']:
    assert required in steps, f"missing step: {required}"
print("wiring ok")
PY
```
Expected: prints `wiring ok`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/publish.yaml
git commit -m "feat(release): publish container images from the release workflow"
```

---

### Task 5: Documentation — `docs/container_images.md`

**Files:**
- Create: `docs/container_images.md`
- Modify: `docs/getting_started.md` (add a link to the new page)

**Interfaces:**
- Consumes: nothing at runtime; documents the images from Tasks 1–4.
- Produces: user-facing docs covering the API-server run, the runner-base `FROM`, and the k8s (RBAC + Docker-to-build) and docker (socket mount) runtime requirements.

Follow the `markdown-style` skill (heading style, admonitions, tables). Run the `shortify` skill after writing.

- [ ] **Step 1: Write `docs/container_images.md`**

Content must cover, in this order:

1. The three images table (name / bundles / role), all `ghcr.io/kolodkin/…`, tags `vX.Y.Z` + `latest`, multi-arch amd64+arm64.
2. **API server:**
   ```bash
   docker run -p 5255:5255 ghcr.io/kolodkin/aaiclick:vX.Y.Z
   # UI + API at http://localhost:5255  (health: /health, API: /api/v0)
   ```
3. **Runner base:**
   ```dockerfile
   FROM ghcr.io/kolodkin/aaiclick:vX.Y.Z
   COPY . /src
   RUN pip install --no-cache-dir /src
   ```
4. **Kubernetes dispatching worker — requirements.** Explain in-cluster config (kubectl auto-reads `KUBERNETES_SERVICE_HOST`, the SA token, and CA — no kubeconfig mount); the RBAC gotcha (403 without a Role); and that the worker ALSO needs a reachable Docker daemon + `AAICLICK_REGISTRY` because it runs `docker build`/`docker push` before creating pods. Include the ServiceAccount + Role (pods: create/get/list/watch/delete; pods/log: get) + RoleBinding manifest, and note `serviceAccountName: aaiclick-worker` on the worker Deployment running `ghcr.io/kolodkin/aaiclick-kubectl`.
5. **Docker dispatching worker — requirements.** Socket mount, sibling containers, security note:
   ```bash
   docker run \
     -v /var/run/docker.sock:/var/run/docker.sock \
     ghcr.io/kolodkin/aaiclick-docker:vX.Y.Z \
     python -m aaiclick worker start ...
   ```
   Note the non-root image user may need the socket's group (or run with `--user root`), and that mounting the socket grants host-daemon control.

Source the exact RBAC manifest and phrasing from `docs/superpowers/specs/2026-07-02-release-container-images-design.md` (the "Kubernetes dispatching worker — requirements" and "Docker dispatching worker — requirements" sections).

- [ ] **Step 2: Link the page from `docs/getting_started.md`**

Add a line under the appropriate section (e.g. near deployment/running the server):

```markdown
See [Container images](container_images.md) for the published `aaiclick`, `aaiclick-docker`, and `aaiclick-kubectl` images and their Kubernetes/Docker runtime requirements.
```

- [ ] **Step 3: Verify links resolve and the page is listed**

Run:
```bash
test -f docs/container_images.md && grep -q "container_images.md" docs/getting_started.md && echo "docs ok"
```
Expected: prints `docs ok`.

- [ ] **Step 4: Run the `shortify` skill on `docs/container_images.md`, then commit**

```bash
git add docs/container_images.md docs/getting_started.md
git commit -m "docs: document published container images and their requirements"
```

---

### Task 6: Release iteration — dispatch until green

**Files:** none (operational task).

**Interfaces:**
- Consumes: Tasks 1–5 merged to the branch (and ultimately to `main`, since `publish.yaml` dispatches against a ref).
- Produces: a real `0.0.x` release with all three images live on GHCR.

> **Note:** `workflow_dispatch` runs the workflow from a git ref. Confirm which ref the release should run from (this feature branch vs `main`) before dispatching — the Dockerfiles and the `publish-image` job must exist on that ref.

- [ ] **Step 1: Push all preceding work and confirm the target ref**

Ensure Tasks 1–5 are committed and pushed to the ref the release will run from. Confirm with the user whether to dispatch from `claude/aaiclick-pypi-release-9247im` or after merging to `main`.

- [ ] **Step 2: Dispatch `publish.yaml` at the next patch tag**

Use the `action-run` skill to trigger `Publish to PyPI` with inputs `tag=v0.0.17`, `pre-release=false`, and monitor to completion. (`v0.0.17` is the next free tag; bump on each retry.)

- [ ] **Step 3: On failure, diagnose → fix → bump patch → re-dispatch**

If any job fails: read the failing job's logs, fix the Dockerfile/workflow, commit, push. Then re-dispatch with the **next** patch tag (`v0.0.18`, `v0.0.19`, …) — never reuse a tag, since `publish` creates the git tag and uploads to immutable PyPI. Repeat until all jobs (including `publish-image`) are green.

- [ ] **Step 4: Verify the images are pullable**

Run (using the successful tag):
```bash
docker pull ghcr.io/kolodkin/aaiclick:v0.0.17
docker pull ghcr.io/kolodkin/aaiclick-docker:v0.0.17
docker pull ghcr.io/kolodkin/aaiclick-kubectl:v0.0.17
docker run --rm ghcr.io/kolodkin/aaiclick-kubectl:v0.0.17 sh -c "docker --version && kubectl version --client"
```
Expected: all three pull; the last prints both CLI versions.

---

## Notes for the implementer

- **Open question carried from the spec:** whether the docker/kubectl variants should run non-root with a socket group or stay root for the mounted docker socket. This plan keeps them non-root (`USER aaiclick`) and documents the socket-group / `--user root` override in Task 5. If the docker-worker e2e later shows socket-permission failures, revisit.
- **Pinned versions** (`DOCKER_CLI_VERSION=27.5.1`, `KUBECTL_VERSION=v1.32.2`) are build-args — bump them in one place. Keep `kubectl` within one minor of the target clusters.
- **PyPI cost:** every dispatch of `publish.yaml` consumes a patch version permanently. Keep iterations tight; fix locally (Tasks 1–3 build the exact Dockerfiles offline) before dispatching.
