# Isolated Image Builds on BuildKit — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task (inline, per CLAUDE.md). Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run every `build` image source on a rootless buildkitd via the buildx remote driver, keep worker secrets out of build steps, key image tags by `image_key`, and let shell/jvm children declare builds from allowlisted remotes.

**Architecture:** The injected build task stays a host-pinned module task; only `docker build` moves to `docker buildx build --builder aaiclick` against `AAICLICK_BUILDKIT_ADDR`. Deploy scaffolds and CI e2e start a buildkitd. `_build_dag_from_dicts` swaps its blanket rejection for a remote allowlist.

**Tech Stack:** Python 3 / pytest (asyncio), docker buildx 0.20.1 remote driver, `moby/buildkit` rootless, docker compose, helm, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-22-buildkit-isolated-builds-design.md`

## Global Constraints

- Imports at top of file; no `TYPE_CHECKING`; no `global`; `Literal` over enums (CLAUDE.md).
- Tag shape: `[<registry>/]aaiclick-job:<sha>-<image_key[:12]>`.
- Builder name: `aaiclick`. Env: `AAICLICK_BUILDKIT_ADDR` (required for builds), `AAICLICK_CHILD_BUILD_REMOTES` (comma-separated prefixes).
- Secret ids: `pip_index_url`, `pip_extra_index_url`. `PIP_TRUSTED_HOST`, `GIT_*`, `AAICLICK_VERSION`, `BASE_IMAGE` stay build-args.
- Child remote schemes: `https://` or scp-style `git@host:` only.
- Never fall back to the host daemon when buildkitd is unreachable.
- buildkitd image pinned once per scaffold: `moby/buildkit:v0.20.2-rootless` (bump to latest stable ≥ 2 weeks old at implementation time; pin digest too).
- Docs follow the `markdown-style` skill.

## Review Focus

- `AAICLICK_BUILDKIT_ADDR` unset on a worker that receives a build task → actionable `RuntimeError` naming the variable (Task 3 test).
- `host.docker.internal:host-gateway` in `AAICLICK_DOCKER_BUILD_ADD_HOST` → resolved to a literal IP before reaching buildx (Task 3 test).
- A child remote that is a prefix-collision (`https://github.com/myorg-evil/x` vs prefix `https://github.com/myorg/`) → rejected; prefixes match on whole path segments only because callers write a trailing `/` — test it (Task 4).
- A child with `dockerfile` only (no `git_remote`) under a prebuilt parent → rejected, not crash (Task 4 test).
- A retry after a failed push in registry mode → still pushes (existing regression test, kept green in Task 3).

---

### Task 1: Image tag keyed by `image_key`

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py` (`compute_image_tag`)
- Modify: `aaiclick/orchestration/execution/image_build_task.py:63`, `aaiclick/orchestration/execution/docker_build.py:179`, `aaiclick/orchestration/execution/runner_env.py:65` (docstring)
- Test: `aaiclick/orchestration/test_docker_config.py`, `aaiclick/orchestration/execution/test_docker_build.py` (call sites)

**Interfaces:**
- Produces: `compute_image_tag(source: ImageBuild) -> str`

- [ ] **Step 1: Write failing tests** in `test_docker_config.py` (replace `test_compute_image_tag_without_registry`):

```python
def test_compute_image_tag_without_registry(monkeypatch):
    monkeypatch.delenv("AAICLICK_REGISTRY", raising=False)
    src = ImageBuild(git_remote="https://x/a.git", git_sha="b" * 40)
    assert compute_image_tag(src) == f"aaiclick-job:{'b' * 40}-{image_key(src)[:12]}"

@pytest.mark.parametrize("other", [
    {"git_remote": "https://x/evil.git"},
    {"dockerfile": "alt.Dockerfile"},
])
def test_compute_image_tag_differs_per_identity(other):
    base = ImageBuild(git_remote="https://x/a.git", git_sha="b" * 40)
    assert compute_image_tag(base) != compute_image_tag(base.model_copy(update=other))
```

- [ ] **Step 2:** Run `pytest aaiclick/orchestration/test_docker_config.py -k compute_image_tag -v` → FAIL (TypeError / mismatch).
- [ ] **Step 3:** Change `compute_image_tag` to take `ImageBuild`; update the three call sites and `test_docker_build.py`'s `compute_image_tag("a" * 40)` calls to pass the `source`.
- [ ] **Step 4:** Run `pytest aaiclick/orchestration -k "image_tag or docker_build or image_build" -q` → PASS.
- [ ] **Step 5:** Commit `fix: key build image tags by remote, SHA and Dockerfile`.

### Task 2: Index URLs as BuildKit secrets

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py` (`_collect_build_args`, new `_collect_secret_flags`)
- Modify: `aaiclick/orchestration/execution/docker_scaffold.py` (`DOCKERFILE_TEMPLATE`, `_PIP_INDEX`/uv index strings, default templates)
- Test: `aaiclick/orchestration/execution/test_docker_build.py`

**Interfaces:**
- Produces: `_collect_secret_flags() -> list[str]` — `["--secret", "id=pip_index_url,env=AAICLICK_PIP_INDEX_URL", ...]`, only for set env vars.

- [ ] **Step 1: Rewrite tests:** `test_collect_build_args_forwards_pip_indices` becomes:

```python
async def test_index_urls_are_secrets_not_build_args(monkeypatch):
    monkeypatch.setenv("AAICLICK_PIP_INDEX_URL", "http://tok@pypi.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_EXTRA_INDEX_URL", "http://extra.test/simple/")
    monkeypatch.setenv("AAICLICK_PIP_TRUSTED_HOST", "pypi.test")
    args = docker_build._collect_build_args(ImageBuild(git_remote="https://e/r.git", git_sha="a" * 40))
    assert not any("pypi.test/simple" in a or "extra.test" in a for a in args)
    assert "PIP_TRUSTED_HOST=pypi.test" in args
    assert docker_build._collect_secret_flags() == [
        "--secret", "id=pip_index_url,env=AAICLICK_PIP_INDEX_URL",
        "--secret", "id=pip_extra_index_url,env=AAICLICK_PIP_EXTRA_INDEX_URL",
    ]
```

  Plus: unset vars → `_collect_secret_flags() == []`; every rendered default Dockerfile (parametrize over manifest kinds) and `DOCKERFILE_TEMPLATE` contains `--mount=type=secret,id=pip_index_url,env=PIP_INDEX_URL`, starts with `# syntax=docker/dockerfile:1`, and has no `ARG PIP_INDEX_URL`.
- [ ] **Step 2:** Run `pytest aaiclick/orchestration/execution/test_docker_build.py -q` → new tests FAIL.
- [ ] **Step 3:** Drop the two index `add(...)` calls from `_collect_build_args`; add `_collect_secret_flags`; in the templates replace `ARG PIP_INDEX_URL`/`ARG PIP_EXTRA_INDEX_URL` with secret mounts (`env=PIP_INDEX_URL`, `env=PIP_EXTRA_INDEX_URL`, both `required=false`) on the install `RUN`; the `${PIP_INDEX_URL:-...}` expansions stay.
- [ ] **Step 4:** Run `pytest aaiclick/orchestration/execution -k "docker_build or scaffold" -q` → PASS.
- [ ] **Step 5:** Commit `feat: pass private index URLs as BuildKit secrets`.

### Task 3: buildx remote-driver backend

**Files:**
- Modify: `aaiclick/orchestration/docker_config.py` (new `get_buildkit_addr`, `resolve_host_gateway`)
- Modify: `aaiclick/orchestration/execution/docker_build.py`
- Test: `aaiclick/orchestration/execution/test_docker_build.py`, `aaiclick/orchestration/test_docker_config.py`

**Interfaces:**
- Consumes: `compute_image_tag(source)` (Task 1), `_collect_secret_flags()` (Task 2)
- Produces in `docker_config`: `get_buildkit_addr() -> str` (raises `RuntimeError` naming `AAICLICK_BUILDKIT_ADDR` when unset); `add_host_flags(env_var: str, *, gateway_ip: str | None = None) -> list[str]` — replaces a `host-gateway` value with `gateway_ip` when given.
- Produces in `docker_build`: `_require_builder() -> None` (replaces `_require_docker`), `_registry_has_image(tag) -> bool` (`docker buildx imagetools inspect`), `_buildx_build(context, dockerfile, tag, build_args, *, push: bool) -> None`, `_host_gateway_ip() -> str`.

- [ ] **Step 1: Write failing tests:**
  - `test_require_builder_creates_remote_builder_on_miss` — stub `cli.run`: `buildx inspect aaiclick` rc=1 → expects a `buildx create --name aaiclick --driver remote <addr>` call, then `buildx inspect --bootstrap aaiclick`.
  - `test_require_builder_raises_without_addr` — env unset → `RuntimeError` matching `AAICLICK_BUILDKIT_ADDR`.
  - `test_require_builder_raises_when_unreachable` — bootstrap rc≠0 → `RuntimeError` naming the address; no host-daemon call.
  - `test_build_registry_mode_pushes_via_buildx` — registry set, `_registry_has_image` False → `_buildx_build(..., push=True)`; argv contains `--builder aaiclick`, `--push`, the secret flags, never `--load`.
  - `test_build_registry_mode_cache_hit_skips_build` — `_registry_has_image` True → no clone, no build.
  - `test_build_local_mode_loads` — `AAICLICK_LOCAL_BUILD=1`, `docker image inspect` miss → argv contains `--load`.
  - `test_add_host_flags_resolves_host_gateway` — `add_host_flags("X", gateway_ip="172.17.0.1")` with `X=host.docker.internal:host-gateway` → `["--add-host", "host.docker.internal:172.17.0.1"]`.
  - Delete `_require_docker` tests and the push-after-local-cache-hit test (registry mode no longer touches the host daemon, so the scenario is gone); keep clone, Dockerfile-resolution and launch-image tests.
- [ ] **Step 2:** Run `pytest aaiclick/orchestration/execution/test_docker_build.py aaiclick/orchestration/test_docker_config.py -q` → new tests FAIL.
- [ ] **Step 3:** Implement. `build_image_to_tag` flow: `_require_builder()`; registry → `_registry_has_image` hit returns; local → `_docker_image_exists_locally` hit returns; else clone, `_resolve_dockerfile`, `_buildx_build(push=registry is not None)`. `_host_gateway_ip` reads the bridge gateway via `docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}'` only when an add-host entry says `host-gateway`; with no daemon it raises a `RuntimeError` telling the operator to write the IP literally. Remove `_docker_pull`, `_docker_push`, `_docker_build`, `_require_docker`.
- [ ] **Step 4:** Run `pytest aaiclick/orchestration -q` → PASS.
- [ ] **Step 5:** Commit `feat: build images on buildkitd through the buildx remote driver`.

### Task 4: Allowlisted builds for shell/jvm children

**Files:**
- Modify: `aaiclick/orchestration/execution/runner.py` (`_build_dag_from_dicts`, `register_returned_tasks`, call sites at ~412 and ~514)
- Test: `aaiclick/orchestration/execution/test_execution.py`

**Interfaces:**
- Produces: `register_returned_tasks(result, parent_task_id, job_id, parent_entry_type, parent_image_source: dict | None = None)`; `_build_dag_from_dicts(items, parent_entry_type, parent_image_source: dict | None)`; `_check_child_remote(remote: str | None, parent_image_source: dict | None) -> None` raising `ValueError` naming the remote and `AAICLICK_CHILD_BUILD_REMOTES`.

- [ ] **Step 1:** Replace `test_finish_shell_result_rejects_build_child` with:

```python
@pytest.mark.parametrize("remote,allow,ok", [
    ("https://git/parent.git", "", True),                      # parent's own repo
    ("https://github.com/myorg/x.git", "https://github.com/myorg/", True),
    ("git@github.com:myorg/x.git", "git@github.com:myorg/", True),
    ("https://github.com/myorg-evil/x.git", "https://github.com/myorg/", False),
    ("file:///etc", "file:///", False),
    ("/srv/repo", "/srv/", False),
    ("https://elsewhere/x.git", "", False),
    (None, "", False),                                         # dockerfile-only child under a remote-less parent
])
async def test_finish_shell_result_child_build_allowlist(orch_ctx, monkeypatch, remote, allow, ok): ...
```

  The parent is a shell task with `image_source=ImageBuild(git_remote="https://git/parent.git", git_sha="a"*40).model_dump()` except the `None` row, which uses a prebuilt parent. `ok` → the child is committed with its `image_source` and a build task is injected; else `pytest.raises(ValueError, match="AAICLICK_CHILD_BUILD_REMOTES")`.
- [ ] **Step 2:** Run `pytest aaiclick/orchestration/execution/test_execution.py -k allowlist -v` → FAIL.
- [ ] **Step 3:** Implement: when `parent_entry_type != ENTRY_MODULE` and the child declares `_BUILD_FIELDS`, the effective remote is `fields.get("git_remote")` or the parent's; allow if it equals the parent's build remote or starts with a listed prefix, and the scheme is `https://` or matches `^git@[^:/]+:`. Pass `task.image_source` from both call sites. Update the docstring sentence.
- [ ] **Step 4:** Run `pytest aaiclick/orchestration/execution/test_execution.py -q` → PASS.
- [ ] **Step 5:** Commit `feat: allow shell/jvm children to declare builds from allowlisted remotes`.

### Task 5: Deploy scaffolds run buildkitd

**Files:**
- Modify: `aaiclick/deploy/templates/compose/docker-compose.yaml`; Create: `aaiclick/deploy/templates/compose/buildkitd.toml` (and wire it into `compose init` copying, wherever templates are listed)
- Modify: `aaiclick/deploy/templates/helm/aaiclick/values.yaml`, `templates/worker.yaml`; Create: `templates/buildkitd.yaml` (Deployment + Service + NetworkPolicy)
- Modify: `docker/kubectl.Dockerfile` header comment
- Test: `aaiclick/deploy/test_compose_scaffold.py` (+ existing helm template test if present)

- [ ] **Step 1: Failing tests:** compose scaffold has a `buildkitd` service on the pinned rootless image with `security_opt: [seccomp=unconfined, apparmor=unconfined]`, `extra_hosts` host-gateway, a cache volume, and worker env `AAICLICK_BUILDKIT_ADDR=tcp://buildkitd:1234`; `buildkitd.toml` marks `host.docker.internal:5000` `http = true, insecure = true`. Helm: rendered chart contains a `buildkitd` Service, a NetworkPolicy admitting ingress only from `app: aaiclick-worker`, and worker env `AAICLICK_BUILDKIT_ADDR` when `buildkit.enabled`.
- [ ] **Step 2:** Run `pytest aaiclick/deploy -q` → FAIL.
- [ ] **Step 3:** Implement templates; replace the "not provided here; use prebuilt-image jobs" comments; compose header documents that build steps can reach host-published ports.
- [ ] **Step 4:** Run `pytest aaiclick/deploy -q` (and `helm template` if CI lints charts) → PASS.
- [ ] **Step 5:** Commit `feat: ship a rootless buildkitd in the compose and helm scaffolds`.

### Task 6: CI e2e on buildkitd

**Files:**
- Modify: `.github/workflows/_docker-e2e-reusable.yaml`, `_kubernetes-e2e-reusable.yaml`, `_compose-e2e-reusable.yaml`, `_helm-e2e-reusable.yaml`

- [ ] **Step 1:** Docker/kubernetes e2e: add a step starting `moby/buildkit` (privileged in CI is fine) with `--add-host host.docker.internal:host-gateway`, a `buildkitd.toml` marking the CI registry insecure, publishing `1234`; set `AAICLICK_BUILDKIT_ADDR: tcp://localhost:1234`. Kubernetes e2e: give buildkitd the registry name it can push to (`host.docker.internal:5000` with the kind mirror mapped to it) instead of `localhost:5000`.
- [ ] **Step 2:** Add a leak-check job step: build a fixture repo whose Dockerfile runs `RUN env > /env.txt` and a task that prints `/env.txt`; assert the log has no `AAICLICK_` and no `8080/simple`.
- [ ] **Step 3:** Push, then run the `check-pr` skill; fix until docker, kubernetes, compose and helm e2e are green.
- [ ] **Step 4:** Commit `ci: run image builds on buildkitd in e2e`.

### Task 7: Docs and cleanup

**Files:**
- Modify: `docs/designs/orchestration.md` ("Image source", prerequisites, default Dockerfile), `docs/user_guide/container_images.md`, `docs/user_guide/deployment.md`, `docs/designs/future.md` (build quotas / per-job caps)
- Delete: the spec and this plan; fix references to them

- [ ] **Step 1:** Update docs per spec "Docs" section; run the `markdown-style` skill on each.
- [ ] **Step 2:** `grep -rn "2026-10-22-buildkit" .` → no hits after deleting spec + plan.
- [ ] **Step 3:** `pytest -q` and `ruff check .` → clean.
- [ ] **Step 4:** Commit `docs: document BuildKit builds; drop implemented spec`, push, run `check-pr`.
