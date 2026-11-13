# GitHub App Credentials for Image Builds Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The build task's `git fetch` authenticates to private `github.com` repos with a GitHub App installation token minted on the worker.

**Architecture:** One new module mints a per-build, single-repo, read-only token and returns an env overlay for git; `docker_build` passes that overlay to the fetch only. Scaffolds ship the settings switched off; docs explain the feature once and give per-mode steps.

**Tech Stack:** Python stdlib (`urllib.request`, `asyncio.to_thread`), `pyjwt[crypto]`, pytest, Helm, Compose.

**Spec:** `docs/superpowers/specs/2026-11-13-github-app-builds-design.md`

## Global Constraints

- Settings: `AAICLICK_GITHUB_APP_ID`, `AAICLICK_GITHUB_APP_PEM_FILE` — no others.
- The token, JWT and key never appear in argv, a remote URL, an error message or a log line.
- `github.com` only; API base `https://api.github.com`.
- Token request body: `{"repositories": ["<repo>"], "permissions": {"contents": "read"}}`.
- JWT: RS256, `iss` = App ID, `iat` = now − 60, `exp` = now + 540.
- All imports at file top; no `Any`; no `global`; `Literal` over enums (`CLAUDE.md`).
- Tests follow the `python-testing-style` skill; docs follow `markdown-style`.

## Review Focus

- A lookalike host (`https://github.com.evil.test/o/r`, `https://notgithub.com/o/r`) must not match: the token is never offered to another host.
- A repo name with dots (`org/my.repo.git`) parses to `my.repo`.
- `GIT_CONFIG_COUNT` already set in the worker env, including a non-numeric value, never crashes: non-numeric counts as 0.
- A network failure or non-JSON reply from GitHub raises `RuntimeError`, not a raw `URLError` / `JSONDecodeError`.
- `AAICLICK_GITHUB_APP_PEM_FILE` pointing at a directory raises the "not a file" error.

Each is pinned by a test in Task 1.

---

### Task 1: Token minting and git env overlay

**Files:**
- Create: `aaiclick/orchestration/execution/github_app.py`
- Test: `aaiclick/orchestration/execution/test_github_app.py`
- Modify: `pyproject.toml` (`"pyjwt>=2.10.0"` → `"pyjwt[crypto]>=2.10.0"`), then `uv lock`

**Interfaces:**
- Produces: `async def git_auth_env(remote: str) -> dict[str, str] | None`
- Internal seam for tests: `def _api(method: str, path: str, jwt_token: str, body: dict | None = None) -> tuple[int, dict]` — the only function touching the network; returns `(status, json)`, converts `URLError` and bad JSON to `RuntimeError`.

- [ ] **Step 1: Write the failing tests.** A module fixture generates an RSA key with `cryptography`, writes the PEM to `tmp_path`; tests monkeypatch `github_app._api` with a recorder. Cases:
  - `test_unset_settings_return_none` — both env vars unset → `None`, `_api` not called.
  - `test_half_configured_raises` — parametrized over which one is set; `RuntimeError` naming the missing variable, even for a non-GitHub remote.
  - `test_remote_matching` — parametrized `(remote, owner_repo | None)`: `https://github.com/o/r`, `https://github.com/o/r.git`, `https://GitHub.com/o/r/`, `https://user@github.com/o/r.git`, `git@github.com:o/r.git`, `ssh://git@github.com/o/r`, `https://github.com/o/my.repo.git` → `my.repo`; `None` for `https://github.com.evil.test/o/r`, `https://notgithub.com/o/r`, `https://gitlab.com/o/r.git`, `https://github.com/o`.
  - `test_token_request_is_scoped` — first call is `GET /repos/o/r/installation`; second is `POST /app/installations/42/access_tokens` with the exact body above; the JWT passed decodes with the public key to `iss == "123"` and `exp - iat == 600`.
  - `test_env_overlay` — for `git@github.com:o/r.git`: `GIT_TERMINAL_PROMPT == "0"`, `AAICLICK_GIT_TOKEN == "tok"`, `GIT_CONFIG_COUNT == "3"`, keys `credential.https://github.com.helper` (value `""`), same key (value starts with `!` and contains `AAICLICK_GIT_TOKEN`, not `tok`), `url.https://github.com/o/r.insteadOf` (value = remote verbatim). For `https://github.com/o/r`: count `"2"`, no `insteadOf`.
  - `test_env_overlay_continues_existing_count` — `GIT_CONFIG_COUNT=2` in env → entries at `_2`, `_3`, count `"4"`; `GIT_CONFIG_COUNT=abc` → starts at 0.
  - `test_app_not_installed_warns_and_returns_none` — `_api` returns `(404, {...})` → `None`, and `capsys` shows one line naming `o/r`.
  - `test_errors_do_not_leak` — parametrized: PEM missing, PEM is a directory, installation `500`, token `403` with `{"message": "nope"}` → `RuntimeError`; message contains the status / `nope` where relevant and never the JWT or `tok`.
  - `test_api_wraps_network_errors` — monkeypatch `urllib.request.urlopen` to raise `URLError`, then to return non-JSON → `RuntimeError` from the real `_api`.

- [ ] **Step 2: Run** `pytest aaiclick/orchestration/execution/test_github_app.py -q` — expect collection/import failure.

- [ ] **Step 3: Implement the module.** Docstring: summary line, then `See docs/designs/orchestration.md — "Image source".` One regex for the three remote forms, host case-insensitive, anchored so lookalike hosts fail. Inline helper value:

  ```python
  _HELPER = '!f() { test "$1" = get && printf "username=x-access-token\\npassword=%s\\n" "$AAICLICK_GIT_TOKEN"; }; f'
  ```

  Network calls run through `asyncio.to_thread(_api, ...)`. The 404 warning goes to `sys.stderr` via `print`.

- [ ] **Step 4: Run** the test file — expect all pass. Run `ruff check` and `ruff format --check` on both files.

- [ ] **Step 5: Verify the helper against real git** (no network): in a temp dir, run `git credential fill` with the overlay env and stdin `protocol=https\nhost=github.com\n\n`; expect `username=x-access-token` and `password=tok` in the output. Keep this as `test_git_accepts_the_helper` (skipped only if `git` is absent).

- [ ] **Step 6: Commit** `feat: mint GitHub App tokens for build clones`.

### Task 2: Wire into the build, add git to the worker image

**Files:**
- Modify: `aaiclick/orchestration/execution/docker_build.py` (`_git_clone_at_sha`, `build_image_to_tag`, module docstring step 3)
- Modify: `docker/docker.Dockerfile` (add `git` to the `apt-get install` line; extend the final sanity line with `git --version`)
- Test: `aaiclick/orchestration/execution/test_docker_build.py`

**Interfaces:**
- Consumes: `git_auth_env(remote) -> dict[str, str] | None`
- Produces: `async def _git_clone_at_sha(remote: str, sha: str, workdir: str, env: dict[str, str] | None = None) -> None`

- [ ] **Step 1: Write the failing tests.**
  - `test_git_clone_passes_auth_env_to_fetch_only` — `cli.run` mocked; call with `env={"X": "1"}`; exactly the call whose argv contains `"fetch"` has `env={"X": "1"}`; the other three have no `env` (or `None`).
  - Extend the existing `build_image_to_tag` builds-on-miss test: monkeypatch `docker_build.git_auth_env` to return `{"X": "1"}`; assert `_git_clone_at_sha` was awaited with `env={"X": "1"}`. Cache-hit test: `git_auth_env` not awaited.
- [ ] **Step 2: Run** those tests — expect fail.
- [ ] **Step 3: Implement.** `build_image_to_tag` awaits `git_auth_env(source.git_remote)` after `_require_builder()` and passes it through.
- [ ] **Step 4: Run** `pytest aaiclick/orchestration/execution -q` — expect pass.
- [ ] **Step 5: Commit** `feat: authenticate build clones with the GitHub App token`.

### Task 3: Scaffolds

**Files:**
- Modify: `aaiclick/deploy/templates/compose/docker-compose.yaml` (`worker` service)
- Modify: `aaiclick/deploy/templates/helm/aaiclick/values.yaml`, `.../templates/worker.yaml`
- Test: `aaiclick/deploy/test_compose_scaffold.py`, `aaiclick/deploy/test_k8s_scaffold.py`

- [ ] **Step 1: Write the failing tests.**
  - `test_init_compose_ships_github_app_switched_off` — rendered file contains the three commented lines from the spec's compose block; parsed YAML's `worker.environment` has no `AAICLICK_GITHUB_APP_*` key and `worker.volumes` is still only the docker socket.
  - `test_init_helm_ships_github_app_switched_off` — `values["githubApp"] == {"appId": "", "secretName": ""}`.
  - `test_helm_renders_github_app` — `pytest.mark.skipif(shutil.which("helm") is None)`; `helm template x <chart> --set devDependencies.enabled=true --set githubApp.appId=123 --set githubApp.secretName=gh`; the worker Deployment has both env vars (`AAICLICK_GITHUB_APP_PEM_FILE` = `/run/secrets/github-app/github-app.pem`), a secret volume `gh` with `defaultMode` 288 (`0440`) mounted read-only at `/run/secrets/github-app`, and `securityContext.fsGroup == 1000`. With only `appId` set, `helm template` exits non-zero and stderr names `githubApp.secretName`. With neither, the worker has no `securityContext` and no such volume.
- [ ] **Step 2: Run** — expect fail.
- [ ] **Step 3: Implement** exactly the spec's "Scaffolds" section. `appId` is rendered with `| quote` (helm parses `123` as a number).
- [ ] **Step 4: Run** `pytest aaiclick/deploy -q` — expect pass. If `helm` is not installed locally, say so in the task report; CI covers it.
- [ ] **Step 5: Commit** `feat: scaffold GitHub App build credentials, switched off`.

### Task 4: Docs and cleanup

**Files:**
- Modify: `docs/user_guide/orchestration.md`, `docs/user_guide/deployment.md`, `docs/designs/orchestration.md`, `docs/designs/deploy_scaffold.md`, `docs/designs/future.md`
- Delete: this plan and the spec (`CLAUDE.md`: remove both once implemented, fix references)

- [ ] **Step 1: Write** each entry listed in the spec's "Docs" section, at the place it names, following `markdown-style`.
- [ ] **Step 2: Run** `devpowers:shortify` over the touched docs; `grep -rn "2026-11-13-github-app" .` returns nothing after deletion.
- [ ] **Step 3: Run** the full affected suites: `pytest aaiclick/orchestration/execution aaiclick/deploy -q`.
- [ ] **Step 4: Commit** `docs: GitHub App credentials for private-repo builds`, push, run `check-pr`.
