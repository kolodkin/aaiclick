GitHub App Credentials for Image Builds
---

# Intent

A `--build` job whose repo is private on `github.com` fails today: the build
task's `git fetch` on the worker has no credentials. Given a GitHub App key,
the worker should clone such repos on its own — no helper script, no custom
worker image, two settings.

# Scope

- **Goal**: the worker-side clone in `execution/docker_build.py` authenticates
  with a GitHub App installation token.
- **Non-goals**: the submission-side `git ls-remote` (private repos are
  submitted from a checkout with the CLI, which never contacts the remote),
  GitHub Enterprise Server, SSH remotes, submodules, private dependencies
  fetched inside the Dockerfile.
- **Anti-goal**: the token or key appearing in a command line, a remote URL,
  an error message or a task log.

# Approaches

| Approach                                                 | Trade-off                                                    |
|----------------------------------------------------------|--------------------------------------------------------------|
| **A. Mint in the build task, pass through git's env**    | Smallest; token stays out of argv and logs. **Chosen.**      |
| B. `python -m aaiclick git-credential` wired into images | Covers every git call, but adds a CLI command + image change |
| C. Token in the clone URL                                | `CommandError` prints the command, so the token hits logs    |

A fits the usage (CLI submission, worker-side clone). B is the upgrade path
if server-side submission by branch ever needs private repos.

# Design

**Settings** (worker env): `AAICLICK_GITHUB_APP_ID`,
`AAICLICK_GITHUB_APP_PEM_FILE`. Neither set: behaviour unchanged. Exactly one
set: the build fails naming the missing one.

**New module** `aaiclick/orchestration/execution/github_app.py`, one public
function:

```python
async def git_auth_env(remote: str) -> dict[str, str] | None:
    """Env overlay that lets git authenticate to ``remote``, or None."""
```

It returns `None` unless both settings are set and `remote` is
`https://github.com/<owner>/<repo>[.git]`. Otherwise it:

1. Signs an RS256 JWT (`iss` = App ID, `iat` = now − 60 s, `exp` = now + 9 min).
2. `GET /repos/<owner>/<repo>/installation` → installation ID.
3. `POST /app/installations/<id>/access_tokens` with
   `{"repositories": ["<repo>"], "permissions": {"contents": "read"}}`.
4. Returns `GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_n` / `GIT_CONFIG_VALUE_n`
   registering an inline credential helper for `https://github.com` that
   prints the token from `AAICLICK_GIT_TOKEN`, plus that variable and
   `GIT_TERMINAL_PROMPT=0`. `n` continues after any `GIT_CONFIG_COUNT`
   already in the worker's env.

A token is minted per build, after the cache check, so nothing is cached and
the one-hour expiry never matters.

**Call site**: `_git_clone_at_sha` takes the overlay and passes it as `env=`
to the `git fetch` only; `build_image_to_tag` computes it from
`source.git_remote`.

**Errors** raise `RuntimeError` with the fix in the message: PEM file missing
or unreadable, App not installed on the repo (404), any other non-2xx
(status + GitHub's `message`). No error text includes the JWT or token.

**Dependencies**: `pyjwt` → `pyjwt[crypto]` (RS256 needs `cryptography`).
HTTP goes through stdlib `urllib.request` in `asyncio.to_thread` — two short
calls do not justify a client dependency in the core install.

**Trust**: `AAICLICK_CHILD_BUILD_REMOTES` still decides which repos a
shell/jvm child may name; the App widens what the worker *can* clone, not
what a container may ask for.

# Scaffolds

Both follow precedent already in the templates (the commented-out
`AWS_ACCESS_KEY_ID` lines in compose, the off-by-default S3 profile in helm).
The key itself never enters a scaffolded file.

**Compose** (`templates/compose/docker-compose.yaml`, `worker` service):
commented out, with a remark saying what uncommenting enables.

```yaml
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      # Private GitHub repos for --build: mount a GitHub App key and uncomment
      # the two AAICLICK_GITHUB_APP_* lines below (docs/user_guide/orchestration.md).
      # - ./secrets/github-app.pem:/run/secrets/github-app.pem:ro
    environment:
      ...
      # AAICLICK_GITHUB_APP_ID: "..."
      # AAICLICK_GITHUB_APP_PEM_FILE: /run/secrets/github-app.pem
```

**Helm**: optional, off by default. `values.yaml` gains

```yaml
# Private GitHub repos for --build. Create the Secret yourself:
#   kubectl create secret generic aaiclick-github-app --from-file=github-app.pem=<key>
# Both empty leaves the worker unchanged.
githubApp:
  appId: ""
  secretName: ""
```

When both are set, `templates/worker.yaml` adds the two env vars, mounts the
Secret read-only at `/run/secrets/github-app` (`defaultMode: 0400`) and sets
Pod `securityContext.fsGroup: 1000` — the image's `aaiclick` user, which
could not read a root-owned `0400` file otherwise. Exactly one set fails
`helm template` with a `fail` message naming the other.

# Tests

- `test_github_app.py`: with a generated RSA key and stubbed HTTP — returns
  `None` for unset settings / non-GitHub / SSH remotes; the JWT verifies
  against the public key; the token request is scoped to the repo and
  `contents: read`; the env registers the helper at the next free index;
  each error path raises without leaking the token.
- `test_docker_build.py`: the fetch receives the overlay; other git calls
  and the no-settings path are unchanged.
- `test_compose_scaffold.py` / `test_k8s_scaffold.py`: the scaffold ships
  the block switched off (compose lines commented, `githubApp` values
  empty). The enabled helm rendering is covered by `helm lint` /
  `helm template` in `_helm-e2e-reusable.yaml`, extended with one
  `--set githubApp.appId=1,githubApp.secretName=x` render.

# Docs

Each fact lives where the neighbouring build settings already do. The
orchestration guide explains the feature once; each deployment mode's section
carries its own steps and links back, so a compose or Kubernetes reader never
has to translate the other's instructions.

| File                                  | Place                                              | Content                                                                                                                                                                                |
|---------------------------------------|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `docs/user_guide/orchestration.md`    | "Image source (docker / kubernetes)", after the build-mode table | The main entry: the two settings; the App needs `Contents: read` and installation on the repo; the remote must be `https://github.com/...`; tokens are per build, read-only, one repo |
| same                                  | same, as a `!!! warning`                           | Submitting by branch without a checkout runs `git ls-remote` on the submitter, which has no App credentials: pass `--git-sha` or submit from a checkout                                |
| `docs/user_guide/deployment.md`       | "Docker Compose mode"                              | The compose how-to, as copy-paste steps: put the key at `./secrets/github-app.pem` (`chmod 600`, git-ignored), uncomment the three worker lines, `docker compose up -d worker`; plus a **Configure** row |
| same                                  | "Kubernetes mode"                                  | The helm how-to: `kubectl create secret generic ...`, then `--set githubApp.appId=...,githubApp.secretName=...` on install/upgrade; plus a **Configure** row for `githubApp.*`           |
| `docs/designs/deploy_scaffold.md`     | "Compose template" and "Helm chart template"       | One line each: the block ships switched off, the key never enters a scaffolded file, and (helm) `fsGroup: 1000` is set so the non-root worker can read it                              |
| `docs/designs/orchestration.md`       | the "Builds run on buildkitd" paragraph            | One sentence: where the token is minted, its scope, and that it reaches only the `git fetch`                                                                                           |
| `docs/designs/future.md`              | "GitHub Actions Image Builder"                     | The "until then" line points at this instead of a hand-rolled credential helper                                                                                                        |
| `execution/github_app.py`             | module docstring                                   | `See docs/designs/orchestration.md — "Image source".`                                                                                                                                  |

`docs/user_guide/container_images.md` is not touched: it describes the
released images, not build settings.

# Left to the builder

Exact error wording, the inline helper's shell text, and how HTTP is stubbed
in tests.
