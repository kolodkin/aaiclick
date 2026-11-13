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
- **Non-goals**: the submission-side `git ls-remote`, GitHub Enterprise
  Server, submodules, Git LFS, private dependencies fetched inside the
  Dockerfile.
- **Anti-goal**: the token or key appearing in a command line, a remote URL,
  an error message or a task log.

`git ls-remote` runs on the submitter whenever the remote is known (passed or
registered) and the SHA is not (`resolve_image_source`). It has no App
credentials, so a private-repo job is submitted with `--git-sha`, or from a
checkout with no remote passed or registered (the working tree supplies the
SHA).

# Approaches

| Approach                                                 | Trade-off                                                    |
|----------------------------------------------------------|--------------------------------------------------------------|
| **A. Mint in the build task, pass through git's env**    | Smallest; token stays out of argv and logs. **Chosen.**      |
| B. `python -m aaiclick git-credential` wired into images | Covers every git call, but adds a CLI command + image change |
| C. Token in the clone URL                                | `CommandError` prints the command, so the token hits logs    |

A fits the usage (CLI submission, worker-side clone). B is the upgrade path
if submission by branch ever needs private repos.

# Design

**Settings** (worker env): `AAICLICK_GITHUB_APP_ID`,
`AAICLICK_GITHUB_APP_PEM_FILE`.

**New module** `aaiclick/orchestration/execution/github_app.py`, one public
function:

```python
async def git_auth_env(remote: str) -> dict[str, str] | None:
    """Env overlay that lets git authenticate to ``remote``, or None."""
```

Checks run in this order:

1. Neither setting set → `None`; behaviour unchanged.
2. Exactly one set → raise, naming the missing one, whatever the remote: a
   half-configured worker fails loudly on its first build.
3. `remote` is not a `github.com` repo → `None`.

A `github.com` repo is any of `https://github.com/<owner>/<repo>`,
`git@github.com:<owner>/<repo>` or `ssh://git@github.com/<owner>/<repo>` —
host matched case-insensitively, optional `user@`, `.git` suffix and trailing
slash. SSH forms count because a checkout's `origin`, which the CLI stamps
verbatim, is usually one.

For a match it:

1. Signs an RS256 JWT (`iss` = App ID, `iat` = now − 60 s, `exp` = now + 9 min).
2. `GET /repos/<owner>/<repo>/installation` → installation ID. A 404 means
   the App is not installed on this repo: print a one-line warning to the
   task log and return `None`, so a public repo still clones anonymously and
   a private one fails in git with the warning right above it.
3. `POST /app/installations/<id>/access_tokens` with
   `{"repositories": ["<repo>"], "permissions": {"contents": "read"}}`.
4. Returns `GIT_TERMINAL_PROMPT=0`, the token in `AAICLICK_GIT_TOKEN`, and
   `GIT_CONFIG_COUNT` / `GIT_CONFIG_KEY_n` / `GIT_CONFIG_VALUE_n` entries,
   numbered after any already in the worker's env:
    - `credential.https://github.com.helper` = empty, resetting helpers from
      the worker's gitconfig so none is asked first or sent the token to
      store;
    - the same key = an inline helper printing the token from the env var;
    - `url.https://github.com/<owner>/<repo>.insteadOf` = the remote
      verbatim (skipped when identical), so an SSH or oddly spelled remote
      is fetched over canonical HTTPS with the token.

A token is minted per build, after the cache check, so nothing is cached and
the one-hour expiry never matters.

**Call site**: `_git_clone_at_sha` takes the overlay and passes it as `env=`
to the `git fetch` only; `build_image_to_tag` computes it from
`source.git_remote`.

**Errors** raise `RuntimeError` with the fix in the message: PEM path
missing, unreadable or not a file (compose creates a directory when the
mounted host file is absent), any non-2xx other than the 404 above (status +
GitHub's `message`). No error text includes the JWT or token.

**Dependencies**: `pyjwt` → `pyjwt[crypto]` (RS256 needs `cryptography`).
HTTP goes through stdlib `urllib.request` in `asyncio.to_thread` — two short
calls do not justify a client dependency in the core install.

**Worker image**: `docker/docker.Dockerfile` adds `git` to its `apt-get
install`. The released worker images do not ship it, so `--build` there fails
at `git init` before credentials matter.

# Trust

- `AAICLICK_CHILD_BUILD_REMOTES` still decides which repos a shell/jvm child
  may name.
- **The key is readable by tasks that run in-place on the worker** (no image
  → subprocess of the worker, same user). Such a task can mint its own tokens
  with the App's full permissions on every repo it is installed on. The
  per-repo read-only scoping protects the build path only. Mitigations, both
  documented: install the App on the repos that need building and grant it
  `Contents: read` only; keep jobs that run in-place off a worker holding the
  key.

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
Secret read-only at `/run/secrets/github-app` (`defaultMode: 0440`) and sets
Pod `securityContext.fsGroup: 1000` — the group of the image's non-root
`aaiclick` user, which could not read a root-owned file otherwise. Exactly one set fails
`helm template` with a `fail` message naming the other.

# Tests

- `test_github_app.py`, with a generated RSA key and stubbed HTTP: the three
  checks in order; each remote form matches or not; the JWT verifies against
  the public key; the token request is scoped to the repo and
  `contents: read`; the config entries (reset, helper, `insteadOf`) start at
  the next free index; a 404 warns and returns `None`; each error path
  raises without leaking the token.
- `test_docker_build.py`: the fetch receives the overlay; other git calls
  and the no-settings path are unchanged.
- `test_compose_scaffold.py`: the block ships commented out.
- `test_k8s_scaffold.py`: `githubApp` ships empty; with `helm` on `PATH`
  (GitHub's runners have it), `helm template` renders the worker with both
  values set and fails with one. This runs in PR CI, unlike
  `_helm-e2e-reusable.yaml`, which only the release pipeline calls.

# Docs

The orchestration guide explains the feature once; each deployment mode's
section carries its own steps and links back.

- **`docs/user_guide/orchestration.md`**, "Image source (docker /
  kubernetes)", after the build-mode table: the two settings; the App needs
  `Contents: read` and installation on the repo; accepted remote forms;
  tokens are per build, read-only, one repo. Two `!!! warning` blocks: the
  `--git-sha` rule from Scope, and the in-place task exposure from Trust.
- **`docs/user_guide/deployment.md`**, "Docker Compose mode": copy-paste
  steps — key at `./secrets/github-app.pem` (`chmod 600`, git-ignored),
  uncomment the three worker lines, `docker compose up -d worker` — plus a
  **Configure** row.
- **`docs/user_guide/deployment.md`**, "Kubernetes mode": `kubectl create
  secret generic ...`, then `--set githubApp.appId=...,githubApp.secretName=...`,
  plus a **Configure** row.
- **`docs/designs/deploy_scaffold.md`**, "Compose template (docker-runner
  setup)" and "Helm chart template (kubernetes setup)": one line each — ships switched off, the key never enters a
  scaffolded file, helm sets `fsGroup: 1000`.
- **`docs/designs/orchestration.md`**, "Image source", the "Builds run on
  buildkitd" paragraph: one sentence on where the token is minted, its scope,
  and that only the `git fetch` sees it.
- **`docs/designs/future.md`**, "GitHub Actions Image Builder": the "until
  then" line points here.
- **`github_app.py`** docstring: `See docs/designs/orchestration.md — "Image
  source".`

# Left to the builder

Exact error wording, the inline helper's shell text, and how HTTP is stubbed
in tests.
