Isolated Image Builds on BuildKit
---

# Intent

The injected image-build task runs `docker build` against the worker host's
daemon — the same daemon that runs task containers, reached through a mounted
`docker.sock`, which is root on that host. Because of that, a shell/jvm task may
not return a child that declares an image build (`git_*` / `dockerfile`): the
container's `out.json` would steer the worker into building whatever repo it
names (`execution/runner.py`, `_build_dag_from_dicts`).

Move every build onto a dedicated, rootless **BuildKit daemon** (`buildkitd` from
the `moby/buildkit` image) that is the default build backend. Untrusted
Dockerfile steps then run in a sandbox that shares nothing with the worker: no
host daemon, no worker credentials, no worker network. With builds isolated,
shell/jvm children may declare builds like module children do.

Side benefit: the Kubernetes worker gains in-cluster builds — today the helm
chart has no daemon and documents "use prebuilt-image jobs in-cluster".

# Goals

- All `build` image sources build on a BuildKit daemon, never the host docker daemon.
- Shipped deploy scaffolds (compose, helm) run that daemon by default, rootless.
- Worker secrets never reach a Dockerfile step as a build-arg or env var.
- An image tag identifies exactly what was built, so a build cannot overwrite another's image.
- Lift the shell/jvm "cannot declare an image build" restriction, limited to allowlisted remotes.

# Non-goals

- Per-task build containers (one buildkit container per build task). See "Rejected alternative".
- Build quotas or per-job build caps — tracked in `docs/designs/future.md`.
- Multi-arch builds, remote build cache export, a shared cache across daemons.
- Changing how `prebuilt` images or task containers launch.

# Anti-goals

- A build step that can read `AAICLICK_SQL_URL`, `AAICLICK_CH_URL`, registry
  credentials or private-index tokens. That would make the lifted restriction a
  credential leak.
- Silently falling back to the host daemon when buildkitd is unreachable. A
  missing builder fails the build task with an actionable error, like the
  current Docker preflight.
- Breaking existing user Dockerfiles that build without a private index.

# Threat model

What a malicious repo controls: its Dockerfile, its build context, and (via a
shell/jvm child, once lifted) which remote/SHA/Dockerfile gets built.

| Risk today                                                          | After                                                                  |
|---------------------------------------------------------------------|------------------------------------------------------------------------|
| `RUN` steps on the host daemon that also runs every task container  | `RUN` steps in rootless buildkitd's sandbox, separate daemon           |
| `PIP_INDEX_URL` (may embed a token) passed as `--build-arg`         | Passed as a BuildKit secret, mounted only into the install step        |
| Tag `aaiclick-job:<sha>` ignores remote and Dockerfile → poisoning  | Tag keyed by `image_key` (remote, SHA, Dockerfile)                     |
| Build steps reach worker-network services (Postgres, ClickHouse)    | helm: NetworkPolicy blocks them; compose: unchanged (see below)        |

The tag-poisoning row is a live bug for any job submitter, not just shell
children: a job naming `git_remote=<evil>` with another repo's SHA pushes
`aaiclick-job:<that sha>`, and the next job on the real repo pulls the evil
image as a cache hit. Two Dockerfiles at one SHA collide the same way.

# Design

## Build backend

**Implementation**: `aaiclick/orchestration/execution/docker_build.py`

The build task stays a host-pinned module task (`image_source=NULL`); only the
place the Dockerfile executes moves. Clone, default-Dockerfile scaffolding,
cache checks and push credentials stay in trusted worker code.

The worker already ships `docker buildx`. It talks to buildkitd through the
buildx **remote driver**:

```bash
docker buildx create --name aaiclick --driver remote tcp://buildkitd:1234   # once, idempotent
docker buildx build --builder aaiclick -f <dockerfile> -t <tag> \
  --secret id=pip_index_url,env=AAICLICK_PIP_INDEX_URL \
  --secret id=pip_extra_index_url,env=AAICLICK_PIP_EXTRA_INDEX_URL \
  --build-arg ... (non-secret args only) \
  --push | --load  <context>
```

| Setting                   | Meaning                                                                 |
|---------------------------|-------------------------------------------------------------------------|
| `AAICLICK_BUILDKIT_ADDR`  | buildkitd address, e.g. `tcp://buildkitd:1234`. Required for builds.    |
| `AAICLICK_BUILDKIT_TLS_*` | Optional client cert/key/CA for a TLS buildkitd (remote driver options) |

The builder is created lazily per worker process (`docker buildx inspect
aaiclick` → create on miss). The preflight replaces `_require_docker`'s daemon
check: `docker buildx inspect --bootstrap aaiclick` must succeed, else the task
fails naming `AAICLICK_BUILDKIT_ADDR`.

Per build mode:

| Mode     | Cache check                                         | Build output | Needs host daemon |
|----------|-----------------------------------------------------|--------------|-------------------|
| registry | `docker buildx imagetools inspect <tag>` → done     | `--push`     | no                |
| local    | `docker image inspect <tag>` → done                 | `--load`     | yes (to load)     |

Registry push auth comes from the worker's docker config (`~/.docker/config.json`),
sent by the buildx client over the build session — build steps never see it.
The push itself is made by buildkitd, so the registry name must resolve the
same from the worker (`imagetools inspect`), buildkitd (push) and the task
launcher (pull). Plain-HTTP registries are marked insecure in a `buildkitd.toml`
the scaffolds ship. Registry mode no longer needs a host daemon, which is what
unlocks Kubernetes builds.

Dropping `docker pull` as the cache check also drops its side effect of
pre-loading the image; the docker runner's `docker run` pulls at launch instead.

`AAICLICK_DOCKER_BUILD_ADD_HOST` stays, but the `host-gateway` keyword is
resolved by dockerd, not buildkitd (unverified for the remote driver — check
first). My call: the worker resolves `host-gateway` to the gateway IP itself
and passes a literal `--add-host`. Build steps keep reaching the private index
(CI's test PyPI at `host.docker.internal:8080`) by design.

## Secrets instead of build-args

`PIP_INDEX_URL` and `PIP_EXTRA_INDEX_URL` move from `--build-arg` to
`--secret`. `PIP_TRUSTED_HOST`, `GIT_*`, `AAICLICK_VERSION` and `BASE_IMAGE`
stay build-args (not secret).

- The default Dockerfiles (`docker_scaffold.render_default_dockerfile`) read the
  index URLs with `RUN --mount=type=secret,id=pip_index_url,env=PIP_INDEX_URL`.
- The `docker init` scaffold template does the same.
- Secret mounts with `env=` need the `# syntax=docker/dockerfile:1` frontend line.
- **Compatibility** (my call: hard break, no transition period): user
  Dockerfiles — including ones scaffolded by an older `docker init` — that
  declare `ARG PIP_INDEX_URL` stop receiving it. Release notes and
  `docs/user_guide/container_images.md` show the one-line secret mount. A
  Dockerfile that never used a private index is unaffected.

## Image tag

**Implementation**: `aaiclick/orchestration/docker_config.py`

`compute_image_tag` takes the `ImageBuild` and returns
`[<registry>/]aaiclick-job:<sha>-<image_key[:12]>` — still readable by SHA,
unique per (remote, SHA, Dockerfile). Every caller moves to the new signature:
`image_build_task.run_image_build`, `docker_build.resolve_launch_image`, and the
`runner_env.py` docs. Old `aaiclick-job:<sha>` images are simply never hit
again (one rebuild per SHA after upgrade).

## Lifting the shell/jvm restriction

**Implementation**: `aaiclick/orchestration/execution/runner.py`

Replace the `_BUILD_FIELDS` rejection in `_build_dag_from_dicts` with a
remote allowlist; the module-child restriction stays (module tasks carry
aaiclick's credentials). The clone runs on the worker with its git credentials
and network, so a shell/jvm child may only name a remote that is:

- the parent's own build `git_remote` (same repo, any SHA/Dockerfile), or
- a prefix match in the worker's `AAICLICK_CHILD_BUILD_REMOTES`
  (comma-separated, e.g. `https://github.com/myorg/,git@github.com:myorg/`).

Either way the scheme must be `https://` or scp-style `git@host:` — never
`file://`, `ssh://` to arbitrary hosts, or a local path. Anything else raises
`ValueError` naming the remote and the setting. Default (setting unset): only
the parent's own repo — my call. Job submissions and module children are not
checked; they are already trusted callers. `test_execution.py`'s "cannot declare an image build" test flips
to assert the child is committed with its build source and a build task is
injected for it.

## Deploy scaffolds

**Compose** (`aaiclick/deploy/templates/compose/docker-compose.yaml`): add a
`buildkitd` service on `moby/buildkit:<pinned>-rootless`, with the rootless security options
(`seccomp=unconfined`, `apparmor=unconfined`) so the process sandbox stays on, listening on
`tcp://0.0.0.0:1234`, on a dedicated network shared only with `worker`. Worker
gets `AAICLICK_BUILDKIT_ADDR=tcp://buildkitd:1234`. A named volume keeps the
build cache across restarts. buildkitd needs `extra_hosts:
host.docker.internal:host-gateway` to push to the scaffold's host-published
registry — which means build steps can also reach the host-published Postgres
and ClickHouse ports. Compose has no egress control; my call: compose is the
single-host scaffold and documents this, network isolation is a helm guarantee.

**Helm** (`aaiclick/deploy/templates/helm/aaiclick/templates/`): add a
`buildkitd` Deployment + ClusterIP Service (rootless, same image), a
NetworkPolicy admitting ingress only from worker pods and denying egress to
in-cluster services (registry, git hosts and package indexes stay reachable).
`values.yaml` gets `buildkit.enabled` (default `true`), `buildkit.image`,
`buildkit.resources`. Builds still need `env.registry` (empty by default); my
call: with no registry the chart deploys buildkitd anyway and the build task
fails with the existing missing-registry error. The helm e2e's `localhost:5000`
registry is the pod itself from inside buildkitd — the e2e moves to a
cluster-addressable registry name. Replace the "not provided here; use
prebuilt-image jobs" comments in `worker.yaml` and `docker/kubectl.Dockerfile`.

Pin the buildkit image by version (and digest) in one place per scaffold.

## Docs

- `docs/designs/orchestration.md` "Image source": builds run on buildkitd; new
  tag shape; prerequisites table (`AAICLICK_BUILDKIT_ADDR`; Kubernetes no longer
  needs Docker on the worker host for registry builds).
- `docs/user_guide/container_images.md`: secret mount for private indexes.
- `docs/user_guide/deployment.md`: the buildkitd service.
- `docs/designs/future.md`: build quotas / per-job build caps.

# Rejected alternative: per-task buildkit container

Give the injected build task `image_source = prebuilt moby/buildkit:rootless`
and run `buildctl-daemonless.sh` in it. Rejected because:

- Clone and default-Dockerfile scaffolding are Python; the image would have to
  be a new `aaiclick-buildkit` variant, and the container would need git
  credentials and registry push credentials injected via `command_env`.
- An ephemeral daemon has no layer cache — every build is cold.
- Local mode would have to ship a tarball back through the io dir to `docker load`.
- Each runner (docker, kubernetes) needs a privileged/unconfined launch path for
  this one task type.

A long-lived daemon gets the same isolation with a warm cache and no new
launch path.

# Constraints

- Rootless buildkitd needs `seccomp=unconfined` + `apparmor=unconfined` (or
  `privileged`); scaffolds must document this as the only elevated container.
- The buildx remote driver needs buildx ≥ 0.20 (shipped: 0.20.1).
- Local mode remains single-host: `--load` needs a daemon on the worker host.
- `image_source` JSON and `image_key` are unchanged; no migration.

# Testing

- Unit: rewrite `test_docker_build.py`'s build-arg tests — `_collect_build_args`
  emits no index URL; secret flags are present when the env vars are set; `compute_image_tag` differs for two remotes at one SHA.
- Unit: a shell task returning a child with `git_remote`/`git_sha` commits it
  and injects one build task; parametrize the allowlist — parent's remote and a
  configured prefix pass, `file://`, a local path and an unlisted host raise.
- e2e (CI already builds images): compose stack with buildkitd builds a
  sandbox-style repo with no Dockerfile in registry mode; helm e2e builds
  in-cluster.
- Leak check e2e: a Dockerfile `RUN env; cat /proc/1/environ` step's log
  contains no `AAICLICK_` value and no index URL.

# Left to the builder

- Exact buildkitd flags, image version and digest.
- Whether the lazy `buildx create` runs at worker start or first build.
- Error-message wording.
