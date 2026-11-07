Deployment
---

# Deployment Modes

Three ways to run aaiclick. Local is one process with embedded databases; the
other two are scaffolds that start the full stack — ClickHouse, PostgreSQL,
the API server, an execution worker, the background worker and a `buildkitd`
for `--build` jobs — from the published images:

| Aspect             | Local (default)                 | Docker Compose                                    | Kubernetes                                         |
|--------------------|---------------------------------|---------------------------------------------------|----------------------------------------------------|
| **Data backend**   | chdb (embedded ClickHouse)      | ClickHouse container                              | ClickHouse (in-chart dev instance or managed)      |
| **SQL backend**    | SQLite                          | PostgreSQL container                              | PostgreSQL (in-chart dev instance or managed)      |
| **Tasks run as**   | subprocesses                    | containers on the host's Docker daemon            | Pods — one worker drives many at once              |
| **Start**          | `python -m aaiclick local start`| `compose init` + `docker compose up`              | `k8s init` + `helm install`                        |
| **Auth**           | off — anyone on the port is admin | enforced, seeded `admin` login                  | enforced, seeded `admin` login                     |

Compose and Kubernetes are the two distributed modes: every component reads
`AAICLICK_CH_URL` and `AAICLICK_SQL_URL` for shared servers, and the worker's
`AAICLICK_RUNNER` (`docker` or `kubernetes`) decides where a task runs — see
[Orchestration — Runner modes](orchestration.md#runner-modes).

## Local mode

Single process, no infrastructure required. `local start` runs the combined
REST + MCP server with the background and execution workers built in —
auto-runs setup if needed:

```bash
python -m aaiclick local start [--host HOST] [--port PORT] [--reload]
# Stop with Ctrl+C / SIGTERM.
```

`--host` defaults to `127.0.0.1`.

!!! warning "`--host 0.0.0.0` opens the server with no auth"
    Local mode has auth off: anyone who can reach the port is admin. Use
    a distributed mode for per-user logins. To block DNS rebinding the server
    answers only loopback `Host` headers, so list the names browsers use in
    `AAICLICK_ALLOWED_HOSTS` — see [DNS Rebinding](dns_rebinding.md).

    ```bash
    AAICLICK_ALLOWED_HOSTS=203.0.113.7,aaiclick.lan \
      python -m aaiclick local start --host 0.0.0.0 --port 80
    ```

## Docker Compose mode

Needs [Docker Compose](https://docs.docker.com/compose/). Scaffold and start
in one go:

```bash
python -m aaiclick compose init && docker compose up
```

`init` writes `docker-compose.yaml` (`--path` to change) with every credential
filled with a random value and prints the admin login once:

```text
Wrote /path/to/docker-compose.yaml
Admin login: admin / <generated> (change it after first login)
```

`up` brings up the whole stack — there are no profiles. A one-shot `migrate`
service applies the schema before the server starts. The UI is at
<http://127.0.0.1:5255>; the worker image carries the Docker CLI and runs
`--image` / `--build` tasks as containers on the host's daemon. Drive jobs
from inside the server container:

```bash
docker compose exec server python -m aaiclick register-job my_pipeline.my_pipeline
docker compose exec server python -m aaiclick run-job my_pipeline --progress
```

## Kubernetes mode

```bash
python -m aaiclick k8s init [--path ./aaiclick-chart]
helm install aaiclick ./aaiclick-chart
kubectl port-forward svc/aaiclick-server 5255:5255
```

`init` fills `values.yaml` the same way and prints the admin login. The chart
ships a server Deployment, a worker Deployment on the `aaiclick-kubectl`
image with `AAICLICK_RUNNER=kubernetes`, the background worker, `buildkitd`,
and dev PostgreSQL + ClickHouse under `devDependencies` (no volumes — point
`env.sqlUrl` / `env.chUrl` at managed databases and set
`devDependencies.enabled=false` for anything real).

One worker replica drives many Pods at once, bounded by what the cluster
schedules: it stops claiming while a job has `AAICLICK_K8S_MAX_PENDING` (10)
unscheduled Pods, or `AAICLICK_K8S_MAX_PENDING_TOTAL` (4× that) in all, and
`AAICLICK_K8S_MAX_PODS` optionally caps live Pods. A Pod not started within
`AAICLICK_K8S_PENDING_TIMEOUT` (900 s) fails with the scheduler's reason and
retries as any failure. Pods survive a worker restart; the next worker adopts
them. Worker replicas are for availability, not throughput.

The worker's ServiceAccount needs the RBAC in `rbac.yaml`, or every `kubectl`
call returns 403 — see [Container Images](container_images.md). Run jobs with
`kubectl exec` into the server Pod, the same commands as in compose.

## Worker commands

Both scaffolds run these; on your own hosts, run them against shared
PostgreSQL + ClickHouse URLs:

```bash
python -m aaiclick migrate upgrade head          # once, before the first start
python -m aaiclick execution-worker start [--max-tasks N]
python -m aaiclick execution-worker stop <execution_worker_id>
python -m aaiclick execution-worker list
python -m aaiclick background start              # scheduler + cleanup
```

!!! warning "`execution-worker start`/`background start` require distributed backends"
    In local mode, use `local start` instead.

## Worker logs

Every long-running command (`local start`, `execution-worker start`,
`background start`) writes timestamped `INFO` lines to stderr. The execution
worker logs `executing task` before each task and `completed task` (or the
failure state) after it, so a slow task shows up as a gap between its two
lines. Local mode also logs `chdb session ready in Ns` when the embedded
engine starts. The first task after a start is still slower than the rest: the
engine's first DDL and first insert fault more of its ~540 MB library into
memory.

# Configuration

| Variable                             | Default                               | Description                                                                                                                                                             |
|--------------------------------------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `AAICLICK_LOCAL_ROOT`                | `~/.aaiclick`                         | Base directory for all local-mode state                                                                                                                                 |
| `AAICLICK_SQL_URL`                   | `sqlite+aiosqlite:///{root}/local.db` | SQLAlchemy async URL for orchestration DB                                                                                                                               |
| `AAICLICK_CH_URL`                    | `chdb://{root}/chdb_data`             | ClickHouse connection URL for data ops                                                                                                                                  |
| `AAICLICK_JOB_TTL_DAYS`              | `90`                                  | Days after completion before a job's tables, logs and lineage are deleted                                                                                               |
| `AAICLICK_DEFAULT_PRESERVATION_MODE` | unset                                 | Level-3 preservation-mode default                                                                                                                                       |
| `AAICLICK_JOB_WAIT_TIMEOUT`          | `3600`                                | Seconds `job wait` / `run-job --progress` watch a job before giving up. Bounds the wait, not the job — a timeout stops watching and exits 1 while the job keeps running |
| `AAICLICK_DOCKER_BIN`                | `docker`                              | Docker CLI used by the docker runner                                                                                                                                    |
| `AAICLICK_REGISTRY`                  | unset                                 | Registry build mode for `build` images — pushed after build, pulled as cache by docker & kubernetes runners; required for kubernetes `build`                            |
| `AAICLICK_LOCAL_BUILD`               | unset                                 | Local build mode for `build` images — kept in the worker's Docker daemon, never pushed; mutually exclusive with `AAICLICK_REGISTRY`                                     |
| `AAICLICK_BUILDKIT_ADDR`             | unset                                 | buildkitd address for `build` images (e.g. `tcp://buildkitd:1234`); required to build                                                                                   |
| `AAICLICK_CHILD_BUILD_REMOTES`       | unset                                 | Comma-separated remote prefixes a shell/jvm task's children may build from, besides its own repo                                                                        |

# Credentials

The raw templates spell each credential `change-me-...`. The server refuses to
start while any of them is still a placeholder, so a template copied by hand
fails loudly instead of running with public secrets.

| Value                | Compose                                                    | Helm (`values.yaml`)                                 |
|----------------------|------------------------------------------------------------|------------------------------------------------------|
| JWT signing secret   | `server.AAICLICK_JWT_SECRET`                               | `auth.jwtSecret`                                     |
| Seed admin password  | `server.AAICLICK_ADMIN_PASSWORD`                           | `auth.adminPassword`                                 |
| Postgres password    | `postgres.POSTGRES_PASSWORD` + every `AAICLICK_SQL_URL`    | `devDependencies.postgres.password` + `env.sqlUrl`   |
| ClickHouse password  | `clickhouse.CLICKHOUSE_PASSWORD` + every `AAICLICK_CH_URL` | `devDependencies.clickhouse.password` + `env.chUrl`  |

Database passwords appear twice — on the database service and in every URL that
dials it. To set your own, change both sides together. In the URL,
percent-encode `@`, `:` and `/` (`p@ss` → `p%40ss`); the service side takes the
password as is.

!!! warning "The signing secret is what forges tokens"
    Anyone holding `AAICLICK_JWT_SECRET` can mint an admin token for your
    deployment. Keep the generated value out of version control.

The seed admin (username `admin`, overridable with `AAICLICK_ADMIN_USERNAME`) is
inserted only on first startup, while the users table is empty. Change its
password afterwards in the UI, not by editing the env var.

# Hardening

- `--build` jobs build on the scaffold's rootless `buildkitd`
  (`AAICLICK_BUILDKIT_ADDR`), never on the worker's Docker daemon. In compose
  its build steps can still reach host-published ports (Postgres,
  ClickHouse); the chart blocks private ranges with a NetworkPolicy
  (`buildkit.networkPolicy`). Plain-HTTP registries go in
  `buildkit.insecureRegistries`.
- `AAICLICK_SANDBOX=<git remote>` on the API server enables the
  [sandbox](sandbox.md) page; the server pushes submissions there and the
  workers build them.
- Inject secrets instead of committing them: `helm install --set-file`, an
  external secret manager, or a compose `env_file` kept out of git.
- Auth is enforced whenever the URLs are distributed, and without a signing
  secret the server refuses to start. Full variable list: the
  [auth design reference](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/auth.md).
