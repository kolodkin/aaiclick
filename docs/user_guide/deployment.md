Deployment
---

# Deployment Modes

Three ways to run aaiclick. Local is one process with embedded databases; the
other two start the full stack — ClickHouse, PostgreSQL,
the API server, an execution worker, the background worker and a `buildkitd`
for `--build` jobs — from the published images:

| Aspect             | Local (default)                 | Docker Compose                                    | Kubernetes                                         |
|--------------------|---------------------------------|---------------------------------------------------|----------------------------------------------------|
| **Data backend**   | chdb (embedded ClickHouse)      | ClickHouse container                              | ClickHouse (in-chart dev instance or managed)      |
| **SQL backend**    | SQLite                          | PostgreSQL container                              | PostgreSQL (in-chart dev instance or managed)      |
| **Tasks run as**   | subprocesses                    | containers on the host's Docker daemon            | Pods — one worker drives many at once              |
| **Start**          | `python -m aaiclick local start`| `sh env.init.sh` + `docker compose up`            | `helm install oci://…/aaiclick`                    |
| **Auth**           | off — anyone on the port is admin | enforced, seeded `admin` login                    | enforced, seeded `admin` login                     |

Compose and Kubernetes are the distributed modes:

- Every component reads `AAICLICK_CH_URL` and `AAICLICK_SQL_URL` for shared
  servers; the worker's `AAICLICK_RUNNER` (`docker` or `kubernetes`) decides
  where a task runs — see [Orchestration — Runner modes](orchestration.md#runner-modes).
- For private buckets, give the ClickHouse server its S3 identity and a
  `users.xml` profile line — see [Object — Blob Storage](object.md#credentials).
- Credentials are random unless you set them: compose's `env.init.sh` fills
  `.env`, and the chart generates a Secret. `.env.template` spells them
  `change-me-...`, and the server refuses to start while any is still one.
- Each secret is set in one place — compose's `.env`, the chart's Secret —
  and the connection URLs are built from it.
- The seed admin (`admin`, or `AAICLICK_ADMIN_USERNAME`) is inserted only on
  first startup while the users table is empty; change its password later in
  the UI, not the env var.

!!! warning "The signing secret is what forges tokens"
    Anyone holding `AAICLICK_JWT_SECRET` can mint an admin token for your
    deployment. Keep it out of version control — the compose `.env` kept out
    of git, or the chart's `auth.existingSecret` from an external secret
    manager.

## Local mode

Single process, no infrastructure. `local start` runs the REST + MCP server
with the background and execution workers built in, running setup if needed:

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

**Configure** (environment of the one process):

| Variable                 | Default                               | Description                                                                |
|--------------------------|---------------------------------------|----------------------------------------------------------------------------|
| `AAICLICK_LOCAL_ROOT`    | `~/.aaiclick`                         | Base directory for all local-mode state                                    |
| `AAICLICK_CH_URL`        | `chdb:///{root}/chdb_data`            | Embedded ClickHouse data directory                                         |
| `AAICLICK_SQL_URL`       | `sqlite+aiosqlite:///{root}/local.db` | Orchestration database file                                                |
| `AAICLICK_ALLOWED_HOSTS` | loopback names                        | Extra `Host` values the server answers — [DNS Rebinding](dns_rebinding.md) |

## Docker Compose mode

Needs [Docker Compose](https://docs.docker.com/compose/) and `openssl`.
Fetch the reference stack from a release tag, generate `.env`, and start:

```bash
V=v1.4.2
for f in docker-compose.yaml .env.template env.init.sh; do
  curl -fsSLO https://raw.githubusercontent.com/kolodkin/aaiclick/$V/deploy/compose/$f
done
sh env.init.sh && docker compose up
```

`env.init.sh` writes `.env` from `.env.template` with every credential as a
random value, then prints the admin login once. It never overwrites an
existing `.env`. The compose file only references `.env`, so it can be
committed; `up` without `.env` fails naming the missing variable. Set
`AAICLICK_IMAGE_TAG` in `.env` to the release you fetched (`latest` by
default).

```text
Wrote .env (keep it out of git)
Admin login: admin / <generated>
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

**Configure** (secrets in `.env`, the rest in `docker-compose.yaml`):

| Key                                                         | Purpose                                                                                                                   |
|-------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------|
| `.env`: `AAICLICK_IMAGE_TAG`                                | Image tag for every aaiclick service — pin a release                                                                      |
| `.env`: `AAICLICK_JWT_SECRET`                               | Token signing secret                                                                                                      |
| `.env`: `AAICLICK_ADMIN_PASSWORD`                           | Seed admin password                                                                                                       |
| `.env`: `AAICLICK_PG_PASSWORD`                              | PostgreSQL password, read by the service and every URL — use URL-safe characters (letters, digits, `-`, `_`)              |
| `.env`: `AAICLICK_CH_PASSWORD`                              | ClickHouse password, same rule                                                                                            |
| `worker.AAICLICK_REGISTRY`, `worker.AAICLICK_BUILDKIT_ADDR` | Preset to the stack's registry and rootless `buildkitd` — [Image source](orchestration.md#image-source-docker-kubernetes) |
| `worker.AAICLICK_GITHUB_APP_ID`, `_PEM_FILE`                | Commented out: GitHub App key for private-repo builds — [Image source](orchestration.md#image-source-docker-kubernetes)   |
| `server.AAICLICK_SANDBOX`                                   | Git remote that enables the [sandbox](sandbox.md) page                                                                    |

Build steps run on `buildkitd`, never on the worker's Docker daemon, but can
still reach host-published ports (Postgres, ClickHouse).

**Private GitHub repos for `--build`**: put the GitHub App key beside the
compose file, uncomment the worker's key mount and its two
`AAICLICK_GITHUB_APP_*` lines (set the App ID), then recreate the worker:

```bash
mkdir -p secrets && cp /path/to/app.private-key.pem secrets/github-app.pem
chmod 600 secrets/github-app.pem && echo "secrets/" >> .gitignore
docker compose up -d worker
```

!!! warning "Place the key before `up`"
    With the mount uncommented and no file at `./secrets/github-app.pem`,
    Docker creates a directory there and builds fail with "is not a file".

## Kubernetes mode

Needs [Helm](https://helm.sh/docs/) and a `kubectl` context on the cluster.
Install the published chart, then reach the UI through a port-forward:

```bash
helm install aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version 1.4.2 \
  --set devDependencies.enabled=true
kubectl port-forward svc/aaiclick-server 5255:5255
```

The chart version matches the aaiclick release and pins its images. Without
`auth.existingSecret`, the chart generates the Secret `aaiclick-credentials`,
and `helm upgrade` keeps its values. The install notes print how to read the
admin password:

```bash
kubectl get secret aaiclick-credentials -o jsonpath='{.data.admin-password}' | base64 -d
```

**Production credentials**: create the Secret outside Helm (External Secrets
Operator, Sealed Secrets, Vault, `kubectl`) with keys `jwt-secret`,
`admin-password`, `sql-url` and `ch-url` (plus `pg-password` and
`ch-password` with `devDependencies`), and name it with
`--set auth.existingSecret=<name>`. The chart then renders no credentials.

!!! warning "`helm template` and Argo CD rotate generated credentials"
    They render without cluster access, so the chart can't find the stored
    Secret and generates new values on every render. Set
    `auth.existingSecret` for GitOps installs.

The chart ships a server Deployment, a worker Deployment on the
`aaiclick-kubectl` image with `AAICLICK_RUNNER=kubernetes`, the background
worker, `buildkitd`, and dev PostgreSQL + ClickHouse under `devDependencies`,
off by default and switched on above for evaluation. They keep no volumes: for anything real,
leave them off and point `env.sqlUrl` / `env.chUrl` at managed databases.

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

**Private GitHub repos for `--build`**: store the GitHub App key as a Secret
with the key name `github-app.pem`, then name it and the App ID at install or
upgrade:

```bash
kubectl create secret generic aaiclick-github-app --from-file=github-app.pem=/path/to/app.private-key.pem
helm upgrade aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version 1.4.2 --reuse-values \
  --set githubApp.appId=123456 --set githubApp.secretName=aaiclick-github-app
```

**Configure** (`values.yaml`, or `--set` / `--set-file` at install):

| Key                                                                               | Purpose                                                                         |
|-----------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| `env.sqlUrl`, `env.chUrl`                                                         | Full URLs for managed databases; empty builds them from `devDependencies`       |
| `env.registry`                                                                    | Registry the worker pushes `--build` images to; empty disables building         |
| `githubApp.appId`, `githubApp.secretName`                                         | GitHub App key for private-repo builds; both or neither                         |
| `auth.existingSecret`                                                             | Secret managed outside Helm holding every credential — use it in production     |
| `auth.jwtSecret`, `auth.adminPassword`                                            | Evaluation overrides; empty values are generated                                |
| `devDependencies.enabled` + `.postgres` / `.clickhouse`                           | In-chart dev databases without volumes; generated passwords also feed the URLs  |
| `images.*`, `imagePullSecrets`                                                    | Image repositories and tags, pull secret for private registries                 |
| `worker.replicas`, `server.replicas`                                              | Availability, not throughput — one worker already drives many Pods              |
| `rbac.create`, `serviceAccount.name`                                              | The worker's Pod permissions                                                    |
| `buildkit.networkPolicy`, `buildkit.insecureRegistries`                           | Block private ranges from build steps; allow plain-HTTP registries              |
| `AAICLICK_K8S_MAX_PENDING`, `_MAX_PENDING_TOTAL`, `_MAX_PODS`, `_PENDING_TIMEOUT` | Pod limits above, as env on the worker Deployment                               |

**Upgrading from `k8s init`**: earlier releases copied the chart into your
project. Delete the `images.*.tag` lines from that `values.yaml`, then
upgrade onto the published chart with it; the credentials it holds win over
generation, so the databases keep their passwords:

```bash
helm upgrade aaiclick oci://ghcr.io/kolodkin/charts/aaiclick --version 1.4.2 -f aaiclick-chart/values.yaml
```
