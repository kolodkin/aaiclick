Deployment
---

Two scaffolds write a starter stack you own from there:

```bash
python -m aaiclick compose init [--path docker-compose.yaml]   # docker-runner stack
python -m aaiclick k8s init [--path ./aaiclick-chart]          # helm chart
```

Both come up unedited. `init` fills every credential with a random value and
prints the admin login once:

```text
Wrote /path/to/docker-compose.yaml
Admin login: admin / <generated> (change it after first login)
```

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
dials it. To set your own, change both sides together or the stack stops connecting. In the URL,
percent-encode `@`, `:` and `/` (`p@ss` → `p%40ss`); the service side takes the
password as is.

!!! warning "The signing secret is what forges tokens"
    Anyone holding `AAICLICK_JWT_SECRET` can mint an admin token for your
    deployment. Keep the generated value out of version control.

The seed admin (username `admin`, overridable with `AAICLICK_ADMIN_USERNAME`) is
inserted only on first startup, while the users table is empty. Change its
password afterwards in the UI, not by editing the env var.

# Real deployments

- Both scaffolds set the worker's `AAICLICK_RUNNER` (`docker` in compose,
  `kubernetes` in the chart), which turns `--image` / `--build` jobs into
  containers or Pods. Without it a worker runs subprocess tasks only.
- `AAICLICK_SANDBOX=<git remote>` on the API server enables the
  [sandbox](sandbox.md) page; the server pushes submissions there and the
  workers build them.
- Point `env.sqlUrl` / `env.chUrl` at managed databases and set
  `devDependencies.enabled=false` — the in-chart databases keep no volume.
- Inject secrets instead of committing them: `helm install --set-file`, an
  external secret manager, or a compose `env_file` kept out of git.
- Auth is enforced whenever the URLs are distributed, and without a signing
  secret the server refuses to start. Full variable list: the
  [auth design reference](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/auth.md).
