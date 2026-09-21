Deployment
---

Two scaffolds write a starter stack you own from there:

```bash
python -m aaiclick compose init [--path docker-compose.yaml]   # docker-runner stack
python -m aaiclick k8s init [--path ./aaiclick-chart]          # helm chart
```

Both come up unedited, and neither is production-ready until you replace the
credentials below.

# What you must replace

Every shipped credential reads `change-me-...`. The server logs one `WARNING`
naming each one still unreplaced, on every startup.

| Value                | Compose                                                    | Helm (`values.yaml`)                                 |
|----------------------|------------------------------------------------------------|------------------------------------------------------|
| JWT signing secret   | `server.AAICLICK_JWT_SECRET`                               | `auth.jwtSecret`                                     |
| Seed admin password  | `server.AAICLICK_ADMIN_PASSWORD`                           | `auth.adminPassword`                                 |
| Postgres password    | `postgres.POSTGRES_PASSWORD` + every `AAICLICK_SQL_URL`    | `devDependencies.postgres.password` + `env.sqlUrl`   |
| ClickHouse password  | `clickhouse.CLICKHOUSE_PASSWORD` + every `AAICLICK_CH_URL` | `devDependencies.clickhouse.password` + `env.chUrl`  |

Database passwords appear twice — on the database service and in every URL that
dials it. Change both sides together or the stack stops connecting.

!!! warning "The signing secret is what forges tokens"
    Anyone holding `AAICLICK_JWT_SECRET` can mint an admin token for your
    deployment. Replace it before the server is reachable by anyone else, and
    keep it out of version control.

The seed admin (username `admin`, overridable with `AAICLICK_ADMIN_USERNAME`) is
inserted only on first startup, while the users table is empty. Change its
password afterwards in the UI, not by editing the env var.

# Real deployments

- Point `env.sqlUrl` / `env.chUrl` at managed databases and set
  `devDependencies.enabled=false` — the in-chart databases keep no volume.
- Inject secrets instead of committing them: `helm install --set-file`, an
  external secret manager, or a compose `env_file` kept out of git.
- Auth is enforced whenever the URLs are distributed, and without a signing
  secret the server refuses to start. Full variable list: the
  [auth design reference](https://github.com/kolodkin/aaiclick/blob/main/docs/designs/auth.md).
