Tenant RBAC
---

Multi-tenant role-based access control on top of the existing auth system
(`docs/designs/auth.md`). Tenants are the isolation unit: registered jobs,
jobs (with their tasks, groups, and lineage), and persistent objects belong
to exactly one tenant. Users hold a per-tenant role via memberships; an
instance-level superadmin flag replaces the old global `admin` role.
Execution workers remain shared infrastructure.

# Scope

- **Tenant**: named isolation unit with a unique `slug` (used in the CLI
  and UI; resource scoping keys on the immutable `id`). Owns registered
  jobs, jobs, and (phase 2) persistent objects.
- **Membership**: a user belongs to any number of tenants, each with a role
  — the existing `Role` literal (`admin` | `viewer`) reused per tenant.
- **Superadmin**: boolean on `users`. Superadmins manage tenants and users,
  act as `admin` in every tenant, and exclusively control shared
  infrastructure (execution workers, `/mcp`).
- **Isolation level**: metadata-level. One SQL database, one ClickHouse
  database; every query filters by the active tenant. Tenants are trusted
  teams sharing a deployment, not hostile parties.
- **Workers**: not tenant-scoped. Any worker executes any tenant's tasks.
- **Local mode**: auth stays disabled (`docs/designs/auth.md` — Scope);
  everything runs in the seeded default tenant, preserving zero-config
  behavior.

Out of scope (see `docs/designs/future.md` where planned): custom roles,
per-resource ACLs, per-tenant workers or quotas, ClickHouse
database-per-tenant isolation.

# Data Model

New tables live in `aaiclick/auth/models.py` beside `users` /
`refresh_tokens`. IDs are snowflake `BigInteger` PKs; role columns are plain
`String` typed with the `Role` literal (no DB CHECK — see CLAUDE.md,
"Prefer Literal").

## `tenants`

| Column       | Type                        | Notes                          |
|--------------|-----------------------------|--------------------------------|
| `id`         | `BigInteger` PK (snowflake) |                                |
| `slug`       | `String`, unique, indexed   | Short name; `[a-z0-9_]+`       |
| `name`       | `String`                    | Display name                   |
| `created_at` | `datetime` (`utc_now`)      |                                |

## `tenant_memberships`

| Column       | Type                                    | Notes                  |
|--------------|-----------------------------------------|------------------------|
| `id`         | `BigInteger` PK (snowflake)             |                        |
| `tenant_id`  | `BigInteger` FK → `tenants.id`, indexed |                        |
| `user_id`    | `BigInteger` FK → `users.id`, indexed   |                        |
| `role`       | `String`                                | `Role` literal         |
| `created_at` | `datetime` (`utc_now`)                  |                        |

Unique constraint on `(tenant_id, user_id)`.

## `users` changes

`role` is replaced by `superadmin: bool` (default `false`). A user's
effective role in a tenant is their membership role, or `admin` everywhere
when `superadmin` is set.

## Resource scoping

`tenant_id` (`BigInteger` FK → `tenants.id`, non-null, indexed) is added to
**`registered_jobs` and `jobs` only**. Tasks, groups, dependencies, remote
task results, and lineage all reach their job via `job_id`, so they derive
tenancy by join — no column sprawl and no write-path changes in the worker
or scheduler beyond job creation (a `Job` created from a `RegisteredJob`
inherits its `tenant_id`; a manual run stamps the caller's active tenant).

## Default tenant

A `default` tenant (slug `default`) is seeded by the migration and by
`aaiclick setup`. The migration backfills every existing `registered_jobs`
/ `jobs` row into it, maps existing `admin` users to `superadmin=true`, and
gives existing `viewer` users a `viewer` membership in it. Local mode runs
entirely in this tenant.

# Access Tokens

JWT claims extend the existing scheme (`docs/designs/auth.md` — Auth
Mechanics): `sub`, `exp`, `type="access"` stay; `role` is replaced by

- `superadmin`: bool
- `tenants`: `{"<tenant_id>": "admin" | "viewer"}` — the user's memberships
  at mint time.

The trust model is unchanged: claims are trusted for the access-token
lifetime (≤ 30 min); membership grant/revoke/role-change calls
`store.revoke_all_for_user`, so changes bind at the refresh boundary.

# Active Tenant Resolution

The active tenant is selected per request with the **`X-Tenant-Id`**
header. Header-based selection keeps every router path, the SPA client
chokepoint, and the OpenAPI schema unchanged; the SPA sets the header once
from its tenant switcher.

Resolution is a separate dependency from identity: `require_principal`
keeps resolving *who* is calling, and a new `require_tenant` dependency —
applied only to tenant-scoped routers (jobs, registered-jobs, tasks,
objects) — resolves *where* they act. Tenant-less surfaces (`/auth`,
`/users`, `/tenants`, worker routes, `/health`) never demand the header.

- Header present → must be a tenant the principal can act in (membership,
  or superadmin), else `403`.
- Header absent → if the principal has exactly one tenant, that tenant is
  implied; otherwise `422` (`code="invalid"`) — this includes superadmins,
  who can act in every tenant and so always send the header on
  tenant-scoped routes.
- Auth disabled (local mode) → the default tenant, always.

`Principal` grows to `{user_id, username, superadmin, tenants}` (the
membership map from the JWT); `require_tenant` yields a
`TenantContext {tenant_id, tenant_role}`. `require_admin` now means
*tenant admin* (`tenant_role == "admin"`); a new `require_superadmin`
guards instance-level surfaces.

Enforcement lives in `internal_api` query filters, not in routers: a
tenant contextvar (set by the server dependency, or by the CLI / local
runtime to the default tenant) is read by `internal_api.jobs`,
`registered_jobs`, and `tasks`, which add `WHERE tenant_id = :active`
(tasks/groups via join through `jobs`). A cross-tenant `get` by id returns
`404`, never `403` — no existence leak.

# Role Matrix

| Capability                                          | viewer | tenant admin | superadmin |
|-----------------------------------------------------|:------:|:------------:|:----------:|
| Reads within the active tenant (jobs, tasks, ...)   | ✅     | ✅           | ✅         |
| `/auth/*`, change own password                      | ✅     | ✅           | ✅         |
| Run / cancel jobs, register / enable / disable jobs | ❌     | ✅           | ✅         |
| Clear tasks, delete / purge objects                 | ❌     | ✅           | ✅         |
| Manage memberships of the active tenant             | ❌     | ✅           | ✅         |
| List execution workers                              | ✅     | ✅           | ✅         |
| Start / stop execution workers                      | ❌     | ❌           | ✅         |
| Tenant CRUD (`/tenants`)                            | ❌     | ❌           | ✅         |
| User management (`/users`)                          | ❌     | ❌           | ✅         |
| MCP surface (`/mcp`)                                | ❌     | ❌           | ✅         |

Worker start/stop and `/mcp` move from "admin" to superadmin because both
reach across tenants (workers execute every tenant's tasks; MCP tools are
not tenant-filtered in phase 1).

# API Surface

Existing routers keep their paths and become tenant-scoped through the
principal dependency — no per-endpoint changes beyond swapping
`require_admin` / `require_superadmin` where the role matrix says so.

New `tenants` router:

| Route                                        | Guard                      | Purpose                        |
|----------------------------------------------|----------------------------|--------------------------------|
| `GET /tenants`                               | superadmin                 | List tenants                   |
| `POST /tenants`                              | superadmin                 | Create tenant                  |
| `GET /tenants/{id}`                          | superadmin or member       | Tenant detail                  |
| `GET /tenants/{id}/members`                  | superadmin or tenant admin | List memberships               |
| `PUT /tenants/{id}/members/{user_id}`        | superadmin or tenant admin | Add member / set role          |
| `DELETE /tenants/{id}/members/{user_id}`     | superadmin or tenant admin | Remove member                  |

`GET /auth/me` returns `{id, username, superadmin, tenants}` so the SPA can
populate its switcher without an extra round trip.

# CLI

Thin renderers over `internal_api`, in-process (no HTTP auth):

- `aaiclick tenant create --slug --name`, `tenant list`
- `aaiclick user create --username --password [--superadmin]`
- `aaiclick member add|set-role|remove --tenant <slug> --username <u> [--role {admin,viewer}]`

`AAICLICK_ADMIN_USERNAME` / `AAICLICK_ADMIN_PASSWORD` seed a **superadmin**
on first startup, unchanged otherwise.

# Object Tenancy (Phase 2)

Persistent objects have no SQL metadata — they are discovered by scanning
ClickHouse table names (`aaiclick/data/scope.py` prefix scheme). Tenant
scoping therefore extends the naming convention:

- Default tenant keeps bare `p_<name>` — full backward compatibility.
- Other tenants use `p_<tenant_id>_<name>`.

`scope.py` gains tenant-aware `make_scoped_table_name` / `name_from_table`
variants; `internal_api.objects` lists, opens, deletes, and purges through
the active tenant's prefix. Job-scoped (`j_*`) and temp (`t_*`) tables need
no change — they are reachable only through their tenant-scoped job.

# SPA (Phase 3)

- Tenant switcher in the header, backed by `me.tenants`; the selection is
  stored and sent as `X-Tenant-Id` from the `client.ts` chokepoint.
- `AdminButton` keys off the active tenant's role; superadmin-only controls
  (workers, tenants, users) key off `me.superadmin`.
- Tenant + membership management views for superadmins / tenant admins.

# Delivery Phases

| Phase | Deliverable                                                              |
|-------|--------------------------------------------------------------------------|
| 1     | Tables + migration, JWT/principal changes, tenant contextvar + query scoping, `/tenants` API, CLI, docs |
| 2     | Tenant-prefixed `p_*` naming, tenant-scoped object endpoints             |
| 3     | SPA tenant switcher, membership admin UI, superadmin-gated controls      |

Each phase is one PR. Business-logic tests live in
`aaiclick/internal_api/test_*.py` and `aaiclick/auth/test_*.py` (chdb +
SQLite, no infrastructure); router tests assert HTTP plumbing only.

# Migration

One Alembic revision (via the `generate-migration` skill): create
`tenants` / `tenant_memberships`, seed the default tenant, add
`tenant_id` to `registered_jobs` / `jobs` backfilled to the default tenant,
and convert `users.role` → `users.superadmin` (admin → `true`; viewer →
`false` + default-tenant `viewer` membership). Local/dev (`aaiclick setup`)
builds tables from `SQLModel.metadata` and seeds the default tenant in
code, so the revision is only required for Postgres-backed deployments.
