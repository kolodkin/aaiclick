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

**Implementation**: `aaiclick/auth/models.py` — see `Tenant`, `TenantMembership`;
store CRUD in `aaiclick/auth/store.py` (`create_tenant`, `set_membership`, ...).

IDs are snowflake `BigInteger` PKs; role columns are plain `String` typed
with the `Role` literal (no DB CHECK — see CLAUDE.md, "Prefer Literal").

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
| `tenant_id`  | `BigInteger` FK → `tenants.id`, indexed |                |
| `user_id`    | `BigInteger` FK → `users.id`, indexed   |                |
| `role`       | `String`                                | `Role` literal |
| `created_at` | `datetime` (`utc_now`)                  |                |

Unique constraint on `(tenant_id, user_id)`.

## `users` changes

`role` is replaced by `superadmin: bool` (default `false`). A user's
effective role in a tenant is their membership role, or `admin` everywhere
when `superadmin` is set.

## Resource scoping

`tenant_id` (`BigInteger`, non-null, indexed — a plain column, not a DB FK:
the auth tables live in a separate module and cross-package DDL coupling buys
nothing, so the reference is enforced at the API boundary) is added to
**`registered_jobs` and `jobs` only**. Tasks, groups, dependencies, remote
task results, and lineage all reach their job via `job_id`, so they derive
tenancy by join — no column sprawl and no write-path changes in the worker
or scheduler beyond job creation (a `Job` created from a `RegisteredJob`
inherits its `tenant_id`; a manual run stamps the caller's active tenant).

## Default tenant

A default tenant (slug `aaiclick`, fixed id `1`) is seeded by the migration and by
`aaiclick setup`. The migration backfills every existing `registered_jobs`
/ `jobs` row into it, maps existing `admin` users to `superadmin=true`, and
gives existing `viewer` users a `viewer` membership in it. Local mode runs
entirely in this tenant.

# Access Tokens

**Implementation**: `aaiclick/auth/security.py` — see `AccessClaims`,
`encode_access_token`; minting in `aaiclick/internal_api/auth.py` — see
`_mint_pair`.

JWT claims extend the existing scheme (`docs/designs/auth.md` — Auth
Mechanics): `sub`, `exp`, `type="access"` stay; `role` is replaced by

- `superadmin`: bool
- `tenants`: `{"<tenant_id>": "admin" | "viewer"}` — the user's memberships
  at mint time.

The trust model is unchanged: claims are trusted for the access-token
lifetime (≤ 30 min); membership grant/revoke/role-change calls
`store.revoke_all_for_user`, so changes bind at the refresh boundary.

# Active Tenant Resolution

**Implementation**: `aaiclick/server/auth.py` — see `resolve_tenant`,
`require_tenant`, `require_admin`, `require_superadmin`; the contextvar in
`aaiclick/tenancy.py`.

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
(tasks/groups via join through `jobs` — see `internal_api/tasks.py`
`_visible_task`). A cross-tenant `get` by id returns `404`, never `403` —
no existence leak. Job rows are stamped in `orchestration/factories.py` —
see `new_job_row` (a scheduled run inherits its registration's tenant).

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

**Implementation**: `aaiclick/server/routers/tenants.py`;
`aaiclick/internal_api/tenants.py`.

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

**Implementation**: `aaiclick/__main__.py` — see `_run_tenant_create`,
`_member_set`, and the global ``--tenant`` handling in `_run_internal_api`.

Thin renderers over `internal_api`, in-process (no HTTP auth):

- `aaiclick tenant create --slug --name`, `tenant list`
- `aaiclick user create --username --password [--superadmin]`
- `aaiclick member add|set-role|remove --tenant <slug> --username <u> [--role {admin,viewer}]`

`AAICLICK_ADMIN_USERNAME` / `AAICLICK_ADMIN_PASSWORD` seed a **superadmin**
on first startup, unchanged otherwise.

# Object Tenancy (Phase 2)

Persistent objects are namespaced twice: the ClickHouse table name carries the
tenant, so two tenants can hold the same object name, and a SQL
`table_registry` row records which tenant owns it. Neither layer is sufficient
alone.

## Physical namespace — tenant-prefixed table names

**Implementation**: `aaiclick/data/scope.py` — see `make_scoped_table_name`,
`name_from_table`; the active tenant is applied in
`aaiclick/data/data_context/data_context.py` — see `_build_scoped_table`.

- Default tenant keeps bare `p_<name>` — full backward compatibility.
- Other tenants use `p_<tenant_id>_<name>`.

Job-scoped (`j_*`) and temp (`t_*`) tables are unchanged — they are reachable
only through their tenant-scoped job, and the tenant prefix never stacks onto
them.

The prefix parses unambiguously because `_validate_persistent_name`
(`aaiclick/data/data_context/data_context.py`) rejects a leading digit, so no
default-tenant object can produce a `p_<digits>_` prefix. A test pins that
coupling (`aaiclick/data/test_scope.py` — see
`test_leading_underscore_name_does_not_look_tenant_prefixed`), so relaxing the
name regex cannot silently introduce a collision.

!!! important "Design decision: the prefix, not the registry, prevents cross-tenant writes"
    Persistent creates use `CREATE TABLE IF NOT EXISTS` (see `create_object`)
    and the registry insert is `ON CONFLICT (table_name) DO NOTHING` (see
    `DBLifecycleHandler._write_table_registry_row`). On a shared physical name,
    a second tenant creating an already-taken name would write silently into
    the first tenant's table while the registry kept attributing it to the
    original owner. Tenant-unique names remove the case, which is why the
    physical namespace stays tenant-prefixed even with registry ownership in
    place — any future change that drops the prefix (see `docs/designs/future.md`,
    "Opaque Object Table Names") must re-establish per-tenant uniqueness by
    other means first.

## Ownership — `table_registry.tenant_id`

**Implementation**: `aaiclick/orchestration/lifecycle/db_lifecycle.py` — see
`TableRegistry`.

`table_registry` holds one row per ClickHouse table aaiclick creates and is
authoritative on the read path: `open_object()` resolves an object's schema
through it (`aaiclick/data/object/ingest.py` — see `_get_table_schema`),
raising `LookupError` when no row exists. The `tenant_id` column
(`BigInteger`, non-null, indexed — a plain column, not a DB FK, matching
`registered_jobs` / `jobs`) makes that path tenant-aware; the orch lifecycle
handler stamps the active tenant on every row it registers
(`aaiclick/orchestration/orch_context.py` — see `register_table`).

| Surface                     | Behaviour                                                                |
|-----------------------------|-------------------------------------------------------------------------|
| `open_object()`             | `_get_table_schema` filters by the active tenant — a cross-tenant open raises `ObjectNotFoundError`, surfacing as `404`, never `403` |
| `list_persistent_objects()` | Reads `table_registry` filtered by the active tenant instead of scanning `system.tables` |
| `delete_persistent_objects()` | Purge candidates come from the tenant-filtered listing; drops clear their registry rows (see `_forget_registry_rows`) |
| Background cleanup          | `j_*` / `t_*` rows carry the tenant too, giving the worker tenant visibility for free |

Global-scope creation already requires an orch context — `_resolve_scope`
rejects `scope="global"` under a bare `data_context()` — so every persistent
object has a registry row by construction, and the column has no
partially-populated case.

Both callers supply that SQL session: the server mounts `orch_scope_with_ch`
(`aaiclick/server/routers/objects.py`) and the CLI runs `data` subcommands
through `_run_data_api` (`aaiclick/__main__.py`), which delegates to
`_run_internal_api(..., with_ch=True)` and so also applies the top-level
`--tenant` flag. Registry-backed listing can rely on both.

!!! warning "Distinguish a missing object from a missing context"
    `open_object()` raises `ObjectNotFoundError` (`aaiclick/data/data_context/data_context.py`),
    a `RuntimeError` subclass, and `get_object` catches only that. Catching
    plain `RuntimeError` there swallows the `get_sql_session()` "no active
    orch_context" error as a `404`, which previously made `aaiclick data get
    <name>` report every object as missing while `aaiclick data list` still
    listed it.

## Name length budget

ClickHouse caps table names near `213 - len(database)` characters (measured
against chdb: 242 in `default`, 205 in a database named `aaiclick`), regardless
of the data directory path. A snowflake renders as 19 digits, so
`p_<tenant_id>_` costs 22 characters — the same as the `j_<job_id>_` and
`t_<name>_<snowid>` prefixes already in use — leaving 183+ for the name.

`_validate_persistent_name` (`aaiclick/data/data_context/data_context.py` —
see `MAX_PERSISTENT_NAME_LEN`) caps names at 128 characters, so an over-long
name raises `ValueError` at the API boundary instead of failing deep in
ClickHouse with `ARGUMENT_OUT_OF_BOUND` — or, past 251 characters, an
unhandled `std::filesystem::filesystem_error`.

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
| 2     | Tenant-prefixed `p_*` naming, `table_registry.tenant_id`, tenant-scoped object endpoints |
| 3     | SPA tenant switcher, membership admin UI, superadmin-gated controls      |

Each phase is one PR. Business-logic tests live in
`aaiclick/internal_api/test_*.py` and `aaiclick/auth/test_*.py` (chdb +
SQLite, no infrastructure); router tests assert HTTP plumbing only.

# Migration

**Implementation**: `aaiclick/orchestration/migrations/versions/` — revision
`39cd0baa9f90` (tenant rbac).

One Alembic revision (via the `generate-migration` skill): create
`tenants` / `tenant_memberships`, seed the default tenant, add
`tenant_id` to `registered_jobs` / `jobs` backfilled to the default tenant,
and convert `users.role` → `users.superadmin` (admin → `true`; viewer →
`false` + default-tenant `viewer` membership). Local/dev (`aaiclick setup`)
builds tables from `SQLModel.metadata` and seeds the default tenant in
code, so the revision is only required for Postgres-backed deployments.

Phase 2's revision `1da307dfbd95` adds `table_registry.tenant_id` with
`server_default='1'`, so rows for tables that predate the column backfill to
the default tenant — matching the bare `p_<name>` prefix they already carry.
