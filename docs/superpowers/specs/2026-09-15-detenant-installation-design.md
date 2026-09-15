De-tenant the Installation
---

Remove multi-tenancy from an aaiclick installation. RBAC stays, reduced to
`admin` / `member` / `viewer`. Fleet-level tenancy moves to a future control
plane, specified separately in `docs/designs/tenants_draft.md`.

# Motivation

Tenancy today is metadata-level: one SQL database, one ClickHouse database,
every query filtered by an active tenant. That never delivered isolation
between mutually-distrusting parties — `tenant_rbac.md` says so outright
("trusted teams sharing a deployment, not hostile parties"). The isolation
unit becomes the installation instead: one customer, one namespace, one
database pair. A tenant is then something a control plane provisions, not a
column an installation filters on.

This document covers only the removal. The control plane is future work.

# Scope

In scope: deleting tenant state from the installation, collapsing the role
and scope ladders, reverting persistent object naming, squashing the
migration chain, and updating every doc that describes tenancy as shipped.

Out of scope: the control plane itself, per-tenant provisioning, central
identity, and asymmetric token signing. All of it is specified in
`docs/designs/tenants_draft.md` and indexed from `docs/designs/future.md`.

# Role Model

`TenantRole` disappears as a distinct type; `Role` becomes the single ladder.
`superadmin` is removed entirely — its remaining job was administering
tenants, which is now the control plane's.

| Role     | Scope   | Authority                                              |
|----------|---------|--------------------------------------------------------|
| `viewer` | `read`  | Reads                                                  |
| `member` | `write` | Reads, plus their own jobs, objects and saved queries  |
| `admin`  | `admin` | Runs the installation — users, workers, audit, setup   |

`users.superadmin` (bool) is replaced by `users.role`, a plain `String`
column typed with the `Role` literal — no DB CHECK, per CLAUDE.md.

Every surface currently gated by `require_superadmin` moves to `admin`:
`/users`, `/execution_workers`, `/audit`, `setup` / `migrate`, and the
superadmin-tagged MCP tools.

Defaults: `aaiclick setup` seeds an initial `admin`; a user created without
an explicit role gets `viewer` (least privilege); local mode's synthetic
principal acts as `admin`, preserving today's zero-config behaviour.

!!! warning "Collapsing a scope rung can widen access"
    `require_superadmin` → `require_admin` is not a mechanical substitution.
    Each call site grants installation-wide authority to a strictly larger
    set of principals, so every one is reviewed individually rather than
    rewritten in bulk.

# Removal Inventory

**Deleted outright** — `aaiclick/tenancy.py`; the `Tenant` and
`TenantMembership` models; `aaiclick/internal_api/tenants.py`;
`aaiclick/server/routers/tenants.py`; `docs/designs/tenant_rbac.md`; and the
tests covering them.

**Columns dropped** — `tenant_id` from `registered_jobs`, `jobs`,
`api_tokens`, `audit_log`, `table_registry`, `viewer_queries` and
`viewer_dashboards`. Tasks, groups, dependencies and lineage never carried
one; they reached tenancy by join, so they need no change.

**Request path** — the `X-Tenant-Id` header, and `resolve_tenant`,
`require_tenant` and `check_tenant_scope` in `aaiclick/server/auth.py`.
`principal_to_scope` loses its `tenant_id` argument and becomes a field read.

**Tokens** — `AccessClaims` drops the `tenants` membership map and the
`superadmin` flag for a single `role` claim. `api_tokens.tenant_id` is
dropped; `scope` remains, now over three rungs.

**CLI** — the global `--tenant` flag, the `tenant` subcommand and the
`member` subcommand, plus the tenant-slug resolution helper in
`aaiclick/__main__.py`.

**SPA** — the tenant columns in `Audit.tsx` and `Tokens.tsx`,
`getActiveTenantId` in `src/lib/auth.ts`, the tenant-id body fields in
`Invite.tsx` and `Tokens.tsx`, and the regenerated `src/api/schema.ts`.

**Query filters** — roughly twelve `WHERE tenant_id = ...` predicates across
`aaiclick/internal_api/` (`jobs`, `registered_jobs`, `tasks`, `viewer`) and
the registry lookups in `aaiclick/data/data_context/data_context.py`.

# Object Naming

Persistent tables revert from `p_<tenant_id>_<name>` to `p_<name>`.
`make_scoped_table_name` in `aaiclick/data/scope.py` loses its `tenant_id`
parameter, and `name_from_table` splits on one underscore rather than two
for the global scope.

The name-length budget improves: the global prefix costs 2 characters
instead of 22. `MAX_PERSISTENT_NAME_LEN` stays at 128 regardless — the cap
is about predictable errors at the API boundary, not about reclaiming every
available character.

# Schema

The 23 existing revisions collapse into a single `initial_schema` with
`down_revision = None`, generated with the `generate-migration` skill
against the post-removal models. Tenancy never appears in the chain.

This is viable only because nothing is released: there are no git tags, no
changelog, and `setuptools_scm` falls back to `0.0.0`.

!!! warning "No upgrade path from an existing database"
    Squashing discards every intermediate revision, so existing databases
    cannot be migrated — they are recreated. This applies to ClickHouse as
    well as Postgres: `table_registry` comes back empty, so any surviving
    `p_<tenant_id>_*` tables would be orphaned rather than renamed. Both
    databases are dropped and recreated together.

`aaiclick setup` loses `_seed_default_tenant` and its staleness probes for
a pre-existing default-tenant row, gaining an initial-admin seed instead.

# Testing

The suite is the main consumer of the removed surface — 80 files reference
tenancy and roughly half are tests. Three test modules delete wholesale
(`test_tenancy.py`, `internal_api/test_tenants.py`,
`server/routers/test_tenants.py`); the rest lose fixtures and assertions.

Per `python-testing-style`, tests that existed only to prove a tenant filter
excluded another tenant's rows are deleted rather than rewritten — the
behaviour they covered no longer exists. Tests that happen to *use* tenancy
while covering something else keep their coverage and shed the tenant
argument.

The scope ladder gets direct coverage at its new shape: each role maps to
its scope, `scope_admits` orders three rungs, and every former-superadmin
route rejects `member` and `viewer`.

`aaiclick/server/conftest.py` holds the shared principal and tenant
fixtures; it is the natural first edit, since most server tests inherit
from it.

# Documentation

`docs/designs/tenants_draft.md` and the `docs/designs/future.md` entry pointing
at it are already written — they carry the control-plane design, including
the constraint that tenancy is Kubernetes-only, with one namespace per
tenant and no Compose or local-mode equivalent.

`docs/designs/tenant_rbac.md` is deleted with the code it documents.

Updated to drop tenancy: `auth.md` (75 references), `viewer.md` (17),
`api_server.md` (6), `orchestration.md`, `ui.md`, `frontend.md`,
`getting_started.md`, `user_guide/object.md`, `user_guide/data_context.md`.

Two incidental tenant references survive elsewhere in `future.md` — in the
event-fanout entry and the foreign-key entry — and are reworded in the same
pass.

# Risks

| Risk                                          | Mitigation                                                                 |
|-----------------------------------------------|----------------------------------------------------------------------------|
| Scope collapse silently widens access          | Review each `require_superadmin` site individually; assert rejection per role |
| Squash leaves no upgrade path                  | Accepted — pre-release, no tags or changelog; documented above               |
| Orphaned ClickHouse tables from prefix change  | Drop and recreate both databases together, not Postgres alone               |
| Removal spans auth, data, CLI, server and SPA  | Phase the work so each phase lands with a green suite                       |
