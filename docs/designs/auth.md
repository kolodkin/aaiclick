Authentication, Users & RBAC
---

aaiclick authenticates its HTTP surfaces (REST + MCP) with username/password
users and per-tenant RBAC (see `docs/designs/tenant_rbac.md`). The browser SPA
and any programmatic HTTP / MCP client share one login flow; the CLI runs
`internal_api` in-process and never crosses the HTTP auth layer.

# Scope

- **Users**: username + password, stored in the orchestration SQL database,
  plus an instance-level `superadmin` flag.
- **Roles**: per-tenant memberships carrying `admin` or `viewer` (see
  `docs/designs/tenant_rbac.md`). No per-resource ACLs or custom roles.
- **Sessions**: password login → short-lived access JWT + rotating refresh
  token. One credential header everywhere: `Authorization: Bearer <access-jwt>`.
- **Mode-derived enforcement**: auth is a hardcoded convention, not a flag —
  **disabled in local mode** (single-process chdb + SQLite; the server is open,
  zero-config) and **enforced in distributed mode**. There are no long-lived API
  tokens / PATs, no SSO/OIDC/MFA, no user-management UI, and the `/mcp` surface
  is all-or-nothing (admin only).

# Configuration

Whether auth is enforced follows the backend mode (`is_local()`), not an env
var. These variables tune the enforced (distributed) case:

| Variable                   | Purpose                                                        | Default        |
|----------------------------|----------------------------------------------------------------|----------------|
| `AAICLICK_JWT_SECRET`      | HS256 signing secret. **Required** in distributed mode.        | unset          |
| `AAICLICK_JWT_ACCESS_TTL`  | Access-JWT lifetime, seconds.                                  | `1800` (30 min)|
| `AAICLICK_JWT_REFRESH_TTL` | Refresh-token lifetime, seconds.                               | `1209600` (14 d)|
| `AAICLICK_ADMIN_USERNAME`  | Seed-superadmin username (inserted on startup when no users exist). | `superadmin`   |
| `AAICLICK_ADMIN_PASSWORD`  | Seed-admin password.                                           | unset          |

!!! warning "Distributed without a secret is a hard error"
    In distributed mode with `AAICLICK_JWT_SECRET` unset, the server refuses to
    start. In local mode auth is disabled, every request is allowed, and the
    server logs a single startup `WARNING`.

# Data Model

Two SQLModel tables in `aaiclick/auth/models.py`. IDs are snowflake
`BigInteger` PKs; `role` is a plain `String` column typed with the `Role`
`Literal` and validated in code (no DB CHECK — see CLAUDE.md, "Prefer Literal").

## `users`

| Column          | Type                            | Notes                  |
|-----------------|---------------------------------|------------------------|
| `id`            | `BigInteger` PK (snowflake)     |                        |
| `username`      | `String`, unique, indexed       | Login identifier       |
| `password_hash` | `String`                        | bcrypt                 |
| `superadmin`    | `Boolean`, default `false`      | Instance-level operator |
| `disabled`      | `Boolean`, default `false`      | Disabled → cannot log in |
| `created_at`    | `datetime` (`utc_now`)          |                        |

## `refresh_tokens`

| Column       | Type                        | Notes                                |
|--------------|-----------------------------|--------------------------------------|
| `id`         | `BigInteger` PK (snowflake) |                                      |
| `user_id`    | `BigInteger` FK → `users.id`, indexed |                            |
| `token_hash` | `String`, unique, indexed   | `sha256(secret)`                     |
| `expires_at` | `datetime`                  |                                      |
| `rotated_at` | `datetime \| None`          | Set when consumed by `/auth/refresh` |
| `revoked_at` | `datetime \| None`          | Set on logout                        |

`Role = Literal["admin", "viewer"]` lives in `aaiclick/auth/models.py` with
module constants `ROLE_ADMIN` / `ROLE_VIEWER`; it is the per-tenant membership
role. The `tenants` / `tenant_memberships` tables live in the same module —
see `docs/designs/tenant_rbac.md` — Data Model.

# Module Layout

```
aaiclick/
  auth/
    models.py        users / refresh_tokens / tenants / tenant_memberships;
                     Role literal + constants
    security.py      bcrypt hash/verify; secret gen + sha256; JWT encode/decode
                     (pure functions, no DB, no contextvars)
    config.py        env getters (enabled, secret, TTLs, admin seed)
    store.py         raw DB CRUD over users / refresh_tokens; revoke_all_for_user
    view_models.py   LoginRequest, RefreshRequest, LogoutRequest, TokenPair,
                     MeView, UserView, CreateUserRequest, ...
  internal_api/
    auth.py          login(), refresh(), logout(), change_password(), my_tenants()
    users.py         create_user, list_users, get_user, set_superadmin,
                     disable_user, set_password
    tenants.py       tenant CRUD + membership management
  server/
    auth.py          principal resolution + RBAC dependencies + /mcp middleware
    routers/
      auth.py        /auth/login, /auth/refresh, /auth/logout, /auth/me,
                     /auth/me/password
      users.py       /users   (superadmin-only)
      tenants.py     /tenants (see docs/designs/tenant_rbac.md)
  __main__.py        aaiclick user|tenant|member commands
```

Business logic is transport-agnostic in `internal_api` / `auth`, running inside
`orch_context` and reading the SQL session via the contextvar getter. `server/`
owns JWT/transport.

# Auth Mechanics

Passwords are hashed with `bcrypt`. Access JWTs are signed HS256 with
`AAICLICK_JWT_SECRET`.

## Login → token pair

`POST /api/v0/auth/login` `{username, password}` → `200 TokenPair`:

```json
{ "access_token": "<jwt>", "refresh_token": "<opaque>", "token_type": "bearer",
  "expires_in": 1800 }
```

- The user must exist, be enabled, and the password must match. Otherwise `401`
  (`code="unauthorized"`) — no user-enumeration distinction.
- Access JWT claims: `sub=<user_id>`, `superadmin`, `tenants` (the membership
  map `tenant_id -> role`), `exp`, `type="access"`.
- Refresh token: a random opaque secret; only its `sha256` is stored in
  `refresh_tokens`.

## Refresh (rotation)

`POST /api/v0/auth/refresh` `{refresh_token}` → new `TokenPair`. The row is
looked up by hash and rejected if missing / expired / rotated / revoked. On
success the old row is stamped `rotated_at` and a fresh refresh token is issued,
re-reading the owner's current `superadmin`, memberships, and `disabled`.
Reusing a rotated token returns `401`.

## Logout

`POST /api/v0/auth/logout` `{refresh_token}` revokes that refresh row. Access
JWTs are stateless and expire on their own (≤ 30 min).

## Me

`GET /api/v0/auth/me` → `MeView {id, username, superadmin, tenants}` for the
current principal (`tenants` lists each membership with slug, name, and role).

## Change own password

`PUT /api/v0/auth/me/password` `{current_password, new_password}` → `204`. Open
to **any** role — `/users` is admin-only, so without this a viewer could never
rotate their own credential. `current_password` is required so a stolen access
token alone cannot seize the account, and a mismatch is `401`. Local mode has no
current user (the synthetic admin's `user_id` is `None`), so the route answers
`422` there.

## Session revocation

`store.revoke_all_for_user` stamps `revoked_at` on every still-active refresh row.
It runs on superadmin change, membership change, disable, admin password reset,
and self-service password change — a demotion must not be outlived by a refresh
token still minting the old claims, and someone changing their password after a
suspected leak needs the other party's token dead.

!!! note "Revocation binds at the refresh boundary, not instantly"
    Access JWTs are verified by signature alone — no DB read — so a revoked
    user keeps their existing access token until it expires (≤ 30 min by
    default). Revocation closes the renewal chain; it does not claw back the
    token in flight. Immediate cutoff would mean a per-request DB lookup or a
    denylist, trading away that statelessness on every read. Tighten
    `AAICLICK_JWT_ACCESS_TTL` if the window matters more than refresh chatter.

# Principal Resolution & RBAC

`require_principal` extracts the credential with FastAPI's
`HTTPBearer(auto_error=False)` (which also registers the `/docs` **Authorize**
box; `auto_error=False` so a missing credential yields the `Problem` envelope
rather than FastAPI's bare `HTTPException`), then resolves a
`Principal {user_id, username, role}`:

- **Auth disabled** → a synthetic superadmin principal; all routes open.
- **Valid access JWT** (`type="access"`, valid signature + `exp`) → claims are
  trusted for the token's ≤30-min lifetime (`sub`, `superadmin`, `tenants`).
  Disabling or demoting a user revokes their refresh rows immediately (see
  *Session revocation*) but takes full effect on the access token only within
  one access-TTL.
- **Otherwise** → `401` with `WWW-Authenticate: Bearer`.

Tenant-scoped routers additionally resolve the active tenant via
`require_tenant` (the `X-Tenant-Id` header); `require_admin` means *tenant
admin* and `require_superadmin` guards instance-level surfaces. The role
matrix and resolution rules live in `docs/designs/tenant_rbac.md` — Role
Matrix / Active Tenant Resolution.

# MCP Surface

The `/mcp` mount is **superadmin-only** and all-or-nothing — no per-tool RBAC.
Because FastAPI's `Depends` does not reach mounted sub-apps, the mount is
wrapped in an ASGI middleware that runs the same principal resolution and
additionally requires `superadmin`; other requests get a `401`/`403`
`Problem`.

# CLI & Admin Bootstrap

- **CLI**: `aaiclick user create <username> --password [--superadmin]`,
  `list`, `set-superadmin`, `disable`, `passwd` — thin renderers over
  `internal_api.users`, running in-process. Tenant and membership commands:
  `docs/designs/tenant_rbac.md` — CLI.
- **Startup seed**: when auth is enabled and `AAICLICK_ADMIN_PASSWORD` is
  set, a **superadmin** is inserted during server lifespan startup if the
  `users` table is empty (username from `AAICLICK_ADMIN_USERNAME`, default
  `superadmin`). The seed and the CLI both bootstrap the first superadmin.

# SPA

- `src/api/client.ts` attaches `Authorization: Bearer <access>` from the auth
  store; on `401` it attempts `/auth/refresh` once and retries, else clears the
  session and drops back to **Login**. Single chokepoint.
- `src/lib/auth.ts`: in-memory access token + `localStorage` refresh token;
  `login` / `logout` / `tryRefresh` / `fetchMe` helpers.
- `src/components/Auth.tsx`: `AuthProvider` / `useAuth`, bootstrapped from
  `/auth/me`; exposes `isAdmin`.
- `src/components/AdminButton.tsx`: renders an admin-only action. Viewers get it
  **disabled with a tooltip** rather than hidden — a greyed-out control shows the
  action exists and why it is unavailable, where hiding it reads as a missing
  feature. Used for run / cancel / register / enable-toggle, and for the
  navigation buttons leading into those flows. Presentation only; `require_admin`
  is still the enforcement.
- `src/views/Login.tsx`: username + password form.
- `App.tsx` gates rendering on the session. When auth is disabled `/auth/me`
  returns the synthetic admin, so no login wall appears.

# Migration

The auth tables (`users`, `refresh_tokens`, `tenants`, `tenant_memberships`)
are created by Alembic revisions
(`aaiclick/auth/models.py` is imported in `migrations/env.py` so autogenerate
sees them). Local/dev (`aaiclick setup`) builds the tables from
`SQLModel.metadata`, so the revision is only required for Postgres-backed
deployments.
