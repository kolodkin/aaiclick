Authentication, Users & RBAC
---

aaiclick authenticates its HTTP surfaces (REST + MCP) with username/password
users and two-role RBAC. The browser SPA and any programmatic HTTP / MCP client
share one login flow; the CLI runs `internal_api` in-process and never crosses
the HTTP auth layer.

# Scope

- **Users**: username + password, stored in the orchestration SQL database.
- **Roles**: exactly two — `admin` (full access) and `viewer` (read-only). No
  teams, per-resource ACLs, or custom roles.
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
| `AAICLICK_ADMIN_USERNAME`  | Seed-admin username (inserted on startup when no users exist). | unset          |
| `AAICLICK_ADMIN_PASSWORD`  | Seed-admin password.                                           | unset          |

!!! warning "Distributed without a secret is a hard error"
    In distributed mode with `AAICLICK_JWT_SECRET` unset, the server refuses to
    start. In local mode auth is disabled, every request is allowed, and the
    server logs a single startup `WARNING`.

# Data Model

Two SQLModel tables in `aaiclick/auth/models.py`. IDs are snowflake
`BigInteger` PKs; `role` uses the project's `_enum_check` String + CHECK
pattern.

## `users`

| Column          | Type                            | Notes                  |
|-----------------|---------------------------------|------------------------|
| `id`            | `BigInteger` PK (snowflake)     |                        |
| `username`      | `String`, unique, indexed       | Login identifier       |
| `password_hash` | `String`                        | bcrypt                 |
| `role`          | `String` + CHECK `IN ('admin','viewer')` | `Role` literal |
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
module constants `ROLE_ADMIN` / `ROLE_VIEWER`.

# Module Layout

```
aaiclick/
  auth/
    models.py        users / refresh_tokens; Role literal + constants
    security.py      bcrypt hash/verify; secret gen + sha256; JWT encode/decode
                     (pure functions, no DB, no contextvars)
    config.py        env getters (enabled, secret, TTLs, admin seed)
    store.py         raw DB CRUD over users / refresh_tokens
    view_models.py   LoginRequest, RefreshRequest, LogoutRequest, TokenPair,
                     MeView, UserView, CreateUserRequest, ...
  internal_api/
    auth.py          login(), refresh(), logout()  → view models
    users.py         create_user, list_users, get_user, set_role,
                     disable_user, set_password
  server/
    auth.py          principal resolution + RBAC dependencies + /mcp middleware
    routers/
      auth.py        /auth/login, /auth/refresh, /auth/logout, /auth/me
      users.py       /users   (admin-only)
  __main__.py        aaiclick user create|list|set-role|disable|passwd
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
- Access JWT claims: `sub=<user_id>`, `role`, `exp`, `type="access"`.
- Refresh token: a random opaque secret; only its `sha256` is stored in
  `refresh_tokens`.

## Refresh (rotation)

`POST /api/v0/auth/refresh` `{refresh_token}` → new `TokenPair`. The row is
looked up by hash and rejected if missing / expired / rotated / revoked. On
success the old row is stamped `rotated_at` and a fresh refresh token is issued,
re-reading the owner's current `role` and `disabled`. Reusing a rotated token
returns `401`.

## Logout

`POST /api/v0/auth/logout` `{refresh_token}` revokes that refresh row. Access
JWTs are stateless and expire on their own (≤ 30 min).

## Me

`GET /api/v0/auth/me` → `MeView {id, username, role}` for the current principal.

# Principal Resolution & RBAC

`require_principal` extracts the credential with FastAPI's
`HTTPBearer(auto_error=False)` (which also registers the `/docs` **Authorize**
box; `auto_error=False` so a missing credential yields the `Problem` envelope
rather than FastAPI's bare `HTTPException`), then resolves a
`Principal {user_id, username, role}`:

- **Auth disabled** → a synthetic admin principal; all routes open.
- **Valid access JWT** (`type="access"`, valid signature + `exp`) → claims are
  trusted for the token's ≤30-min lifetime (`sub`, `role`). Disabling or
  demoting a user takes full effect within one access-TTL — the next
  `/auth/refresh` re-reads the DB and fails / downgrades.
- **Otherwise** → `401` with `WWW-Authenticate: Bearer`.

`require_admin` depends on `require_principal` and raises `Forbidden` (`403`,
`code="forbidden"`) when `role != "admin"`.

## Role matrix

| Capability                                              | viewer | admin |
|---------------------------------------------------------|:------:|:-----:|
| `GET` reads (jobs, tasks, workers, objects, lineage)    | ✅     | ✅    |
| `/auth/*` (login, refresh, logout, me)                  | ✅     | ✅    |
| Run / cancel jobs, register / enable / disable jobs     | ❌     | ✅    |
| Delete / purge objects                                  | ❌     | ✅    |
| Start / stop workers                                    | ❌     | ✅    |
| User management (`/users`)                              | ❌     | ✅    |
| MCP surface (`/mcp`)                                    | ❌     | ✅    |

`require_principal` guards every `/api/v0` router via
`include_router(dependencies=...)`; `require_admin` is added to each mutating
route. Reads need only a valid principal. Per-router scope deps (`orch_scope`)
run alongside.

# MCP Surface

The `/mcp` mount is **admin-only** and all-or-nothing — no per-tool RBAC.
Because FastAPI's `Depends` does not reach mounted sub-apps, the mount is
wrapped in an ASGI middleware that runs the same principal resolution and
additionally requires `role == "admin"`; non-admin or unauthenticated requests
get a `401`/`403` `Problem`.

# CLI & Admin Bootstrap

- **CLI**: `aaiclick user create --username --password --role {admin,viewer}`,
  `list`, `set-role`, `disable`, `passwd` — thin renderers over
  `internal_api.users`, running in-process.
- **Startup seed**: when auth is enabled and `AAICLICK_ADMIN_USERNAME` /
  `AAICLICK_ADMIN_PASSWORD` are set, that admin is inserted during server
  lifespan startup if the `users` table is empty. The seed and the CLI both
  bootstrap the first admin.

# SPA

- `src/api/client.ts` attaches `Authorization: Bearer <access>` from the auth
  store; on `401` it attempts `/auth/refresh` once and retries, else clears the
  session and drops back to **Login**. Single chokepoint.
- `src/lib/auth.ts`: in-memory access token + `localStorage` refresh token;
  `login` / `logout` / `tryRefresh` / `fetchMe` helpers.
- `src/components/Auth.tsx`: `AuthProvider` / `useAuth`, bootstrapped from
  `/auth/me`.
- `src/views/Login.tsx`: username + password form.
- `App.tsx` gates rendering on the session. When auth is disabled `/auth/me`
  returns the synthetic admin, so no login wall appears.

# Migration

The `users` and `refresh_tokens` tables are created by an Alembic revision
(`aaiclick/auth/models.py` is imported in `migrations/env.py` so autogenerate
sees them). Local/dev (`aaiclick setup`) builds the tables from
`SQLModel.metadata`, so the revision is only required for Postgres-backed
deployments.
