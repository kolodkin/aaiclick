Authentication, Users & RBAC
---

Design spec for replacing the v0 static bearer token with a real auth system:
email/password users, role-based access control (admin / viewer), browser
login sessions (JWT), and user-managed API tokens with expiry.

**Status**: ⚠️ NOT YET IMPLEMENTED — this document is the design of record.
Supersedes the static `AAICLICK_API_TOKEN` described in earlier revisions of
`docs/api_server.md`, and the deferred *Operator UI Auth* / *API Auth — DB-Backed
Token Scopes* items in `docs/future.md`.

# Goals

- **Users**: email + password, stored in the orchestration SQL database.
- **RBAC**: exactly two roles — `admin` (full access) and `viewer`
  (read-only). No teams, no per-resource ACLs, no custom roles.
- **Browser sessions**: password login → short-lived access JWT + rotating
  refresh token (the industry-standard SPA pattern).
- **API tokens (PATs)**: users mint long-lived, named, **expiring** tokens for
  CLI / SDK / MCP access, managed on a token page. Shown once at creation.
- **One unified credential header**: every surface authenticates via
  `Authorization: Bearer <jwt-or-api-token>`.
- **Preserve zero-config local dev**: auth is **off by default**; the
  `local start` onboarding path keeps working with no setup.

# Non-Goals

- Teams / organizations / groups.
- Per-token scope selection or downscoping (a token always carries its
  owner's *current* role).
- OAuth 2.0 / OIDC / SSO, MFA, email verification, password-reset emails.
- A user-management **UI** (admins manage users via REST + CLI this iteration).
- Per-tool RBAC on the MCP surface (MCP is admin-only — see
  [MCP surface](#mcp-surface)).

# Configuration

| Variable                   | Purpose                                                        | Default        |
|----------------------------|----------------------------------------------------------------|----------------|
| `AAICLICK_AUTH_ENABLED`    | Master switch. Off → open server (today's behaviour).          | `false`        |
| `AAICLICK_JWT_SECRET`      | HS256 signing secret. **Required** when auth is enabled.       | unset          |
| `AAICLICK_JWT_ACCESS_TTL`  | Access-JWT lifetime, seconds.                                  | `1800` (30 min)|
| `AAICLICK_JWT_REFRESH_TTL` | Refresh-token lifetime, seconds.                               | `1209600` (14 d)|
| `AAICLICK_ADMIN_EMAIL`     | Seed-admin email (upserted on startup when no users exist).    | unset          |
| `AAICLICK_ADMIN_PASSWORD`  | Seed-admin password.                                           | unset          |

!!! warning "Enabled without a secret is a hard error"
    When `AAICLICK_AUTH_ENABLED=true` and `AAICLICK_JWT_SECRET` is unset, the
    server refuses to start. When auth is **off**, every request is allowed and
    the server logs a single startup `WARNING` (same posture as today's unset
    static token).

# Data Model

Three new SQLModel tables in a new module `aaiclick/auth/models.py`, registered
with `SQLModel.metadata` (add one import line to
`aaiclick/orchestration/migrations/env.py`). All IDs are snowflake
`BigInteger` PKs; `role` uses the project's `_enum_check` String + CHECK
pattern. Created via the `generate-migration` skill — never hand-written.

## `users`

| Column          | Type                            | Notes                                |
|-----------------|---------------------------------|--------------------------------------|
| `id`            | `BigInteger` PK (snowflake)     |                                      |
| `email`         | `String`, unique, indexed       | Login identifier, lower-cased        |
| `password_hash` | `String`                        | bcrypt                               |
| `role`          | `String` + CHECK `IN ('admin','viewer')` | `Role` literal              |
| `disabled`      | `Boolean`, default `false`      | Disabled → cannot log in; tokens dead|
| `created_at`    | `datetime` (`utc_now`)          |                                      |

## `api_tokens`

| Column        | Type                        | Notes                                          |
|---------------|-----------------------------|------------------------------------------------|
| `id`          | `BigInteger` PK (snowflake) |                                                |
| `user_id`     | `BigInteger` FK → `users.id`, indexed | Owner                                |
| `name`        | `String`                    | Human label                                    |
| `token_hash`  | `String`, unique, indexed   | `sha256(secret)`; plaintext shown once         |
| `expires_at`  | `datetime`                  | Required; expired → `401`                      |
| `last_used_at`| `datetime \| None`          | Updated on use (best-effort)                   |
| `created_at`  | `datetime` (`utc_now`)      |                                                |
| `revoked_at`  | `datetime \| None`          | Set on revoke; non-null → `401`                |

No `role` column: a token's effective role is **the owner's current role**, read
at request time. Demoting or disabling a user instantly de-privileges every
token they own — no stale admin tokens.

## `refresh_tokens`

| Column       | Type                        | Notes                                   |
|--------------|-----------------------------|-----------------------------------------|
| `id`         | `BigInteger` PK (snowflake) |                                         |
| `user_id`    | `BigInteger` FK → `users.id`, indexed |                               |
| `token_hash` | `String`, unique, indexed   | `sha256(secret)`                        |
| `expires_at` | `datetime`                  |                                         |
| `rotated_at` | `datetime \| None`          | Set when consumed by `/auth/refresh`    |
| `revoked_at` | `datetime \| None`          | Set on logout                           |

`Role = Literal["admin", "viewer"]` lives in `aaiclick/auth/models.py` with
module constants `ROLE_ADMIN` / `ROLE_VIEWER`.

# Module Layout

```
aaiclick/
  auth/
    models.py        users / api_tokens / refresh_tokens; Role literal
    security.py      bcrypt hash/verify; secret gen + sha256; JWT encode/decode
                     (pure functions, no DB, no contextvars)
    view_models.py   LoginRequest, TokenPair, RefreshRequest, MeView,
                     UserView, CreateUserRequest, ApiTokenView,
                     CreateTokenRequest, CreatedToken (one-time plaintext)
  internal_api/
    auth.py          login(), refresh(), logout(), me()  → view models
    users.py         create_user, list_users, set_role, disable_user, set_password
    tokens.py        create_token, list_tokens, revoke_token  (caller's own)
  server/
    auth.py          REWRITE: principal resolution + RBAC dependencies +
                     /mcp admin-only middleware + gating (replaces static bearer)
    routers/
      auth.py        /auth/login, /auth/refresh, /auth/logout, /auth/me
      users.py       /users   (admin-only)
      tokens.py      /tokens  (own tokens)
  __main__.py        `aaiclick user create|list|set-role|disable|passwd`
```

Business logic stays transport-agnostic in `internal_api` / `auth`, running
inside `orch_context` and reading the SQL session via the contextvar getter —
identical to every other `internal_api` module. `server/` owns JWT/transport.

# Auth Mechanics

## Password hashing

`bcrypt` (new dependency, added to the `server` extra). `security.py` exposes
`hash_password(pw) -> str` and `verify_password(pw, hash) -> bool`.

## Login → token pair

`POST /api/v0/auth/login` `{email, password}` → `200 TokenPair`:

```json
{ "access_token": "<jwt>", "refresh_token": "<opaque>", "token_type": "bearer",
  "expires_in": 1800 }
```

- Verify user exists, `disabled == false`, password matches. Else `401`
  (`code="unauthorized"`) — no user-enumeration distinction.
- Access JWT claims: `sub=<user_id>`, `role`, `exp`, `type="access"`,
  signed HS256 with `AAICLICK_JWT_SECRET`.
- Refresh token: random opaque secret; `sha256` stored in `refresh_tokens`.

## Refresh (rotation)

`POST /api/v0/auth/refresh` `{refresh_token}` → new `TokenPair`. Look up by
hash; reject if missing / expired / `revoked_at` / `rotated_at` set. On success
**rotate**: mark the old row `rotated_at` and issue a fresh refresh token
(re-reads the owner's current `role` and `disabled` from the DB). Reuse of a
rotated token ⇒ `401`.

## Logout

`POST /api/v0/auth/logout` `{refresh_token}` → revoke that refresh row. Access
JWTs are stateless and simply expire (≤ 30 min).

## Me

`GET /api/v0/auth/me` → `MeView {id, email, role}` for the current principal.

# Principal Resolution & RBAC

A single dependency `require_principal` replaces the old `require_bearer`. It
**does not parse the header by hand** — header extraction and the `/docs`
**Authorize** dialog come from FastAPI's built-in
`OAuth2PasswordBearer(tokenUrl="/api/v0/auth/login", auto_error=False)`
(the framework's standard "password login → bearer" helper; `auto_error=False`
so a missing credential yields our `Problem` envelope, not FastAPI's bare
`HTTPException`). `require_principal` then decodes/validates the extracted
credential and resolves a `Principal {user_id, email, role}`:

- **Auth disabled** → returns a synthetic admin principal; all routes open.
- **JWT** (`type="access"`, valid signature + `exp`) → trust claims for the
  token's ≤30-min lifetime (`sub`, `role`). Disabling or demoting a user thus
  takes full effect within one access-TTL — the next `/auth/refresh` re-reads
  the DB and fails / downgrades.
- **API token** (`aaic_…`) → look up `sha256` in `api_tokens`; reject if
  missing / expired / revoked; load owner; reject if `disabled`; role =
  **owner's current role**; best-effort `last_used_at` bump.
- Otherwise → `401` with `WWW-Authenticate: Bearer` (shared
  `problem_response`, as today).

`require_admin` depends on `require_principal` and raises `Forbidden` (`403`,
`code="forbidden"`) when `role != "admin"`.

## Role matrix

| Capability                                              | viewer | admin |
|---------------------------------------------------------|:------:|:-----:|
| `GET` reads (jobs, tasks, workers, objects, lineage)    | ✅     | ✅    |
| Own tokens (`/tokens` GET/POST/DELETE), `/auth/*`       | ✅     | ✅    |
| Run / cancel jobs, register / enable / disable jobs     | ❌     | ✅    |
| Delete / purge objects                                  | ❌     | ✅    |
| Start / stop workers                                    | ❌     | ✅    |
| User management (`/users`)                              | ❌     | ✅    |
| MCP surface (`/mcp`)                                    | ❌     | ✅    |

Enforcement: `require_principal` at every `/api/v0` router (via
`include_router(dependencies=...)`); `Depends(require_admin)` added to each
mutating route. Reads need only a valid principal. Per-router scope deps
(`orch_scope`) are unchanged and run alongside. FastAPI's dependency injection
covers all of this for the app's own routes — the **only** hand-rolled
transport guard is the `/mcp` ASGI middleware below, because `Depends` does not
propagate into mounted sub-apps.

# API Tokens (PATs)

`/api/v0/tokens`, **own tokens only** (no cross-user visibility, including for
admins):

- `POST /tokens` `{name, expires_in_days}` → `201 CreatedToken {id, name,
  token, expires_at}`. `token` (`aaic_<urlsafe>`) is the **only** time the
  plaintext is returned. `expires_in_days` ∈ presets `{7, 30, 90, 365}`
  (validated; expiry is mandatory).
- `GET /tokens` → `Page[ApiTokenView]` (metadata only: `id, name, expires_at,
  last_used_at, created_at, revoked`). Never the secret.
- `DELETE /tokens/{id}` → revoke (`404` if not the caller's).

# MCP Surface

The `/mcp` mount is **admin-only**. Because FastAPI's `Depends` does not reach
mounted sub-apps, the mount keeps its own raw ASGI middleware (the existing
`BearerAuthMiddleware`, evolved): it extracts the credential, runs the **same**
`require_principal` decode logic, and additionally requires `role == "admin"`;
non-admin or unauthenticated → `401`/`403` `Problem`. This middleware is the
one intentional exception to "let FastAPI's security handle it." No per-tool
RBAC this iteration — an MCP credential is all-or-nothing. (Per-tool read/write
RBAC is tracked in `docs/future.md`.)

# CLI & Admin Bootstrap

- **CLI** (`aaiclick user …`, thin renderers over `internal_api.users`):
  `create --email --password --role {admin,viewer}`, `list`, `set-role`,
  `disable`, `passwd`.
- **Startup seed**: when auth is enabled and `AAICLICK_ADMIN_EMAIL` /
  `AAICLICK_ADMIN_PASSWORD` are set, insert that admin during server lifespan
  startup **only if the `users` table is empty** (idempotent, container-friendly).
  Both the seed and the CLI solve the chicken-and-egg of the first admin.

# SPA Changes

- `src/api/client.ts`: attach `Authorization: Bearer <access>` from an auth
  store; on `401`, attempt `/auth/refresh` once, retry the request, else clear
  the session and redirect to **Login**. Single chokepoint — no per-call edits.
- `src/lib/auth.ts` (new): in-memory access token + `localStorage` refresh
  token; `login` / `logout` / `refresh` helpers; current-user (`/auth/me`).
- **Login view** (`src/views/Login.tsx`): email + password form.
- **Tokens view** (`src/views/Tokens.tsx`): list own tokens; create (name +
  expiry preset) with a one-time copy-the-secret dialog; revoke.
- **Route guard** in `App.tsx`: unauthenticated → Login. When auth is disabled
  the server's `/auth/me` returns the synthetic admin, so the UI behaves as
  today with no login wall.
- Admin-only nav/actions are hidden for viewers (cosmetic; the server is
  authoritative via `403`).

# Removing the Static Token

- Delete `AAICLICK_API_TOKEN` handling and the static-bearer path from
  `server/auth.py`.
- Update `docs/api_server.md` (Authentication, Configuration) to point here;
  remove the *Operator UI Auth* and *API Auth — DB-Backed Token Scopes* items
  from `docs/future.md`; refresh `aaiclick/server/CLAUDE.md`.

# Migration

One Alembic revision creating `users`, `api_tokens`, `refresh_tokens`,
generated via the `generate-migration` skill (GitHub Actions). The new
`aaiclick/auth/models.py` must be imported in `migrations/env.py` so
autogenerate sees the tables.

# Testing

- **`aaiclick/auth/`**: `security.py` — bcrypt hash/verify, JWT encode/decode
  (valid, expired, bad-signature, wrong `type`), secret hashing.
- **`internal_api/test_auth.py`**: login success/failure, disabled user,
  refresh rotation + reuse rejection, logout revocation.
- **`internal_api/test_users.py`** / **`test_tokens.py`**: user CRUD + role
  changes; token create / expiry / revoke; own-only scoping.
- **`server/`**: 401/403 plumbing & `Problem` shape; login → access → refresh
  flow; gating on/off (synthetic admin when off); `/mcp` admin-only;
  viewer-blocked-on-write. Deterministic — no `caplog` (patch loggers; see the
  `warn_if_open` precedent).

# Future (tracked in `docs/future.md`)

- Per-tool RBAC on the MCP surface (viewer-readable tools).
- Admin **user-management UI**.
- Token downscoping / custom scopes; OAuth 2.0 / OIDC / SSO; MFA;
  password-reset email flow; per-request audit log.
