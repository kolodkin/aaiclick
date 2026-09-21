Authentication, Users & RBAC
---

aaiclick authenticates its HTTP surfaces (REST + MCP) with username/password
users and role-based access control. The browser SPA
and any programmatic HTTP / MCP client share one login flow; the CLI runs
`internal_api` in-process and never crosses the HTTP auth layer.

# Scope

- **Users**: username + password, stored in the orchestration SQL database,
  each with one role.
- **Roles**: `viewer`, `member` or `admin`, held on `users.role`. No
  per-resource ACLs or custom roles.
  Every gate compares **scopes**, never roles — `ROLE_SCOPES` is the bridge.
- **Sessions**: password login → short-lived access JWT + rotating refresh
  token. One credential header everywhere: `Authorization: Bearer <access-jwt>`.
- **API tokens**: user-minted, named, optionally expiring bearer tokens on an
  ordered `read` < `write` < `admin` ladder, for unattended CLI / SDK / MCP
  clients — see
  [API Tokens](#api-tokens).
- **Mode-derived enforcement**: auth is a hardcoded convention, not a flag —
  **disabled in local mode** (single-process chdb + SQLite; the server is open,
  zero-config) and **enforced in distributed mode**.
- **Optional hardening**, each configuration-driven: TOTP
  multi-factor auth, a password-reset flow, and a per-request audit log.

# Configuration

Whether auth is enforced follows the backend mode (`is_local()`), not an env
var. These variables tune the enforced (distributed) case:

| Variable                   | Purpose                                                        | Default        |
|----------------------------|----------------------------------------------------------------|----------------|
| `AAICLICK_JWT_SECRET`      | HS256 signing secret. **Required** in distributed mode.        | unset          |
| `AAICLICK_JWT_ACCESS_TTL`  | Access-JWT lifetime, seconds.                                  | `1800` (30 min)|
| `AAICLICK_JWT_REFRESH_TTL` | Refresh-token lifetime, seconds.                               | `1209600` (14 d)|
| `AAICLICK_ADMIN_USERNAME`  | Seed-admin username (inserted on startup when no users exist).      | `admin`        |
| `AAICLICK_ADMIN_PASSWORD`  | Seed-admin password.                                           | unset          |
| `AAICLICK_PUBLIC_URL`      | Browser-facing origin (`https://aaiclick.example.com`). Needed by password-reset links. | unset |
| `AAICLICK_AUDIT_LOG`       | `writes` / `all` / `off` — see [Audit Log](#audit-log).        | `writes`       |

Password-reset variables are listed in their own section.

!!! warning "Distributed without a secret is a hard error"
    In distributed mode with `AAICLICK_JWT_SECRET` unset, the server refuses to
    start. In local mode auth is disabled, every request is allowed, and the
    server logs a single startup `WARNING`.

# Data Model

SQLModel tables in `aaiclick/auth/models.py` (`audit_log` in
`aaiclick/audit/models.py`). IDs are snowflake `BigInteger` PKs; `role` and
`scope` are plain `String` columns typed with `Literal`s and validated in code
(no DB CHECK — see CLAUDE.md, "Prefer Literal"), so widening the scope set is a
one-line code change rather than a hand-written constraint migration.

`Role` (`viewer` | `member` | `admin`) is the one role literal; it types
`users.role`, `CreateUserRequest`, `SetRoleRequest` and `InviteUserRequest`.

## `users`

| Column          | Type                            | Notes                  |
|-----------------|---------------------------------|------------------------|
| `id`            | `BigInteger` PK (snowflake)     |                        |
| `username`      | `String`, unique, indexed       | Login identifier       |
| `password_hash` | `String \| None`                | bcrypt; `None` until a reset sets one |
| `role`          | `String`, default `viewer`      | `Role` literal; `admin` runs the installation |
| `disabled`      | `Boolean`, default `false`      | Disabled → cannot log in |
| `email`         | `String \| None`                | Contact address                                                |
| `totp_secret`   | `String \| None`                | Base32 TOTP seed; set by MFA setup, live once `mfa_enabled` |
| `mfa_enabled`   | `Boolean`, default `false`      | Login demands a TOTP code |
| `created_at`    | `datetime` (`utc_now`)          |                        |

`password_hash` is nullable: a user created without one can never pass the
password login until a reset link sets it.

## `refresh_tokens`

| Column       | Type                        | Notes                                |
|--------------|-----------------------------|--------------------------------------|
| `id`         | `BigInteger` PK (snowflake) |                                      |
| `user_id`    | `BigInteger` FK → `users.id`, indexed |                            |
| `token_hash` | `String`, unique, indexed   | `sha256(secret)`                     |
| `expires_at` | `datetime`                  |                                      |
| `rotated_at` | `datetime \| None`          | Set when consumed by `/auth/refresh` |
| `revoked_at` | `datetime \| None`          | Set on logout                        |

## `api_tokens`

| Column         | Type                                  | Notes                                   |
|----------------|---------------------------------------|-----------------------------------------|
| `id`           | `BigInteger` PK (snowflake)           |                                         |
| `user_id`      | `BigInteger` FK → `users.id`, indexed |                                         |
| `name`         | `String`                              | Free-text label (`"ci-deploy"`)         |
| `prefix`       | `String`                              | First 12 chars of the secret, for display |
| `token_hash`   | `String`, unique, indexed             | `sha256(secret)`                        |
| `scope`        | `String`                              | `ScopeLevel` literal — see [The scope ladder](#the-scope-ladder) |
| `expires_at`   | `datetime \| None`                    | `None` → never expires                  |
| `last_used_at` | `datetime \| None`                    | Refreshed at most once a minute         |
| `revoked_at`   | `datetime \| None`                    |                                         |
| `created_at`   | `datetime` (`utc_now`)                |                                         |

## `password_reset_tokens`

| Column        | Type                                  | Notes                       |
|---------------|---------------------------------------|-----------------------------|
| `id`          | `BigInteger` PK (snowflake)           |                             |
| `user_id`     | `BigInteger` FK → `users.id`, indexed |                             |
| `token_hash`  | `String`, unique, indexed             | `sha256(secret)`            |
| `expires_at`  | `datetime`                            | `AAICLICK_PASSWORD_RESET_TTL`, default 1 h |
| `consumed_at` | `datetime \| None`                    | Single use                  |

## `audit_log`

See [Audit Log](#audit-log).

# Module Layout

```
aaiclick/
  auth/
    models.py        users / refresh_tokens / api_tokens /
                     password_reset_tokens;
                     Role + ScopeLevel literals + constants + scope_admits
    security.py      bcrypt hash/verify; secret gen + sha256; JWT encode/decode;
                     API-token format; TOTP (pure functions, no DB, no contextvars)
    config.py        env getters (enabled, secret, TTLs, admin seed, public
                     URL, reset TTL)
    store.py         raw DB CRUD over users / refresh_tokens / api_tokens /
                     password_reset_tokens; revoke_all_for_user
    view_models.py   LoginRequest, TokenPair, MeView, UserView, ApiTokenView,
                     InviteView, MfaSetupView, PasswordReset*, ...
  audit/
    models.py        audit_log table
    store.py         insert + paged query
  internal_api/
    auth.py          login(), refresh(), logout(), change_password(),
                     MFA setup/enable/disable, password reset
    api_tokens.py    create_token, list_tokens, revoke_token
    users.py         create_user, list_users, get_user, set_role,
                     disable_user, set_password, set_email, reset_mfa,
                     create_password_reset
    audit.py         list_audit
  server/
    auth.py          principal resolution (JWT + API token) + RBAC dependencies
                     + /mcp principal middleware
    mcp_rbac.py      FastMCP middleware: per-tool RBAC
    audit.py         ASGI middleware writing audit_log rows
    routers/
      auth.py        /auth/login, /auth/refresh, /auth/logout, /auth/me,
                     /auth/me/password, /auth/me/mfa/*, /auth/tokens,
                     /auth/password-reset*
      users.py       /users   (admin-only)
      audit.py       /audit   (admin-only)
  __main__.py        aaiclick user|token|audit commands
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

- The user must exist, be enabled, have a password,
  and the password must match. Otherwise `401` (`code="unauthorized"`) — no
  user-enumeration distinction.
- When the user has MFA enabled, the request must also carry a valid
  `totp_code`; a correct password without one answers `401`
  `code="mfa_required"` so the client can prompt for the code and retry — see
  [Multi-Factor Auth](#multi-factor-auth).
- Access JWT claims: `sub=<user_id>`, `role`, `exp`, `type="access"`.
- Refresh token: a random opaque secret; only its `sha256` is stored in
  `refresh_tokens`.

## Refresh (rotation)

`POST /api/v0/auth/refresh` `{refresh_token}` → new `TokenPair`. The row is
looked up by hash and rejected if missing / expired / rotated / revoked. On
success the old row is stamped `rotated_at` and a fresh refresh token is issued,
re-reading the owner's current `role` and `disabled`.
Reusing a rotated token returns `401`. The stamp is a conditional `UPDATE`
(`rotated_at IS NULL AND revoked_at IS NULL`), so two refreshes racing on one
token mint exactly one pair — the loser gets the same `401`.

## Logout

`POST /api/v0/auth/logout` `{refresh_token}` revokes that refresh row. Access
JWTs are stateless and expire on their own (≤ 30 min).

## Me

`GET /api/v0/auth/me` → `MeView {id, username, role, mfa_enabled}` for the
current principal.

## Change own password

`PUT /api/v0/auth/me/password` `{current_password, new_password}` → `204`. Open
to **any** role — `/users` is admin-only, so without this a viewer could
never rotate their own credential. `current_password` is required so a stolen access
token alone cannot seize the account, and a mismatch is `401`. Local mode has no
current user (the synthetic admin's `user_id` is `None`), so the route answers
`422` there.

## Session revocation

`store.revoke_all_for_user` stamps `revoked_at` on every still-active refresh row.
It runs on role change, disable, admin password reset,
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
`Principal {user_id, role, scope, kind}`:

- **Auth disabled** → a synthetic admin principal; all routes open.
- **API token** (bearer starting with `aaic_`) → looked up by hash; must be
  unrevoked, unexpired, and belong to an enabled user. The user's *current*
  `role` is read on every request, so revocation and demotion bind instantly
  for tokens. `Principal.scope` carries the token's
  scope and `Principal.kind == "token"`.
- **Valid access JWT** (`type="access"`, valid signature + `exp`) → claims are
  trusted for the token's ≤30-min lifetime (`sub`, `role`).
  Disabling or demoting a user revokes their refresh rows immediately (see
  *Session revocation*) but takes full effect on the access token only within
  one access-TTL. `Principal.kind == "session"` and `scope is None` — a
  session is unscoped, so its ceiling is its role.
- **Otherwise** → `401` with `WWW-Authenticate: Bearer`.

`require_principal` also stores the resolved principal on `request.state` so
the audit middleware can attribute the request after the fact, and enforces
the token scope: a `read`-scoped principal calling any non-safe HTTP method
(`POST` / `PUT` / `PATCH` / `DELETE`) is `403`.

Resource routers gate on scope: `require_scope(SCOPE_WRITE)` (aliased
`require_write`) for a member's own mutations, `require_scope(SCOPE_ADMIN)`
(aliased `require_admin`) for everything else that mutates or administers —
jobs, objects, tasks, users, workers, audit.

# Roles

**Implementation**: `aaiclick/auth/models.py` — see `Role`, `ROLE_SCOPES`;
`aaiclick/server/auth.py` — see `principal_to_scope`, `_SYNTHETIC_ADMIN`.

| Role     | Scope   | Holds                                                          |
|----------|---------|----------------------------------------------------------------|
| `viewer` | `read`  | Every read                                                     |
| `member` | `write` | Reads, plus their own saved queries and dashboards             |
| `admin`  | `admin` | The installation — jobs, objects, users, workers, setup, audit |

Authority is a property of the user: one `users.role` column, bridged to the
scope ladder by `ROLE_SCOPES`. Delegation is capped at what the delegator
holds — an admin mints any scope and invites any role; a member mints up to
`write` and cannot invite.

Local mode has no credential, so `_SYNTHETIC_ADMIN` stands in: `role="admin"`
with `user_id=None`. That is why account routes needing a real user row answer
`422` there.

The first admin comes from `AAICLICK_ADMIN_USERNAME` / `AAICLICK_ADMIN_PASSWORD`,
seeded during server startup when the `users` table is empty, or from the CLI.
A user created without an explicit role is a `viewer`.

# API Tokens


**Implementation**: `aaiclick/auth/models.py` — see `ApiToken`, `ScopeLevel`, `scope_admits`; `aaiclick/internal_api/api_tokens.py` — see `_mint_ceiling`, `create_token`; `aaiclick/server/auth.py` — see `principal_from_credential`, `principal_to_scope`, `enforce_scope`, `require_session`; `aaiclick/server/routers/auth.py` — see `create_token`; `src/views/Tokens.tsx`.
Long-lived credentials for unattended clients (CI, SDK scripts, MCP agents)
that should hold neither a password nor a refresh token.

- **Format**: `aaic_` + 43 URL-safe random characters. The prefix lets the
  resolver route the credential without a JWT parse attempt, and lets secret
  scanners recognise it. Only `sha256(secret)` is stored; the raw secret is
  returned exactly once, in the create response.
- **Expiry**: optional `expires_at`; `None` never expires. The SPA form defaults
  to 90 days.
- **Ownership**: tokens belong to the user who minted them. Disabling the user
  disables every token. A token cannot mint or revoke tokens (`403`) — token
  management needs a real session, so a leaked token cannot bootstrap a
  permanent foothold.
- **MFA**: not applied to tokens — that is the point of them. Minting one
  requires a session, which MFA already protected.

## The scope ladder

A token carries one of three ordered levels, and admits its own level plus
every level beneath it. `read` is the default at every mint site.

| Level        | Admits, plus everything below                                                                     | REST guard it mirrors                  |
|--------------|---------------------------------------------------------------------------------------------------|----------------------------------------|
| `read`       | every read                                                                                        | safe method                            |
| `write`      | a member's own mutations — saved queries, dashboards                                              | `require_write`                        |
| `admin`      | everything else — run / cancel jobs, register, clear tasks, delete / purge objects, users, workers, setup, migrate | `require_admin`       |

A caller mints at or below `ROLE_SCOPES[their role]` — you delegate what you
hold, never more. Above the ceiling is `422` naming the caller's own level.

!!! important "A token stands on its own scope"
    The ceiling applies **at mint**, not on every request. Once issued, the
    token's scope is its authority — a GitHub PAT, not a live projection of its
    owner. Demote the owner from admin to viewer and an outstanding `admin`
    token keeps working: **revoke it** to take it away. What does still stop it
    is being disabled, which `resolve_api_token` checks.

| Route                          | Guard                          | Purpose                                         |
|--------------------------------|--------------------------------|-------------------------------------------------|
| `GET /auth/tokens`             | session                        | The caller's tokens (`ApiTokenView`, no secret)  |
| `POST /auth/tokens`            | session                        | `{name, scope, expires_at}` → `ApiTokenCreated` (includes `token`, once)            |
| `DELETE /auth/tokens/{id}`     | session                        | Revoke (`204`; another user's token is `404`)   |

CLI (in-process, admin-equivalent): `aaiclick token create <username>
--name <n> [--scope read|write|admin] [--expires-days N]`; `token list <username>`,
`token revoke <id>`. The SPA exposes the same at `@tokens`, offering only the
levels the signed-in user may mint.

# MCP Surface


**Implementation**: `aaiclick/server/mcp_rbac.py` — see `required_level`, `authorize_tool`, `McpRbacMiddleware`; `aaiclick/server/auth.py` — see `PrincipalAuthMiddleware`; tool tags in `aaiclick/server/mcp.py`.
The `/mcp` mount takes **API tokens only** once auth is enabled — a session JWT
there is `401`. The surface is for unattended clients, an API token is their
credential, and the restriction is what gives every MCP principal a real level
to gate on. Each tool's tag *is* the level it needs on
[the scope ladder](#the-scope-ladder), and `tools/list` only shows what the
caller may call.

| Tag          | Tools                                                                                         | Needs                                         |
|--------------|-----------------------------------------------------------------------------------------------|-----------------------------------------------|
| `read`       | `list_jobs`, `get_job`, `job_stats`, `list_registered_jobs`, `get_task`, `list_execution_workers`, `list_objects`, `get_object`, `oplog_subgraph`, `query_table`, `get_table_schema`, `query_object`, `list_saved_queries`, `list_dashboards`, `get_dashboard`, `run_dashboard` | `read`; any user |
| `write`      | `save_query`, `delete_saved_query`, `save_dashboard`, `delete_dashboard`                       | `write`; member or admin                      |
| `admin`      | `cancel_job`, `run_job`, `register_job`, `enable_job`, `disable_job`, `clear_task`, `delete_object`, `purge_objects`, `start_execution_worker`, `stop_execution_worker`, `setup`, `migrate`, `bootstrap_ollama` | `admin`; admin |

`required_level` takes the highest tag present, so a mistagged tool fails
closed rather than open.

Because FastAPI's `Depends` does not reach mounted sub-apps, the mount is
wrapped in an ASGI middleware that checks the credential is an API token,
resolves the principal, rejects anonymous calls with a `401` `Problem`, and
stores the principal on the ASGI scope. A FastMCP middleware then runs on every
`tools/call` and `tools/list`: it reads the principal from the current HTTP
request and applies the table above. Denials surface as tool errors.

In local mode (auth disabled) and for in-process clients (`fastmcp.Client(mcp)`,
no HTTP request) the synthetic admin applies and every tool is open.

# CLI & Admin Bootstrap

- **CLI**: `aaiclick user create <username> [--password] [--email] [--role]`,
  `invite`, `list`, `set-role`, `disable`, `enable`, `passwd`, `set-email`,
  `reset-mfa`, `reset-link` — thin renderers over `internal_api.users`,
  running in-process. `aaiclick token ...` and `aaiclick audit list` likewise.
- **Startup seed**: when auth is enabled and `AAICLICK_ADMIN_PASSWORD` is
  set, an **admin** is inserted during server lifespan startup if the
  `users` table is empty (username from `AAICLICK_ADMIN_USERNAME`, default
  `admin`). The seed and the CLI both bootstrap the first admin.

# SPA

- `src/api/client.ts` attaches `Authorization: Bearer <access>` from the auth
  store; on `401` it attempts `/auth/refresh` once and retries, else clears the
  session and drops back to **Login**. Single chokepoint.
- `src/lib/auth.ts`: in-memory access token + `localStorage` refresh token;
  `login` / `logout` / `tryRefresh` / `fetchMe` helpers.
- `src/components/Auth.tsx`: `AuthProvider` / `useAuth`, bootstrapped from
  `/auth/me`; exposes `isAdmin` (`me.role === "admin"`).
- `src/components/AdminButton.tsx`: renders an admin-only action. Viewers get it
  **disabled with a tooltip** rather than hidden — a greyed-out control shows the
  action exists and why it is unavailable, where hiding it reads as a missing
  feature. Used for run / cancel / register / enable-toggle, and for the
  navigation buttons leading into those flows. Presentation only; `require_admin`
  is still the enforcement.
- `src/views/Login.tsx`: username + password form; asks for a TOTP code after a
  `mfa_required` answer; links to the forgot-password form.
- `App.tsx` gates rendering on the session. When auth is disabled `/auth/me`
  returns the synthetic admin, so no login wall appears.
- Account and admin views, all prompt-driven like the rest of the UI:

| Prompt          | View                                                            | Who            |
|-----------------|-----------------------------------------------------------------|----------------|
| `@account`      | Change password, MFA setup / disable                            | any user       |
| `@tokens`       | List / create / revoke the caller's API tokens                  | any user       |
| `@users`        | User table: create, set role, disable / enable, set password, set email, reset MFA, mint reset link | admin |
| `@audit`        | Audit log table with user / path filters                        | admin          |
| `reset <token>` | Set a new password from a reset link                            | anonymous      |

The header shows the signed-in username with a sign-out control.

# Multi-Factor Auth


**Implementation**: `aaiclick/auth/security.py` — see `totp_code`, `verify_totp`, `totp_uri`; `aaiclick/internal_api/auth.py` — see `login`, `mfa_setup`, `mfa_enable`, `mfa_disable`; `aaiclick/internal_api/users.py` — see `reset_mfa`; `aaiclick/internal_api/errors.py` — see `MfaRequired`; `src/views/Account.tsx` — see `MfaPanel`.
TOTP (RFC 6238: SHA-1, 30 s step, 6 digits, ±1 step drift), implemented on the
standard library in `aaiclick/auth/security.py` — no new dependency. Any
authenticator app works from the `otpauth://` URI or the base32 secret.

| Route                          | Guard   | Purpose                                                        |
|--------------------------------|---------|----------------------------------------------------------------|
| `POST /auth/me/mfa/setup`      | session | Generate a pending secret → `MfaSetupView {secret, otpauth_uri}` |
| `POST /auth/me/mfa/enable`     | session | `{code}` — verify against the pending secret, set `mfa_enabled` |
| `POST /auth/me/mfa/disable`    | session | `{password, code}` — both factors required to turn it off       |
| `POST /users/{id}/mfa/reset`   | admin   | Clear the secret and flag (lost-device recovery)             |

Login with `mfa_enabled` set: `{username, password}` alone → `401`
`code="mfa_required"`; with a wrong `totp_code` → plain `401`. Enabling MFA
revokes the user's other refresh tokens so every open session re-authenticates
with the second factor. There are no recovery codes: the admin reset is
the recovery path, matching the CLI-first admin model.

# Password Reset

**Implementation**: `aaiclick/internal_api/password_reset.py` — see `create`, `redeem`; `src/views/ResetPassword.tsx`.

A reset token is a one-time secret bound to a user with a short TTL
(`AAICLICK_PASSWORD_RESET_TTL`, default 3600 s). Consuming it sets the password
and revokes the user's sessions, like an admin reset.

- **Mint**: `POST /users/{id}/password-reset` (admin) →
  `PasswordResetLinkView {token, expires_at, url}` — the operator hands the
  link over out of band. CLI: `aaiclick user reset-link <user_id>`.
- **Redeem**: `POST /auth/password-reset {token, new_password}` (public) →
  `204`, or `401` for an unknown / expired / consumed token.

The link is `AAICLICK_PUBLIC_URL/?p=reset%20<token>`, which the SPA routes to
the new-password form; without that variable only the raw `token` is returned.

There is no self-service "email me a link" flow — mail delivery is not
implemented (`docs/designs/future.md`). The SPA's **Forgot password?** link
tells the user to ask an administrator.

!!! warning "Admin lockout has no in-app recovery"
    Minting a link needs an admin, so a deployment whose only admin loses
    their password must recover through the CLI on a host with database
    access (`aaiclick user passwd <user_id>`). Keep a second admin, or keep
    that step in the runbook.

## Invites

**Implementation**: `aaiclick/internal_api/invites.py` — see `invite`,
`_check_ceiling`; `aaiclick/server/routers/invites.py`;
`src/views/Invite.tsx`.

Onboarding is a reset link by another name: create the user with no password,
grant their role, and mint their link — `POST /invites` →
`InviteView {user, link}` does all three in one call. The account grants
nothing until the link is redeemed, since `login` refuses any user whose
`password_hash` is `None`, so an invite left unredeemed is inert rather than an
open door.

Only an admin may invite, and may grant any role; anyone else is `403`.

Inviting needs a **session**: the route guards on `require_session`, so an API
token cannot mint an invite. An account is exactly the permanent foothold a
leaked token must not be able to create for itself — the same reason token
management is session-only.

`aaiclick user invite <username> [--role viewer|member|admin] [--email]` is
the CLI form; the in-process CLI is admin-equivalent and caps against nothing.
The SPA offers `@invite` to admins.

!!! note "Its own module and its own router"
    `internal_api/password_reset.py` already imports `users`, so composing the
    two there would close an import cycle — `invites.py` imports both instead.
    The router is separate because `/users` takes `orch_scope` and the admin
    guard at the router level, while an invite only needs a session.

# Audit Log


**Implementation**: `aaiclick/audit/` (model, store, view models); `aaiclick/server/audit.py` — see `auditable_path`, `should_audit`, `AuditMiddleware`; `aaiclick/internal_api/audit.py`; `aaiclick/server/routers/audit.py`; `src/views/Audit.tsx`.

Who called what, when — one row per HTTP request under `/api/v0/` or `/mcp`,
written by an ASGI middleware after the response is produced. `/health`,
docs, and static assets are never logged.

`auditable_path` decides that *before* the middleware opens the request's SQL
context, which also excludes the SSE stream (`GET /api/v0/events`,
`docs/designs/frontend.md`): a row written when a client disconnects would say
nothing, and wrapping the stream would pin one SQL engine open for the life of
every open browser tab.

| Column        | Type                        | Notes                                              |
|---------------|-----------------------------|----------------------------------------------------|
| `id`          | `BigInteger` PK (snowflake) |                                                    |
| `at`          | `datetime`                  | Request start                                      |
| `user_id`     | `BigInteger \| None`        | `None` for anonymous / local-mode calls            |
| `username`    | `String \| None`            | Denormalised so rows outlive user deletion; the attempted username on `/auth/login` |
| `auth_kind`   | `String`                    | `AuthKind` literal: `none` / `session` / `token`   |
| `method`      | `String`                    |                                                    |
| `path`        | `String`                    |                                                    |
| `action`      | `String \| None`            | MCP tool name for `/mcp` calls                     |
| `status`      | `Integer`                   | HTTP status                                        |
| `duration_ms` | `Integer`                   |                                                    |
| `client_ip`   | `String \| None`            |                                                    |

`AAICLICK_AUDIT_LOG` selects the policy: `writes` (default) records every
non-safe method plus every `/mcp` tool call; `all` also records reads (the SPA
polls every 2 s, so expect volume); `off` disables the middleware. Insert
failures are logged and never fail the request.

The principal comes from `request.state` (set by `require_principal` and the
`/mcp` mount middleware); the login route stamps the attempted username so
failed logins are attributable. `GET /audit` (admin) pages the table
newest-first with `user_id`, `path` prefix, `method`, and `since` filters;
`aaiclick audit list` mirrors it and the SPA shows it at `@audit`.

# Migration

The auth tables (`users`, `refresh_tokens`, `api_tokens`,
`password_reset_tokens`) and `audit_log` are created by the Alembic chain
(`aaiclick/auth/models.py` is imported in `migrations/env.py` so autogenerate
sees them). Local/dev (`aaiclick setup`) builds the tables from
`SQLModel.metadata`, so the revision is only required for Postgres-backed
deployments.
