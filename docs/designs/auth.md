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
- **API tokens**: user-minted, named, optionally expiring bearer tokens with a
  `read` / `write` scope for unattended CLI / SDK / MCP clients — see
  [API Tokens](#api-tokens).
- **Mode-derived enforcement**: auth is a hardcoded convention, not a flag —
  **disabled in local mode** (single-process chdb + SQLite; the server is open,
  zero-config) and **enforced in distributed mode**.
- **Optional hardening**, each configuration-driven: OIDC / SSO login, TOTP
  multi-factor auth, a password-reset flow, and a per-request audit log.

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
| `AAICLICK_PUBLIC_URL`      | Browser-facing origin (`https://aaiclick.example.com`). Needed by OIDC (redirect URI) and password-reset mail (link). | unset |
| `AAICLICK_AUDIT_LOG`       | `writes` / `all` / `off` — see [Audit Log](#audit-log).        | `writes`       |

OIDC, SMTP, and password-reset variables are listed in their own sections.

!!! warning "Distributed without a secret is a hard error"
    In distributed mode with `AAICLICK_JWT_SECRET` unset, the server refuses to
    start. In local mode auth is disabled, every request is allowed, and the
    server logs a single startup `WARNING`.

# Data Model

SQLModel tables in `aaiclick/auth/models.py` (`audit_log` in
`aaiclick/audit/models.py`). IDs are snowflake `BigInteger` PKs; `role` and
`scope` are plain `String` columns typed with `Literal`s and validated in code
(no DB CHECK — see CLAUDE.md, "Prefer Literal").

## `users`

| Column          | Type                            | Notes                  |
|-----------------|---------------------------------|------------------------|
| `id`            | `BigInteger` PK (snowflake)     |                        |
| `username`      | `String`, unique, indexed       | Login identifier       |
| `password_hash` | `String \| None`                | bcrypt; `None` for SSO-only users |
| `superadmin`    | `Boolean`, default `false`      | Instance-level operator |
| `disabled`      | `Boolean`, default `false`      | Disabled → cannot log in |
| `email`         | `String \| None`                | Password-reset mail target; filled from the OIDC `email` claim |
| `oidc_subject`  | `String \| None`, unique, indexed | `"<issuer>|<sub>"` for SSO-linked users |
| `totp_secret`   | `String \| None`                | Base32 TOTP seed; set by MFA setup, live once `mfa_enabled` |
| `mfa_enabled`   | `Boolean`, default `false`      | Login demands a TOTP code |
| `created_at`    | `datetime` (`utc_now`)          |                        |

`password_hash` is nullable: an SSO-provisioned user has no password and can
never pass the password login.

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
| `scope`        | `String`                              | `TokenScope` literal: `read` / `write`  |
| `expires_at`   | `datetime \| None`                    | `None` → never expires                  |
| `last_used_at` | `datetime \| None`                    | Refreshed at most once a minute         |
| `revoked_at`   | `datetime \| None`                    |                                         |
| `created_at`   | `datetime` (`utc_now`)                |                                         |

## `oidc_states`

One row per in-flight SSO login; consumed by the callback, expired rows are
inert (10-minute TTL).

| Column          | Type                        | Notes                          |
|-----------------|-----------------------------|--------------------------------|
| `id`            | `BigInteger` PK (snowflake) |                                |
| `state_hash`    | `String`, unique, indexed   | `sha256(state)`                |
| `nonce`         | `String`                    | Echoed in the `id_token`       |
| `code_verifier` | `String`                    | PKCE verifier                  |
| `expires_at`    | `datetime`                  |                                |
| `consumed_at`   | `datetime \| None`          | Single use                     |

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

`Role = Literal["admin", "viewer"]` lives in `aaiclick/auth/models.py` with
module constants `ROLE_ADMIN` / `ROLE_VIEWER`; it is the per-tenant membership
role. The `tenants` / `tenant_memberships` tables live in the same module —
see `docs/designs/tenant_rbac.md` — Data Model.

# Module Layout

```
aaiclick/
  auth/
    models.py        users / refresh_tokens / api_tokens / oidc_states /
                     password_reset_tokens / tenants / tenant_memberships;
                     Role + TokenScope literals + constants
    security.py      bcrypt hash/verify; secret gen + sha256; JWT encode/decode;
                     API-token format; TOTP (pure functions, no DB, no contextvars)
    config.py        env getters (enabled, secret, TTLs, admin seed, public URL,
                     OIDC, SMTP, reset TTL, audit policy)
    store.py         raw DB CRUD over users / refresh_tokens / api_tokens /
                     oidc_states / password_reset_tokens; revoke_all_for_user
    oidc.py          discovery, PKCE, code exchange, id_token validation (httpx)
    mail.py          SMTP sender for password-reset mail
    view_models.py   LoginRequest, TokenPair, MeView, UserView, ApiTokenView,
                     OidcStartView, MfaSetupView, PasswordReset*, ...
  audit/
    models.py        audit_log table
    store.py         insert + paged query
  internal_api/
    auth.py          login(), refresh(), logout(), change_password(), my_tenants(),
                     OIDC start/callback, MFA setup/enable/disable, password reset
    api_tokens.py    create_token, list_tokens, revoke_token
    users.py         create_user, list_users, get_user, set_superadmin,
                     disable_user, set_password, set_email, reset_mfa,
                     create_password_reset
    audit.py         list_audit
    tenants.py       tenant CRUD + membership management
  server/
    auth.py          principal resolution (JWT + API token) + RBAC dependencies
                     + /mcp principal middleware
    mcp_rbac.py      FastMCP middleware: per-tool RBAC + tenant pinning
    audit.py         ASGI middleware writing audit_log rows
    routers/
      auth.py        /auth/login, /auth/refresh, /auth/logout, /auth/me,
                     /auth/me/password, /auth/me/mfa/*, /auth/tokens,
                     /auth/oidc/*, /auth/password-reset*
      users.py       /users   (superadmin-only)
      audit.py       /audit   (superadmin-only)
      tenants.py     /tenants (see docs/designs/tenant_rbac.md)
  __main__.py        aaiclick user|tenant|member|token|audit commands
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

- The user must exist, be enabled, have a password (SSO-only users have none),
  and the password must match. Otherwise `401` (`code="unauthorized"`) — no
  user-enumeration distinction.
- When the user has MFA enabled, the request must also carry a valid
  `totp_code`; a correct password without one answers `401`
  `code="mfa_required"` so the client can prompt for the code and retry — see
  [Multi-Factor Auth](#multi-factor-auth).
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
- **API token** (bearer starting with `aaic_`) → looked up by hash; must be
  unrevoked, unexpired, and belong to an enabled user. The user's *current*
  `superadmin` flag and memberships are read on every request, so revocation
  and demotion bind instantly for tokens. `Principal.scope` carries the token's
  scope and `Principal.kind == "token"`.
- **Valid access JWT** (`type="access"`, valid signature + `exp`) → claims are
  trusted for the token's ≤30-min lifetime (`sub`, `superadmin`, `tenants`).
  Disabling or demoting a user revokes their refresh rows immediately (see
  *Session revocation*) but takes full effect on the access token only within
  one access-TTL. `Principal.kind == "session"`, `scope == "write"`.
- **Otherwise** → `401` with `WWW-Authenticate: Bearer`.

`require_principal` also stores the resolved principal on `request.state` so
the audit middleware can attribute the request after the fact, and enforces
the token scope: a `read`-scoped principal calling any non-safe HTTP method
(`POST` / `PUT` / `PATCH` / `DELETE`) is `403`. Role checks are unchanged, so a
`write` token is bounded by its owner's roles.

Tenant-scoped routers additionally resolve the active tenant via
`require_tenant` (the `X-Tenant-Id` header); `require_admin` means *tenant
admin* and `require_superadmin` guards instance-level surfaces. The role
matrix and resolution rules live in `docs/designs/tenant_rbac.md` — Role
Matrix / Active Tenant Resolution.

# API Tokens


**Implementation**: `aaiclick/auth/models.py` — see `ApiToken`; `aaiclick/internal_api/api_tokens.py`; `aaiclick/server/auth.py` — see `principal_from_credential`, `enforce_scope`, `require_session`; `aaiclick/server/routers/auth.py` — see `create_token`; `src/views/Tokens.tsx`.
Long-lived credentials for unattended clients (CI, SDK scripts, MCP agents)
that should hold neither a password nor a refresh token.

- **Format**: `aaic_` + 43 URL-safe random characters. The prefix lets the
  resolver route the credential without a JWT parse attempt, and lets secret
  scanners recognise it. Only `sha256(secret)` is stored; the raw secret is
  returned exactly once, in the create response.
- **Scope**: `TokenScope = Literal["read", "write"]` (`TOKEN_SCOPE_READ` /
  `TOKEN_SCOPE_WRITE`). `read` may call safe HTTP methods and `read`-tagged MCP
  tools only; `write` inherits the owner's full roles. A token never exceeds
  its owner: tenant membership and `superadmin` are read live at resolution.
- **Expiry**: optional `expires_at`; `None` never expires. The SPA form defaults
  to 90 days.
- **Ownership**: tokens belong to the user who minted them. Disabling the user
  disables every token. A token cannot mint or revoke tokens (`403`) — token
  management needs a real session, so a leaked token cannot bootstrap a
  permanent foothold.
- **MFA**: not applied to tokens — that is the point of them. Minting one
  requires a session, which MFA already protected.

| Route                          | Guard                          | Purpose                                         |
|--------------------------------|--------------------------------|-------------------------------------------------|
| `GET /auth/tokens`             | session                        | The caller's tokens (`ApiTokenView`, no secret)  |
| `POST /auth/tokens`            | session                        | `{name, scope, expires_at}` → `ApiTokenCreated` (includes `token`, once) |
| `DELETE /auth/tokens/{id}`     | session                        | Revoke (`204`; another user's token is `404`)   |

CLI (in-process, superadmin-equivalent): `aaiclick token create <username>
--name <n> [--scope read|write] [--expires-days N]`, `token list <username>`,
`token revoke <id>`. The SPA exposes the same at `@tokens`.

# MCP Surface


**Implementation**: `aaiclick/server/mcp_rbac.py` — see `authorize_tool`, `McpRbacMiddleware`; `aaiclick/server/auth.py` — see `PrincipalAuthMiddleware`; tool tags in `aaiclick/server/mcp.py`.
The `/mcp` mount admits **any authenticated principal**; each tool is gated
individually by a tag, and `tools/list` only shows what the caller may call.

| Tag          | Tools                                                                                         | Who may call                                  |
|--------------|-----------------------------------------------------------------------------------------------|-----------------------------------------------|
| `read`       | `list_jobs`, `get_job`, `job_stats`, `list_registered_jobs`, `get_task`, `list_execution_workers`, `list_objects`, `get_object`, `oplog_subgraph`, `query_table`, `get_table_schema` | Any member of the active tenant (viewer+); `read` tokens |
| `write`      | `cancel_job`, `run_job`, `register_job`, `enable_job`, `disable_job`, `clear_task`, `delete_object`, `purge_objects` | Tenant admin with `write` scope           |
| `superadmin` | `start_execution_worker`, `stop_execution_worker`, `setup`, `migrate`, `bootstrap_ollama`      | Superadmin with `write` scope                 |

Because FastAPI's `Depends` does not reach mounted sub-apps, the mount is
wrapped in an ASGI middleware that resolves the principal (JWT or API token),
rejects anonymous calls with a `401` `Problem`, and stores the principal on
the ASGI scope. A FastMCP middleware then runs on every `tools/call` and
`tools/list`: it reads the principal and the `X-Tenant-Id` header from the
current HTTP request, resolves the active tenant exactly like the REST
`require_tenant` (single membership implied, superadmins must name one), pins
the tenancy contextvar around the tool call, and applies the table above.
Denials surface as tool errors. `superadmin` tools never need a tenant.

In local mode (auth disabled) and for in-process clients (`fastmcp.Client(mcp)`,
no HTTP request) the synthetic superadmin applies and every tool is open.

# CLI & Admin Bootstrap

- **CLI**: `aaiclick user create <username> [--password] [--email] [--superadmin]`,
  `list`, `set-superadmin`, `disable`, `enable`, `passwd`, `set-email`,
  `reset-mfa`, `reset-link` — thin renderers over `internal_api.users`,
  running in-process. `aaiclick token ...` and `aaiclick audit list` likewise.
  Tenant and membership commands: `docs/designs/tenant_rbac.md` — CLI.
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
- `src/views/Login.tsx`: username + password form; asks for a TOTP code after a
  `mfa_required` answer; offers an SSO button when `/auth/oidc/config` reports a
  provider; links to the forgot-password form.
- `App.tsx` gates rendering on the session. When auth is disabled `/auth/me`
  returns the synthetic admin, so no login wall appears. An OIDC redirect
  (`?code=&state=` on the site root) is completed before the session probe.
- Account and admin views, all prompt-driven like the rest of the UI:

| Prompt          | View                                                            | Who            |
|-----------------|-----------------------------------------------------------------|----------------|
| `@account`      | Change password, MFA setup / disable                            | any user       |
| `@tokens`       | List / create / revoke the caller's API tokens                  | any user       |
| `@users`        | User table: create, superadmin toggle, disable / enable, set password, set email, reset MFA, mint reset link | superadmin |
| `@audit`        | Audit log table with user / path filters                        | superadmin     |
| `reset <token>` | Set a new password from a reset link                            | anonymous      |

The header shows the signed-in username with a sign-out control.

# OIDC / SSO


**Implementation**: `aaiclick/auth/oidc.py`; `aaiclick/auth/config.py` — see `oidc_settings`; `aaiclick/internal_api/auth.py` — see `oidc_start`, `oidc_callback`, `_resolve_oidc_user`; `src/lib/auth.ts` — see `startOidcLogin`, `completeOidcLogin`.
Authorization-code login against any OpenID Connect provider. The SPA drives
the redirect; the server holds the client secret and validates the `id_token`.

| Variable                        | Purpose                                                   | Default               |
|---------------------------------|-----------------------------------------------------------|-----------------------|
| `AAICLICK_OIDC_ISSUER`          | Issuer URL; discovery at `<issuer>/.well-known/openid-configuration`. Enables SSO when set with the client id. | unset |
| `AAICLICK_OIDC_CLIENT_ID`       | Registered client id                                      | unset                 |
| `AAICLICK_OIDC_CLIENT_SECRET`   | Client secret (`client_secret_post`); optional for public clients | unset          |
| `AAICLICK_OIDC_SCOPES`          | Requested scopes                                          | `openid profile email` |
| `AAICLICK_OIDC_USERNAME_CLAIM`  | `id_token` claim used as the aaiclick username            | `preferred_username`  |
| `AAICLICK_OIDC_AUTO_PROVISION`  | `1` → create unknown users on first login (no memberships) | `1`                  |
| `AAICLICK_OIDC_LABEL`           | Button label in the SPA                                   | `SSO`                 |

The redirect URI is `AAICLICK_PUBLIC_URL` + `/` — register that with the
provider.

1. `GET /auth/oidc/config` (public) → `{enabled, label}` so the SPA knows
   whether to show the button.
2. `POST /auth/oidc/start` (public) → `{authorization_url}`. The server runs
   discovery, generates `state`, `nonce`, and a PKCE verifier, stores their
   hashes in `oidc_states`, and builds the provider URL. The browser navigates
   there.
3. The provider redirects to `AAICLICK_PUBLIC_URL/?code=…&state=…`. The SPA
   posts both to `POST /auth/oidc/callback` (public) → `TokenPair`.
4. The server consumes the `oidc_states` row (missing, expired, or reused →
   `401`), exchanges the code at the token endpoint with the verifier, fetches
   the JWKS, and validates the `id_token`: signature, `iss`, `aud`, `exp`,
   `nonce`. Then it resolves the user:
    - `oidc_subject == "<issuer>|<sub>"` → that user.
    - Else a user whose `username` equals the username claim → linked (the
      subject is stored) — this is how existing password users adopt SSO.
    - Else, with auto-provision on, a new user with no password and no
      memberships; otherwise `401`.
    - A disabled user is `401`.
5. The regular token pair is minted; the SPA is now in the normal session flow.

MFA is not applied to SSO logins — the provider owns that factor. Password
login stays available alongside SSO for users that have a password.

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
| `POST /users/{id}/mfa/reset`   | superadmin | Clear the secret and flag (lost-device recovery)             |

Login with `mfa_enabled` set: `{username, password}` alone → `401`
`code="mfa_required"`; with a wrong `totp_code` → plain `401`. Enabling MFA
revokes the user's other refresh tokens so every open session re-authenticates
with the second factor. There are no recovery codes: the superadmin reset is
the recovery path, matching the CLI-first admin model.

# Password Reset


**Implementation**: `aaiclick/internal_api/users.py` — see `create_password_reset`; `aaiclick/internal_api/auth.py` — see `request_password_reset`, `redeem_password_reset`; `aaiclick/auth/mail.py`; `src/views/ResetPassword.tsx`.
A reset token is a one-time secret bound to a user with a short TTL
(`AAICLICK_PASSWORD_RESET_TTL`, default 3600 s). Consuming it sets the password
and revokes the user's sessions, like an admin reset.

- **Admin-minted**: `POST /users/{id}/password-reset` (superadmin) →
  `PasswordResetLinkView {token, expires_at, url}` — the operator hands the
  link over out of band. Always available; needs no mail server. CLI:
  `aaiclick user reset-link <user_id>`.
- **Self-service**: `POST /auth/password-reset/request {username}` (public)
  always answers `204`. When SMTP is configured and the user has an `email`,
  a mail with the link is sent; otherwise nothing happens beyond a log line.
  No user-enumeration signal either way.
- **Redeem**: `POST /auth/password-reset {token, new_password}` (public) →
  `204`, or `401` for an unknown / expired / consumed token.

The link is `AAICLICK_PUBLIC_URL/?p=reset%20<token>`, which the SPA routes to
the new-password form.

| Variable                   | Purpose                              | Default |
|----------------------------|--------------------------------------|---------|
| `AAICLICK_SMTP_HOST`       | Enables mail when set                | unset   |
| `AAICLICK_SMTP_PORT`       |                                      | `587`   |
| `AAICLICK_SMTP_USERNAME`   | Optional login                       | unset   |
| `AAICLICK_SMTP_PASSWORD`   |                                      | unset   |
| `AAICLICK_SMTP_FROM`       | Sender address                       | `AAICLICK_SMTP_USERNAME` |
| `AAICLICK_SMTP_STARTTLS`   | `1` → `STARTTLS` before login        | `1`     |

Mail is sent through `smtplib` on a worker thread (`asyncio.to_thread`), so the
request path stays async.

# Audit Log


**Implementation**: `aaiclick/audit/` (model, store, view models); `aaiclick/server/audit.py` — see `should_audit`, `AuditMiddleware`; `aaiclick/internal_api/audit.py`; `aaiclick/server/routers/audit.py`; `src/views/Audit.tsx`.
Who called what, when — one row per HTTP request under `/api/v0/` or `/mcp`,
written by an ASGI middleware after the response is produced. `/health`,
docs, and static assets are never logged.

| Column        | Type                        | Notes                                              |
|---------------|-----------------------------|----------------------------------------------------|
| `id`          | `BigInteger` PK (snowflake) |                                                    |
| `at`          | `datetime`                  | Request start                                      |
| `user_id`     | `BigInteger \| None`        | `None` for anonymous / local-mode calls            |
| `username`    | `String \| None`            | Denormalised so rows outlive user deletion; the attempted username on `/auth/login` |
| `auth_kind`   | `String`                    | `AuthKind` literal: `none` / `session` / `token`   |
| `tenant_id`   | `BigInteger \| None`        | Active tenant when one was resolved                |
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
failed logins are attributable. `GET /audit` (superadmin) pages the table
newest-first with `user_id`, `path` prefix, `method`, and `since` filters;
`aaiclick audit list` mirrors it and the SPA shows it at `@audit`.

# Migration

The auth tables (`users`, `refresh_tokens`, `api_tokens`, `oidc_states`,
`password_reset_tokens`, `tenants`, `tenant_memberships`) and `audit_log`
are created by Alembic revisions (this expansion: `ff9242208cc6`)
(`aaiclick/auth/models.py` is imported in `migrations/env.py` so autogenerate
sees them). Local/dev (`aaiclick setup`) builds the tables from
`SQLModel.metadata`, so the revision is only required for Postgres-backed
deployments.
