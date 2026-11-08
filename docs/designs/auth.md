Authentication, Users & RBAC
---

aaiclick authenticates its HTTP surfaces (REST + MCP) with username/password
users and role-based access control. The browser SPA and programmatic HTTP /
MCP clients share one login flow; the CLI runs `internal_api` in-process and
never crosses the HTTP auth layer.

# Scope

- **Users**: username + password in the orchestration SQL database, one role
  each.
- **Roles**: `viewer`, `member` or `admin`, held on `users.role`. No
  per-resource ACLs or custom roles. Every gate compares **scopes**, never
  roles — `ROLE_SCOPES` is the bridge.
- **Sessions**: password login → short-lived access JWT + rotating refresh
  token. One credential header everywhere: `Authorization: Bearer <access-jwt>`.
- **API tokens**: user-minted, named, optionally expiring bearer tokens on an
  ordered `read` < `write` < `admin` ladder, for unattended CLI / SDK / MCP
  clients — see [API Tokens](#api-tokens).
- **Mode-derived enforcement**: a hardcoded convention, not a flag —
  **disabled in local mode** (single-process chdb + SQLite; open, zero-config)
  and **enforced in distributed mode**.
- **Optional hardening**, each configuration-driven: TOTP multi-factor auth, a
  password-reset flow, a per-request audit log.

# Configuration

Enforcement follows the backend mode (`is_local()`), not an env var. These tune
the enforced (distributed) case:

| Variable                      | Purpose                                                                                 | Default          |
|-------------------------------|-----------------------------------------------------------------------------------------|------------------|
| `AAICLICK_JWT_SECRET`         | HS256 signing secret. **Required** in distributed mode.                                 | unset            |
| `AAICLICK_JWT_ACCESS_TTL`     | Access-JWT lifetime, seconds.                                                           | `1800` (30 min)  |
| `AAICLICK_JWT_REFRESH_TTL`    | Refresh-token lifetime, seconds.                                                        | `1209600` (14 d) |
| `AAICLICK_ADMIN_USERNAME`     | Seed-admin username (inserted on startup when no users exist).                          | `admin`          |
| `AAICLICK_ADMIN_PASSWORD`     | Seed-admin password.                                                                    | unset            |
| `AAICLICK_PUBLIC_URL`         | Browser-facing origin (`https://aaiclick.example.com`). Needed by password-reset links. | unset            |
| `AAICLICK_PASSWORD_RESET_TTL` | Reset-link lifetime, seconds — see [Password Reset](#password-reset).                   | `3600` (1 h)     |
| `AAICLICK_AUDIT_LOG`          | `writes` / `all` / `off` — see [Audit Log](#audit-log).                                 | `writes`         |

!!! warning "Distributed without a secret is a hard error"
    In distributed mode with `AAICLICK_JWT_SECRET` unset, or still the scaffold's
    `change-me-...` placeholder, the server refuses to start. In local mode auth
    is disabled, every request is allowed, and the server logs a single startup
    `WARNING`. `local start` binds `127.0.0.1` by default, and the server answers
    only allowed `Host` headers — see [DNS Rebinding](../user_guide/dns_rebinding.md).
    `--host 0.0.0.0` opens the server to the network with no auth.

# Data Model

**Implementation**: `aaiclick/auth/models.py`

IDs are snowflake `BigInteger` PKs. `role` (the one `Role` literal) and `scope`
are plain `String` columns typed with `Literal`s and validated in code (no DB
CHECK — see CLAUDE.md, "Prefer Literal"), so widening a set is a one-line code
change, not a constraint migration. `audit_log` lives in `aaiclick/audit/` — see
[Audit Log](#audit-log).

## `users`

| Column           | Type                        | Notes                                                       |
|------------------|-----------------------------|-------------------------------------------------------------|
| `id`             | `BigInteger` PK (snowflake) |                                                             |
| `username`       | `String`, unique, indexed   | Login identifier                                            |
| `password_hash`  | `String \| None`            | bcrypt; `None` → password login impossible until a reset    |
| `role`           | `String`, default `viewer`  | `Role` literal; `admin` runs the installation               |
| `disabled`       | `Boolean`, default `false`  | Disabled → cannot log in                                    |
| `email`          | `String \| None`            | Contact address                                             |
| `totp_secret`    | `String \| None`            | Base32 TOTP seed; set by MFA setup, live once `mfa_enabled` |
| `mfa_enabled`    | `Boolean`, default `false`  | Login demands a TOTP code                                   |
| `totp_last_step` | `BigInteger \| None`        | Time step of the last accepted code; refuses replays        |
| `failed_logins`  | `Integer`, default `0`      | Consecutive failed passwords or codes; reset on success     |
| `locked_until`   | `datetime \| None`          | Login refused until then                                    |
| `created_at`     | `datetime` (`utc_now`)      |                                                             |

## `refresh_tokens`

| Column       | Type                                  | Notes                                |
|--------------|---------------------------------------|--------------------------------------|
| `id`         | `BigInteger` PK (snowflake)           |                                      |
| `user_id`    | `BigInteger` FK → `users.id`, indexed |                                      |
| `token_hash` | `String`, unique, indexed             | `sha256(secret)`                     |
| `expires_at` | `datetime`                            |                                      |
| `rotated_at` | `datetime \| None`                    | Set when consumed by `/auth/refresh` |
| `revoked_at` | `datetime \| None`                    | Set on logout                        |

## `api_tokens`

| Column         | Type                                  | Notes                                                            |
|----------------|---------------------------------------|------------------------------------------------------------------|
| `id`           | `BigInteger` PK (snowflake)           |                                                                  |
| `user_id`      | `BigInteger` FK → `users.id`, indexed |                                                                  |
| `name`         | `String`                              | Free-text label (`"ci-deploy"`)                                  |
| `prefix`       | `String`                              | First 12 chars of the secret, for display                        |
| `token_hash`   | `String`, unique, indexed             | `sha256(secret)`                                                 |
| `scope`        | `String`                              | `ScopeLevel` literal — see [The scope ladder](#the-scope-ladder) |
| `expires_at`   | `datetime \| None`                    | `None` → never expires                                           |
| `last_used_at` | `datetime \| None`                    | Refreshed at most once a minute                                  |
| `revoked_at`   | `datetime \| None`                    |                                                                  |
| `created_at`   | `datetime` (`utc_now`)                |                                                                  |

## `password_reset_tokens`

| Column        | Type                                  | Notes                                      |
|---------------|---------------------------------------|--------------------------------------------|
| `id`          | `BigInteger` PK (snowflake)           |                                            |
| `user_id`     | `BigInteger` FK → `users.id`, indexed |                                            |
| `token_hash`  | `String`, unique, indexed             | `sha256(secret)`                           |
| `expires_at`  | `datetime`                            | `AAICLICK_PASSWORD_RESET_TTL`, default 1 h |
| `consumed_at` | `datetime \| None`                    | Single use                                 |

# Module Layout

| Package                  | Holds                                                                                                                                   |
|--------------------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `aaiclick/auth/`         | Tables + literals, env getters, raw DB store, view models; bcrypt / JWT / token format / TOTP as pure functions (no DB, no contextvars) |
| `aaiclick/audit/`        | `audit_log` table, insert + paged query                                                                                                 |
| `aaiclick/internal_api/` | Login, refresh, MFA, password reset, API tokens, users, invites, audit listing                                                          |
| `aaiclick/server/`       | Principal resolution (JWT + API token), RBAC dependencies, `/mcp` principal and per-tool RBAC middleware, audit middleware, routers     |
| `aaiclick/__main__.py`   | `aaiclick user\|token\|audit` commands                                                                                                  |

Business logic is transport-agnostic in `internal_api` / `auth`, running inside
`orch_context` and reading the SQL session via the contextvar getter. `server/`
owns JWT and transport.

# Auth Mechanics

Passwords are hashed with `bcrypt`; access JWTs are signed HS256 with
`AAICLICK_JWT_SECRET`.

## Login → token pair

`POST /api/v0/auth/login` `{username, password}` → `200 TokenPair`:

```json
{ "access_token": "<jwt>", "refresh_token": "<opaque>", "token_type": "bearer",
  "expires_in": 1800 }
```

- The user must exist, be enabled, have a password, and the password must
  match. Otherwise `401` (`code="unauthorized"`) — no user-enumeration
  distinction in body or timing: every rejection runs bcrypt, against a dummy
  hash when there is no real one.
- With MFA enabled, the request must also carry a valid `totp_code`; a correct
  password without one answers `401` `code="mfa_required"` so the client can
  prompt and retry — see [Multi-Factor Auth](#multi-factor-auth).
- Five failed passwords or codes in a row — at login, password change or MFA
  changes — lock the account for 15 minutes, so neither can be brute-forced. A
  locked login gets the same `401` "invalid username or password" as a wrong
  one, so a lock does not confirm the account exists. Success resets the count.
- Access JWT claims: `sub=<user_id>`, `role`, `exp`, `type="access"`.
- Refresh token: a random opaque secret; only its `sha256` is stored.

## Refresh (rotation)

`POST /api/v0/auth/refresh` `{refresh_token}` → new `TokenPair`. The row is
looked up by hash and rejected if missing / expired / rotated / revoked. On
success the old row is stamped `rotated_at` and a fresh token is issued,
re-reading the owner's current `role` and `disabled`. Reusing a rotated token is
`401`. The stamp is a conditional `UPDATE` (`rotated_at IS NULL AND revoked_at
IS NULL`), so two refreshes racing on one token mint one pair; the loser gets
`401`.

## Logout

`POST /api/v0/auth/logout` `{refresh_token}` revokes that refresh row. Access
JWTs are stateless and expire on their own (≤ 30 min). `GET /api/v0/auth/me`
returns `MeView {id, username, role, mfa_enabled}` for the current principal.

## Change own password

`PUT /api/v0/auth/me/password` `{current_password, new_password}` → `204`. Open
to **any** role — `/users` is admin-only, so otherwise a viewer could never
rotate their own credential. `current_password` is required so a stolen access
token alone cannot seize the account; a mismatch is `401`. Local mode has no
real user (see [Roles](#roles)), so the route answers `422` there.

## Session revocation

Every still-active refresh row of a user is stamped `revoked_at` on role change,
disable, admin password reset and self-service password change — a demotion
must not be outlived by a refresh token still minting the old claims, and
someone changing their password after a suspected leak needs the other party's
token dead.

!!! note "Revocation binds at the refresh boundary, not instantly"
    Access JWTs are verified by signature alone — no DB read — so a revoked
    user keeps their existing access token until it expires (≤ 30 min by
    default). Revocation closes the renewal chain; it does not claw back the
    token in flight. Immediate cutoff would mean a per-request DB lookup or a
    denylist, trading away that statelessness on every read. Tighten
    `AAICLICK_JWT_ACCESS_TTL` if the window matters more than refresh chatter.

# Principal Resolution & RBAC

**Implementation**: `aaiclick/server/auth.py`

`require_principal` reads the credential with `HTTPBearer(auto_error=False)` —
registering the `/docs` **Authorize** box, while a missing credential still
yields the `Problem` envelope rather than FastAPI's bare `HTTPException` — and
resolves a `Principal {user_id, role, scope, kind}`:

- **Auth disabled** → the synthetic admin; all routes open.
- **API token** (bearer starting `aaic_`) → looked up by hash; must be
  unrevoked, unexpired, and owned by an enabled user. The owner's *current*
  `role` is read on every request, so revocation and demotion bind instantly
  for tokens. `kind == "token"`, `scope` is the token's.
- **Valid access JWT** (`type="access"`, valid signature + `exp`) → `sub` and
  `role` are trusted for the token's lifetime, so disable / demote takes full
  effect only within one access-TTL (see
  [Session revocation](#session-revocation)). `kind == "session"`, `scope is
  None` — a session is unscoped, so its ceiling is its role.
- **Otherwise** → `401` with `WWW-Authenticate: Bearer`.

The resolved principal is stored on `request.state` so the audit middleware can
attribute the request afterwards. `require_principal` only authenticates: the
HTTP method says nothing about scope — `POST /viewer/query` and a dashboard run
are reads, open to a `read` token.

The guard a route declares is the one scope gate: `require_write` for a
member's own mutations, `require_admin` for everything else that mutates or
administers — jobs, objects, tasks, users, workers, audit. A test in
`aaiclick/server/test_app.py` fails any non-safe route without a guard.

# Roles

**Implementation**: `aaiclick/auth/models.py`

| Role     | Scope   | Holds                                                          |
|----------|---------|----------------------------------------------------------------|
| `viewer` | `read`  | Every read                                                     |
| `member` | `write` | Reads, plus their own saved queries and dashboards             |
| `admin`  | `admin` | The installation — jobs, objects, users, workers, setup, audit |

Authority is a property of the user: one `users.role` column, bridged to the
scope ladder by `ROLE_SCOPES`. Delegation is capped at what the delegator holds
— an admin mints any scope and invites any role; a member mints up to `write`
and cannot invite. A user created without an explicit role is a `viewer`.

Local mode has no credential, so a synthetic admin stands in: `role="admin"`
with `user_id=None`. That is why account routes needing a real user row answer
`422` there. The first admin comes from the startup seed or the CLI — see
[CLI & Admin Bootstrap](#cli--admin-bootstrap).

# API Tokens

**Implementation**: `aaiclick/internal_api/api_tokens.py`

Long-lived credentials for unattended clients (CI, SDK scripts, MCP agents)
that should hold neither a password nor a refresh token.

- **Format**: `aaic_` + 43 URL-safe random characters. The prefix lets the
  resolver route the credential without a JWT parse attempt, and lets secret
  scanners recognise it. Only `sha256(secret)` is stored; the raw secret is
  returned exactly once, in the create response.
- **Expiry**: optional `expires_at` (an aware value is normalized to naive
  UTC); `None` never expires. The SPA form defaults to 90 days.
- **Ownership**: tokens belong to their minter; disabling the user disables
  every token. A token cannot mint or revoke tokens (`403`) — token management
  needs a real session, so a leaked token cannot bootstrap a permanent foothold.
- **MFA**: not applied to tokens — that is the point of them. Minting one
  requires a session, which MFA already protected.

## The scope ladder

A token carries one of three ordered levels and admits its own level plus every
level beneath it. `read` is the default at every mint site.

| Level   | Admits, plus everything below                                                                                      | REST guard it mirrors     |
|---------|--------------------------------------------------------------------------------------------------------------------|---------------------------|
| `read`  | every read, including `POST /viewer/query` and dashboard runs                                                      | `require_principal` alone |
| `write` | a member's own mutations — saved queries, dashboards                                                               | `require_write`           |
| `admin` | everything else — run / cancel jobs, register, clear tasks, delete / purge objects, users, workers, setup, migrate | `require_admin`           |

A caller mints at or below `ROLE_SCOPES[their role]`; above the ceiling is `422`
naming the caller's own level.

!!! important "A token never outranks its owner"
    The ceiling applies at mint **and** on every request: a token holds the
    lower of its own scope and its owner's current role. Demote the owner from
    admin to viewer and an outstanding `admin` token drops to `read` at once.
    Disabling the owner stops it outright.

| Route                      | Guard   | Purpose                                                                  |
|----------------------------|---------|--------------------------------------------------------------------------|
| `GET /auth/tokens`         | session | The caller's tokens (`ApiTokenView`, no secret)                          |
| `POST /auth/tokens`        | session | `{name, scope, expires_at}` → `ApiTokenCreated` (includes `token`, once) |
| `DELETE /auth/tokens/{id}` | session | Revoke (`204`; another user's token is `404`)                            |

CLI (in-process, admin-equivalent): `aaiclick token create <username> --name <n>
[--scope read|write|admin] [--expires-days N]`, `token list <username>`,
`token revoke <id>`. The SPA's `@tokens` offers only the levels the signed-in
user may mint.

# MCP Surface

**Implementation**: `aaiclick/server/mcp_rbac.py`

With auth enabled, `/mcp` takes **API tokens only** — a session JWT there is
`401`. The surface is for unattended clients, an API token is their credential,
and the restriction gives every MCP principal a real level to gate on. Each
tool's tag *is* the level it needs on [the scope ladder](#the-scope-ladder), and
`tools/list` only shows what the caller may call.

| Tag     | Needs                    | Tools                                                                                                                                                                                                                                                                                                                          |
|---------|--------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `read`  | `read`; any user         | `list_jobs`, `get_job`, `job_stats`, `list_registered_jobs`, `list_tasks`, `get_task`, `get_task_logs`, `list_execution_workers`, `list_objects`, `get_object`, `lineage_graph`, `lineage_nodes`, `lineage_query`, `lineage_schema`, `query_object`, `list_saved_queries`, `list_dashboards`, `get_dashboard`, `run_dashboard` |
| `write` | `write`; member or admin | `save_query`, `delete_saved_query`, `save_dashboard`, `delete_dashboard`                                                                                                                                                                                                                                                       |
| `admin` | `admin`; admin           | `cancel_job`, `run_job`, `register_job`, `enable_job`, `disable_job`, `clear_task`, `delete_object`, `purge_objects`, `start_execution_worker`, `stop_execution_worker`, `setup`, `migrate`                                                                                                                                    |

A tool's required level is its highest tag, so a mistagged tool fails closed
rather than open.

FastAPI's `Depends` does not reach mounted sub-apps, so the mount is wrapped in
an ASGI middleware that checks the credential is an API token, resolves the
principal, rejects anonymous calls with a `401` `Problem`, and stores the
principal on the ASGI scope. A FastMCP middleware then runs on every
`tools/call` and `tools/list`, reads the principal from the current HTTP
request and applies the table above. Denials surface as tool errors.

In local mode and for in-process clients (`fastmcp.Client(mcp)`, no HTTP
request) the synthetic admin applies and every tool is open.

# CLI & Admin Bootstrap

- **CLI**: `aaiclick user create <username> [--password] [--email] [--role]`,
  `invite`, `list`, `set-role`, `disable`, `enable`, `passwd`, `set-email`,
  `reset-mfa`, `reset-link` — thin in-process renderers over
  `internal_api.users`. `aaiclick token ...` and `aaiclick audit list` likewise.
- **Startup seed**: with auth enabled and `AAICLICK_ADMIN_PASSWORD` set, an
  **admin** (username `AAICLICK_ADMIN_USERNAME`, default `admin`) is inserted
  during server lifespan startup if the `users` table is empty.

# SPA

- The API client attaches `Authorization: Bearer <access>`; on `401` it tries
  `/auth/refresh` once and retries, else clears the session and drops back to
  **Login**. Single chokepoint.
- The access token is held in memory; the refresh token in `localStorage`.
- The auth provider bootstraps from `/auth/me` and gates rendering on the
  session. With auth disabled `/auth/me` returns the synthetic admin, so no
  login wall appears.
- Login asks for a TOTP code after a `mfa_required` answer and links to the
  forgot-password form.
- Admin-only actions (run / cancel / register / enable-toggle, and the
  navigation buttons into those flows) render **disabled with a tooltip** for
  viewers rather than hidden — a greyed-out control shows the action exists and
  why it is unavailable, where hiding it reads as a missing feature.
  Presentation only; `require_admin` is still the enforcement.
- The header shows the signed-in username with a sign-out control. Account and
  admin views are prompt-driven like the rest of the UI:

| Prompt          | View                                                                                                | Who       |
|-----------------|-----------------------------------------------------------------------------------------------------|-----------|
| `@account`      | Change password, MFA setup / disable                                                                | any user  |
| `@tokens`       | List / create / revoke the caller's API tokens                                                      | any user  |
| `@users`        | User table: create, set role, disable / enable, set password, set email, reset MFA, mint reset link | admin     |
| `@invite`       | Invite a user — see [Invites](#invites)                                                             | admin     |
| `@audit`        | Audit log table with user / path filters                                                            | admin     |
| `reset <token>` | Set a new password from a reset link                                                                | anonymous |

# Multi-Factor Auth

**Implementation**: `aaiclick/auth/security.py`

TOTP (RFC 6238: SHA-1, 30 s step, 6 digits, ±1 step drift) on the standard
library — no new dependency. Any authenticator app works from the `otpauth://`
URI or the base32 secret.

| Route                        | Guard   | Purpose                                                          |
|------------------------------|---------|------------------------------------------------------------------|
| `POST /auth/me/mfa/setup`    | session | Generate a pending secret → `MfaSetupView {secret, otpauth_uri}` |
| `POST /auth/me/mfa/enable`   | session | `{code}` — verify against the pending secret, set `mfa_enabled`  |
| `POST /auth/me/mfa/disable`  | session | `{password, code}` — both factors required to turn it off        |
| `POST /users/{id}/mfa/reset` | admin   | Clear the secret and flag (lost-device recovery)                 |

Login with `mfa_enabled`: `{username, password}` alone → `401`
`code="mfa_required"`; a wrong `totp_code` → plain `401`. Each code admits once:
an accepted code's time step is claimed in `totp_last_step` (conditional update,
so racing logins cannot both win), and a code at or before it is refused —
including the code that just confirmed enrollment. Enabling MFA revokes the
user's other refresh tokens so every open session re-authenticates with the
second factor. There are no recovery codes: the admin reset is the recovery
path, matching the CLI-first admin model.

# Password Reset

**Implementation**: `aaiclick/internal_api/password_reset.py`

A reset token is a one-time secret bound to a user with a short TTL
(`AAICLICK_PASSWORD_RESET_TTL`). Consuming it sets the password and revokes the
user's sessions, like an admin reset.

- **Mint**: `POST /users/{id}/password-reset` (admin) →
  `PasswordResetLinkView {token, expires_at, url}`; the operator hands the link
  over out of band. CLI: `aaiclick user reset-link <user_id>`.
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

**Implementation**: `aaiclick/internal_api/invites.py`

Onboarding is a reset link by another name: `POST /invites` → `InviteView {user,
link}` creates the user with no password, grants their role and mints their
link in one call. The account grants nothing until the link is redeemed, since
login refuses any user whose `password_hash` is `None` — an unredeemed invite is
inert, not an open door.

Only an admin may invite, and may grant any role; anyone else is `403`.
Inviting needs a **session** (`require_session`), so an API token cannot mint
one. An account is exactly the permanent foothold a leaked token must not be
able to create for itself — the same reason token management and every `/users`
route are session-only.

CLI: `aaiclick user invite <username> [--role viewer|member|admin] [--email]`;
the in-process CLI is admin-equivalent and caps against nothing.

!!! note "Its own module and its own router"
    `password_reset` already imports `users`, so composing the two there would
    close an import cycle — `invites` imports both instead. The router is
    separate because `/users` takes `orch_scope` and the admin guard at the
    router level, while an invite only needs a session.

# Audit Log

**Implementation**: `aaiclick/audit/`

Who called what, when — one row per HTTP request under `/api/v0/` or `/mcp`,
written by an ASGI middleware after the response is produced. `/health`, docs
and static assets are never logged.

The path filter runs *before* the middleware opens the request's SQL context,
which also excludes the SSE stream (`GET /api/v0/events`,
`docs/designs/frontend.md`): a row written when a client disconnects would say
nothing, and wrapping the stream would pin one SQL engine open for the life of
every open browser tab.

| Column        | Type                        | Notes                                                                               |
|---------------|-----------------------------|-------------------------------------------------------------------------------------|
| `id`          | `BigInteger` PK (snowflake) |                                                                                     |
| `at`          | `datetime`                  | Request start                                                                       |
| `user_id`     | `BigInteger \| None`        | `None` for anonymous / local-mode calls                                             |
| `username`    | `String \| None`            | Denormalised so rows outlive user deletion; the attempted username on `/auth/login` |
| `auth_kind`   | `String`                    | `AuthKind` literal: `none` / `session` / `token`                                    |
| `method`      | `String`                    |                                                                                     |
| `path`        | `String`                    |                                                                                     |
| `action`      | `String \| None`            | MCP tool name for `/mcp` calls                                                      |
| `status`      | `Integer`                   | HTTP status                                                                         |
| `duration_ms` | `Integer`                   |                                                                                     |
| `client_ip`   | `String \| None`            |                                                                                     |

`AAICLICK_AUDIT_LOG` selects the policy: `writes` (default) records every
non-safe method plus every `/mcp` tool call; `all` also records reads (the SPA
polls every 2 s, so expect volume); `off` disables the middleware. Insert
failures are logged and never fail the request.

The principal comes from `request.state` (set by `require_principal` and the
`/mcp` mount middleware); the login route stamps the attempted username so
failed logins are attributable. `GET /audit` (admin) pages the table
newest-first with `user_id`, `path` prefix, `method` and `since` filters;
`aaiclick audit list` and the SPA's `@audit` mirror it.

# Migration

The auth tables and `audit_log` are created by the Alembic chain (both model
modules are imported in `migrations/env.py` so autogenerate sees them).
`aaiclick setup` builds them from `SQLModel.metadata` locally, so the revision
is only required for Postgres-backed deployments.
