Authentication, Users & RBAC
---

Scope, data model, principal resolution and roles: `docs/architecture/security.md`.

aaiclick authenticates its HTTP surfaces (REST + MCP) with username/password
users and role-based access control. The browser SPA and programmatic HTTP /
MCP clients share one login flow; the CLI runs `internal_api` in-process and
never crosses the HTTP auth layer.

# Configuration

Enforcement follows the backend mode (`is_local()`), not an env var. These tune
the enforced (distributed) case:

The variables and their defaults are user-facing: [Access — Configuration](../user_guide/access.md#configuration).

<!-- req: mode-derived-gating -->
The lifespan enforces it at startup: distributed mode calls
`require_jwt_secret()` and `require_no_placeholder_credentials()` and refuses to
start without a real secret; local mode allows every request and
`warn_if_open()` logs one `WARNING`. What a user sees:
[Access — Configuration](../user_guide/access.md#configuration).

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

<!-- req: login-token-pair -->
`POST /api/v0/auth/login` `{username, password}` → `200 TokenPair`:

```json
{ "access_token": "<jwt>", "refresh_token": "<opaque>", "token_type": "bearer",
  "expires_in": 1800 }
```

<!-- req: login-rejections -->
- The user must exist, be enabled, have a password, and the password must
  match. Otherwise `401` (`code="unauthorized"`) — no user-enumeration
  distinction in body or timing: every rejection runs bcrypt, against a dummy
  hash when there is no real one.
- With MFA enabled, the request must also carry a valid `totp_code`; a correct
  password without one answers `401` `code="mfa_required"` so the client can
  prompt and retry — see [Multi-Factor Auth](#multi-factor-auth).
<!-- req: login-lockout -->
- Five failed passwords or codes in a row — at login, password change or MFA
  changes — lock the account for 15 minutes, so neither can be brute-forced. A
  locked login gets the same `401` "invalid username or password" as a wrong
  one, so a lock does not confirm the account exists. Success resets the count.
<!-- req: token-claims -->
- Access JWT claims: `sub=<user_id>`, `role`, `exp`, `type="access"`.
- Refresh token: a random opaque secret; only its `sha256` is stored.

## Refresh (rotation)

<!-- req: refresh-rotation -->
`POST /api/v0/auth/refresh` `{refresh_token}` → new `TokenPair`. The row is
looked up by hash and rejected if missing / expired / rotated / revoked. On
success the old row is stamped `rotated_at` and a fresh token is issued,
re-reading the owner's current `role` and `disabled`. Reusing a rotated token is
`401`. The stamp is a conditional `UPDATE` (`rotated_at IS NULL AND revoked_at
IS NULL`), so two refreshes racing on one token mint one pair; the loser gets
`401`.

## Logout

<!-- req: logout-and-me -->
`POST /api/v0/auth/logout` `{refresh_token}` revokes that refresh row. Access
JWTs are stateless and expire on their own (≤ 30 min). `GET /api/v0/auth/me`
returns `MeView {id, username, role, mfa_enabled}` for the current principal.

## Change own password

<!-- req: change-own-password -->
`PUT /api/v0/auth/me/password` `{current_password, new_password}` → `204`. Open
to **any** role — `/users` is admin-only, so otherwise a viewer could never
rotate their own credential. `current_password` is required so a stolen access
token alone cannot seize the account; a mismatch is `401`. Local mode has no
real user (see [Roles](../architecture/security.md#roles)), so the route answers `422` there.

## Session revocation

<!-- req: session-revocation -->
Every still-active refresh row of a user is stamped `revoked_at` on role change,
disable, admin password reset and self-service password change — a demotion
must not be outlived by a refresh token still minting the old claims, and
someone changing their password after a suspected leak needs the other party's
token dead.

!!! note "Revocation binds at the refresh boundary, not instantly"
    <!-- req: revocation-at-refresh-boundary -->
    Access JWTs are verified by signature alone — no DB read — so a revoked
    user keeps their existing access token until it expires (≤ 30 min by
    default). Revocation closes the renewal chain; it does not claw back the
    token in flight. Immediate cutoff would mean a per-request DB lookup or a
    denylist, trading away that statelessness on every read. Tighten
    `AAICLICK_JWT_ACCESS_TTL` if the window matters more than refresh chatter.

# API Tokens

**Implementation**: `aaiclick/internal_api/api_tokens.py`

What a user does with tokens, and the rules they follow:
[Access — API tokens](../user_guide/access.md#api-tokens). The scope ladder:
[Security — Roles](../architecture/security.md#roles).

<!-- req: token-format -->
- **Storage**: only `sha256(secret)` is stored. The `aaic_` prefix (then 43
  URL-safe random characters) lets the resolver route the credential without a
  JWT parse attempt, and lets secret scanners recognise it.
<!-- req: token-expiry -->
- **Expiry**: an aware `expires_at` is normalized to naive UTC; `None` never expires.
- **MFA**: not applied to tokens — that is the point of them. Minting one
  requires a session, which MFA already protected.

<!-- req: mint-ceiling -->
A mint above `ROLE_SCOPES[caller's role]` answers `422` naming the caller's own level.

<!-- req: token-capped-by-owner -->
The owner cap is enforced on every request, not only at mint: principal
resolution reads the owner's current role and takes the lower of it and the
token's scope.

<!-- req: token-routes -->
| Route                      | Guard   | Purpose                                                                  |
|----------------------------|---------|--------------------------------------------------------------------------|
| `GET /auth/tokens`         | session | The caller's tokens (`ApiTokenView`, no secret)                          |
| `POST /auth/tokens`        | session | `{name, scope, expires_at}` → `ApiTokenCreated` (includes `token`, once) |
| `DELETE /auth/tokens/{id}` | session | Revoke (`204`; another user's token is `404`)                            |

# MCP Surface

**Implementation**: `aaiclick/server/mcp_rbac.py`

<!-- req: mcp-tokens-only -->
With auth enabled, `/mcp` takes **API tokens only** — a session JWT there is
`401`. The surface is for unattended clients, an API token is their credential,
and the restriction gives every MCP principal a real level to gate on. Each
tool's tag *is* the level it needs on [the scope ladder](../architecture/security.md#roles), and
`tools/list` only shows what the caller may call.

<!-- req: mcp-tool-tags -->
| Tag     | Needs                    | Tools                                                                                                                                                                                                                                                                                                                   |
|---------|--------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `read`  | `read`; any user         | `list_jobs`, `get_job`, `job_stats`, `list_registered_jobs`, `list_tasks`, `get_task`, `get_task_logs`, `list_execution_workers`, `list_objects`, `get_object`, `lineage_graph`, `lineage_nodes`, `lineage_query`, `lineage_schema`, `query_object`, `list_dashboards`, `get_dashboard`, `run_dashboard`, `run_queries` |
| `write` | `write`; member or admin | `save_dashboard`, `delete_dashboard`                                                                                                                                                                                                                                                                                    |
| `admin` | `admin`; admin           | `cancel_job`, `run_job`, `register_job`, `enable_job`, `disable_job`, `unregister_job`, `clear_task`, `delete_object`, `purge_objects`, `setup`, `migrate`                                                                                                                                                              |

<!-- req: mcp-fail-closed -->
A tool's required level is its highest tag, so a mistagged tool fails closed
rather than open.

<!-- req: mcp-mount-middleware -->
FastAPI's `Depends` does not reach mounted sub-apps, so the mount is wrapped in
an ASGI middleware that checks the credential is an API token, resolves the
principal, rejects anonymous calls with a `401` `Problem`, and stores the
principal on the ASGI scope. A FastMCP middleware then runs on every
`tools/call` and `tools/list`, reads the principal from the current HTTP
request and applies the table above. Denials surface as tool errors.

<!-- req: mcp-local-open -->
In local mode and for in-process clients (`fastmcp.Client(mcp)`, no HTTP
request) the synthetic admin applies and every tool is open.

# CLI & Admin Bootstrap

The `user` / `token` / `audit` CLI verbs are thin in-process renderers over `internal_api.users`, `internal_api.api_tokens` and `aaiclick.audit`, admin-equivalent. Commands and the startup seed: [Access — Admin bootstrap](../user_guide/access.md#admin-bootstrap).

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
- Account and admin views are listed in [Access — Account pages](../user_guide/access.md#account-pages).

# Multi-Factor Auth

**Implementation**: `aaiclick/auth/security.py`

<!-- req: totp -->
TOTP (RFC 6238: SHA-1, 30 s step, 6 digits, ±1 step drift) on the standard
library — no new dependency. Any authenticator app works from the `otpauth://`
URI or the base32 secret.

<!-- req: mfa-routes -->
| Route                        | Guard   | Purpose                                                          |
|------------------------------|---------|------------------------------------------------------------------|
| `POST /auth/me/mfa/setup`    | session | Generate a pending secret → `MfaSetupView {secret, otpauth_uri}` |
| `POST /auth/me/mfa/enable`   | session | `{code}` — verify against the pending secret, set `mfa_enabled`  |
| `POST /auth/me/mfa/disable`  | session | `{password, code}` — both factors required to turn it off        |
| `POST /users/{id}/mfa/reset` | admin   | Clear the secret and flag (lost-device recovery)                 |

<!-- req: mfa-login -->
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

<!-- req: reset-token -->
A reset token is a one-time secret bound to a user with a short TTL
(`AAICLICK_PASSWORD_RESET_TTL`). Consuming it sets the password and revokes the
user's sessions, like an admin reset.

<!-- req: reset-mint-redeem -->
- **Mint**: `POST /users/{id}/password-reset` (admin) →
  `PasswordResetLinkView {token, expires_at, url}`; the operator hands the link
  over out of band. CLI: `aaiclick user reset-link <user_id>`.
- **Redeem**: `POST /auth/password-reset {token, new_password}` (public) →
  `204`, or `401` for an unknown / expired / consumed token.

<!-- req: reset-link-url -->
The link is `AAICLICK_PUBLIC_URL/?p=reset%20<token>`, which the SPA routes to
the new-password form; without that variable only the raw `token` is returned.

There is no self-service "email me a link" flow — mail delivery is not
implemented (`docs/designs/future.md`). The SPA's **Forgot password?** link
tells the user to ask an administrator.

## Invites

**Implementation**: `aaiclick/internal_api/invites.py`

<!-- req: invite-inert-user -->
Onboarding is a reset link by another name: `POST /invites` → `InviteView {user,
link}` creates the user with no password, grants their role and mints their
link in one call. The account grants nothing until the link is redeemed, since
login refuses any user whose `password_hash` is `None` — an unredeemed invite is
inert, not an open door.

<!-- req: invite-admin-session -->
Only an admin may invite, and may grant any role; anyone else is `403`.
Inviting needs a **session** (`require_session`), so an API token cannot mint
one. An account is exactly the permanent foothold a leaked token must not be
able to create for itself — the same reason token management and every `/users`
route are session-only.

<!-- req: invite-cli-unrestricted -->
The in-process `user invite` CLI is admin-equivalent and caps against nothing.

!!! note "Its own module and its own router"
    `password_reset` already imports `users`, so composing the two there would
    close an import cycle — `invites` imports both instead. The router is
    separate because `/users` takes `orch_scope` and the admin guard at the
    router level, while an invite only needs a session.

# Audit Log

**Implementation**: `aaiclick/audit/`

<!-- req: audit-scope -->
Who called what, when — one row per HTTP request under `/api/v0/` or `/mcp`,
written by an ASGI middleware after the response is produced. `/health`, docs
and static assets are never logged.

<!-- req: audit-filter-before-sql -->
The path filter runs *before* the middleware opens the request's SQL context,
which also excludes the SSE stream (`GET /api/v0/events`,
`docs/designs/frontend.md`): a row written when a client disconnects would say
nothing, and wrapping the stream would pin one SQL engine open for the life of
every open browser tab.

<!-- req: audit-row -->
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

<!-- req: audit-policy -->
`AAICLICK_AUDIT_LOG` selects the policy: `writes` (default) records every
non-safe method plus every `/mcp` tool call; `all` also records reads (the SPA
polls every 2 s, so expect volume); `off` disables the middleware. Insert
failures are logged and never fail the request.

<!-- req: audit-attribution -->
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
