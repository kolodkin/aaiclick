API Auth Expansion — Implementation Plan
---

Spec: `docs/designs/auth.md` (API Tokens, MCP Surface, OIDC / SSO,
Multi-Factor Auth, Password Reset, Audit Log). One branch, one commit per
phase; TDD per phase (store → internal_api → router → CLI → SPA).

# Phase 1 — API tokens

- `auth/models.py`: `ApiToken`, `TokenScope` literal + constants.
- `auth/security.py`: `generate_api_token()` (`aaic_` prefix), `is_api_token()`.
- `auth/store.py`: create / list / get-active-by-hash / revoke / touch-last-used.
- `internal_api/api_tokens.py`: `create_token`, `list_tokens`, `revoke_token`.
- `server/auth.py`: `Principal.scope` / `.kind`; async `resolve_principal`
  handling `aaic_`; method-based scope enforcement; `request.state.principal`.
- `server/routers/auth.py`: `/auth/tokens` routes (session-only).
- `__main__.py` + `cli_renderers.py`: `aaiclick token create|list|revoke`.
- SPA: `@tokens` view, hooks, types.

# Phase 2 — MCP per-tool RBAC

- `server/mcp.py`: tag every tool `read` / `write` / `superadmin`.
- `server/mcp_rbac.py`: FastMCP middleware (`on_call_tool`, `on_list_tools`).
- `server/auth.py`: `PrincipalAuthMiddleware` replaces `AdminAuthMiddleware`.
- Tests: tag matrix, list filtering per role, tenant pinning, denial as tool error.

# Phase 3 — User admin UI

- Routes: `GET /users/{id}`, `POST /users/{id}/enable`, `PUT /users/{id}/email`.
- `CreateUserRequest.email`; CLI `user enable`, `user set-email`.
- SPA: `@users`, `@account` (password change), header username + sign-out.

# Phase 4 — OIDC

- `auth/config.py` OIDC getters; `auth/oidc.py` (discovery, PKCE, exchange,
  id_token validation); `OidcState` model + store; `internal_api/auth.py`
  `oidc_config` / `oidc_start` / `oidc_callback`; routes; SPA button + callback.
- Tests with `httpx.MockTransport` and an RSA-signed id_token.

# Phase 5 — MFA

- `security.py` TOTP; `User.totp_secret` / `mfa_enabled`; `LoginRequest.totp_code`;
  `MfaRequired` error + `ProblemCode.MFA_REQUIRED`; routes; admin reset; SPA.

# Phase 6 — Password reset

- `PasswordResetToken` model + store; `auth/mail.py`; internal_api + routes;
  CLI `user reset-link`; SPA forgot / `reset <token>` forms.

# Phase 7 — Audit log

- `audit/models.py`, `audit/store.py`, `internal_api/audit.py`,
  `server/audit.py` middleware, `/audit` router, CLI, SPA `@audit`.

# Finish

- Alembic migration via the `generate-migration` workflow on this branch.
- `npm run gen-types`, `npm run check`, `npm run build` (SPA smoke).
- Implementation references in `docs/designs/auth.md`; drop the item from
  `docs/designs/future.md`; delete this plan.
