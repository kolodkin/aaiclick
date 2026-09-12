Token Scopes
---

API tokens carry one of four ordered scopes — `read`, `write`, `admin`,
`superadmin` — and a token may be minted only at or below its owner's own
level. The scope is the single gate on the `/mcp` surface and the token half of
the gate on REST. Identity, token format, and storage are unchanged:
`docs/designs/auth.md` — API Tokens.

# Why

Today `TokenScope` is `read` | `write`, and `write` means "inherits the owner's
full roles". Two problems follow.

The scope cannot say *which* mutations. A superadmin's `write` token reaches
`migrate` and `start_execution_worker`; least privilege is unavailable short of
minting nothing at all.

The surfaces also disagree about who may mutate. REST takes the role from each
route's guard — `require_admin` for jobs and objects, only `require_tenant` for
the viewer's saved queries and dashboards — while MCP reads one tool tag for
both questions. So a member who may save a dashboard over REST is refused over
MCP, because that tag conflates "this mutates" with "an admin must do it".

One ordered scope separates the two questions: the ladder says how much a
credential may do, the tenant role still says who the caller is.

# The Ladder

| Level        | Admits, plus everything below            | REST guard it mirrors                         |
|--------------|------------------------------------------|-----------------------------------------------|
| `read`       | every read                               | safe method                                   |
| `write`      | member-level mutations — saved queries, dashboards | mutating method under `require_tenant` |
| `admin`      | tenant mutations — run / cancel jobs, register, clear tasks, delete / purge objects, memberships | `require_admin` |
| `superadmin` | instance operations — setup, migrate, worker start / stop, users, tenants | `require_superadmin` |

Ordering is the whole mechanism: a token admits its own level and every level
beneath it. `read` is the default at every mint site.

# Data Model

`ScopeLevel = Literal["read", "write", "admin", "superadmin"]` replaces
`TokenScope` in `aaiclick/auth/models.py`, alongside `SCOPE_READ` … and an
ordered `SCOPE_LEVELS` tuple whose index *is* the comparison. One helper,
`scope_admits(held, required) -> bool`, is the only place the ordering is read.

`api_tokens.scope` stays a plain `String` column typed with the literal, so
widening the set is this code change and nothing else — the reason CLAUDE.md
keeps closed string sets out of DB CHECK constraints. Existing `read` and
`write` rows stay valid, so there is no migration.

# Minting

A caller may mint at or below their own effective level:

| Owner is                          | May mint up to |
|-----------------------------------|----------------|
| `superadmin`                      | `superadmin`   |
| tenant admin in any tenant        | `admin`        |
| member of any tenant              | `write`        |
| no membership                     | `read`         |

Above that ceiling is `Invalid` (422) naming the caller's own level. Minting
still requires a session, so a token can never mint a token
(`docs/designs/auth.md` — API Tokens).

The ceiling reads "in any tenant" rather than "in the active tenant" because
the live per-tenant check below already caps what a token delivers. Its job
here is to keep a token's stated power honest: a viewer holding an `admin`
token that grants admin nowhere is a worse outcome than a refusal at mint.

# Enforcement

## MCP

Each tool's tag becomes its required level, and `authorize_tool` reduces to
`scope_admits(principal.scope, required)` plus the tenancy ceiling below. The
twelve tools tagged `write` today split: the viewer's saves and deletes stay
`write`, while job, registered-job, and object mutations become `admin`. Read
and superadmin tags are unchanged.

`/mcp` accepts **API tokens only** once auth is enabled; a session JWT there is
`401`. The surface is for unattended clients, an API token is their credential,
and the restriction is what gives every MCP principal a real scope to gate on.
Nothing sends a session JWT there today.

## REST

The same ladder, with a route's required level read off its existing guard and
method — `require_superadmin` → `superadmin`, `require_admin` → `admin`, a
mutating method under `require_tenant` → `write`, a safe method → `read`. This
replaces the `writes = method not in SAFE_METHODS` heuristic in
`enforce_scope`, so both surfaces answer the question the same way.

Session principals stay unscoped: a browser session is bounded by its user's
role, not by a ladder. Local mode is unchanged — no credential, synthetic
principal, everything open.

## Tenancy

A single scalar cannot say "admin in tenant A, viewer in tenant B", so the
scope is a **ceiling rather than a grant**: effective authority is the lesser
of the token's scope and the owner's live authority in the active tenant, with
`X-Tenant-Id` still selecting that tenant. A role reads onto the ladder the
same way the mint ceiling does — `superadmin` flag → `superadmin`, tenant
admin → `admin`, member → `write` — so the two are comparable.

!!! important "Design decision: a ceiling, not a grant"
    Binding a token to one tenant at mint — the fine-grained-PAT shape — would
    let the token answer the question alone, at the cost of a token per tenant
    and, more seriously, of freezing authority into it. API tokens resolve the
    owner's flag and memberships live on every request precisely so demotion
    binds immediately (`docs/designs/auth.md` — Principal Resolution). Freeze
    that and a demoted user keeps `admin` until somebody revokes the token.
    The ceiling keeps instant demotion while still bounding a leak.

# Surfaces

`--scope` gains the two new values in `aaiclick token create`; the SPA's
`@tokens` form offers the levels the signed-in user may mint and explains each
in a line. `ApiTokenView.scope` widens with the literal, so the generated SPA
types follow from `npm run gen-types`.

# Delivery Phases

| Phase | Deliverable                                                                 |
|-------|-----------------------------------------------------------------------------|
| 1     | `ScopeLevel` + `scope_admits`, mint ceiling, REST enforcement, docs          |
| 2     | MCP tool re-tagging, API-token-only `/mcp`                                   |
| 3     | CLI and SPA scope pickers                                                    |

Each phase is one commit. Business-logic tests live in
`aaiclick/internal_api/test_api_tokens.py` and `aaiclick/auth/`; the ladder
itself gets a table-driven test in `aaiclick/auth/test_models.py`; router and
`server/test_mcp_rbac.py` tests assert the plumbing.
