Token Scopes
---

API tokens carry one of four ordered scopes — `read`, `write`, `admin`,
`superadmin`. The three tenant-scoped levels are **bound to one tenant** at
mint; `superadmin` is instance-wide because its operations cross tenants. A
token may be minted only at or below its owner's own level in the tenant it
names. The scope is the single gate on the `/mcp` surface and the token half of
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

The first three name a tenant and act only there. `superadmin` names none — its
operations are instance-level, and for the tenant-scoped tools it reaches it
selects a tenant with `X-Tenant-Id`, exactly as a superadmin session does
today.

# Data Model

`ScopeLevel = Literal["read", "write", "admin", "superadmin"]` replaces
`TokenScope` in `aaiclick/auth/models.py`, alongside `SCOPE_READ` … and an
ordered `SCOPE_LEVELS` tuple whose index *is* the comparison. One helper,
`scope_admits(held, required) -> bool`, is the only place the ordering is read.

`api_tokens.scope` stays a plain `String` column typed with the literal, so
widening the set is a code change and nothing else — the reason CLAUDE.md keeps
closed string sets out of DB CHECK constraints. Existing `read` and `write`
rows keep their meaning.

Binding does need schema: `api_tokens.tenant_id`, a nullable `BigInteger`
(plain column, not a DB FK — matching `jobs` and `table_registry`), null exactly
when the scope is `superadmin`. That is one Alembic revision, generated through
the `generate-migration` workflow. Existing rows backfill to the default tenant,
which is where their owners already act.

# Minting

A `read` / `write` / `admin` mint names a tenant, and the caller may mint at or
below their own role **in that tenant**:

| Owner's role in the named tenant | May mint up to |
|----------------------------------|----------------|
| tenant admin                     | `admin`        |
| member (viewer)                  | `write`        |
| not a member                     | nothing (404)  |

A superadmin may mint any level in any tenant, and is the only one who may mint
an untenanted `superadmin` token. Above the ceiling is `Invalid` (422) naming
the caller's own level; a tenant the caller cannot act in reads as missing
(404), never as forbidden, so tokens cannot probe for tenants.

Minting still requires a session, so a token can never mint a token
(`docs/designs/auth.md` — API Tokens).

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

A tenant-scoped token names its tenant, so `X-Tenant-Id` no longer selects one
for it: the header may be omitted, and one naming a *different* tenant is
`Invalid` (422) rather than quietly ignored — a client sending it has a bug
worth surfacing. `superadmin` tokens carry no tenant and still select with the
header, exactly as a superadmin session does.

This also shrinks resolution: `resolve_api_token` reads one membership — the
token's own tenant — instead of the owner's whole membership map.

Binding answers *where*, not *how much*. The level stays a **ceiling rather
than a grant**: effective authority is the lesser of the token's level and the
owner's live role in that tenant, where a role reads onto the ladder as
tenant admin → `admin`, member → `write`. A superadmin reads as tenant admin
everywhere (`role_in_tenant`), so a bound token they mint keeps working without
an explicit membership — and stops the moment the flag is cleared.

!!! important "Design decision: binding does not make the level a grant"
    It is tempting to treat a bound token as self-describing and skip the role
    lookup entirely. API tokens resolve the owner's flag and membership live on
    every request precisely so a demotion binds immediately
    (`docs/designs/auth.md` — Principal Resolution). Drop that and someone
    demoted from admin to viewer keeps an `admin` token until a human
    remembers to revoke it. The check costs nothing extra — it rides the query
    `resolve_api_token` already makes.

# Surfaces

`aaiclick token create` gains the two new `--scope` values and takes its tenant
from the existing top-level `--tenant` flag, so no new CLI concept appears. The
SPA's `@tokens` form offers the levels the signed-in user may mint in the tenant
they are acting in, one line of explanation each. `ApiTokenView` grows `tenant_id`
and widens `scope`, so the generated SPA types follow from `npm run gen-types`.

# Delivery Phases

| Phase | Deliverable                                                                          |
|-------|--------------------------------------------------------------------------------------|
| 1     | `ScopeLevel` + `scope_admits`, `api_tokens.tenant_id` + migration, mint ceiling, resolution |
| 2     | REST enforcement off guard + method, retiring the `SAFE_METHODS` heuristic            |
| 3     | MCP tool re-tagging, API-token-only `/mcp`                                            |
| 4     | CLI and SPA scope pickers                                                             |

Each phase is one commit. Business-logic tests live in
`aaiclick/internal_api/test_api_tokens.py` and `aaiclick/auth/`; the ladder
itself gets a table-driven test in `aaiclick/auth/test_models.py`; router and
`server/test_mcp_rbac.py` tests assert the plumbing.
