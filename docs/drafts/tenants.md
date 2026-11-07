Tenants — Kubernetes Control Plane
---

Multi-tenancy as a fleet layer: a control plane that provisions one full
aaiclick installation per tenant, each in its own Kubernetes namespace.

Not implemented. Indexed from `docs/designs/future.md`.

!!! warning "Kubernetes only"
    Tenancy is a Kubernetes feature. There is no Compose or local-mode
    equivalent, and no abstraction over deployment backends. An
    installation not running on Kubernetes is single-tenant, and the
    installation itself carries no tenant state at all — see
    `docs/designs/auth.md` for the RBAC it does carry.

# Concept

A tenant is an *installation*, not a column. Creating one provisions a
namespace containing a complete aaiclick stack — server, worker, background
service, and its own Postgres and ClickHouse. Isolation is the namespace
boundary rather than a `WHERE` clause, so tenants may be mutually
distrusting parties, which the earlier metadata-level scheme explicitly
could not support.

The installation is unmodified aaiclick. It gains exactly one capability:
verifying access tokens signed by the control plane.

# Components

| Component            | Packaging                    | Runs in            |
|----------------------|------------------------------|--------------------|
| Control plane        | `aaiclick[tenants]`          | `aaiclick-system`  |
| Tenant installation  | `aaiclick[server,distributed]` | `tenant-<slug>`  |

The control plane is a distinct service with its own FastAPI app, its own
Postgres, its own SPA, and a ServiceAccount holding cluster RBAC to create
namespaces and apply releases. It does not share a database with any
installation.

It imports `aaiclick.auth` for the authorization vocabulary — `Role`,
`ScopeLevel`, `ROLE_SCOPES`, `scope_admits`, and the password and JWT
primitives in `aaiclick.auth.security` — so roles and scopes are the same
code, not a parallel implementation.

## Package layout

| Module                        | Responsibility                                  |
|-------------------------------|--------------------------------------------------|
| `tenants/models.py`           | `Installation`, `InstallationEvent`              |
| `tenants/store.py`            | CRUD against the control plane's own session     |
| `tenants/provisioner/state.py`| Lifecycle state machine                          |
| `tenants/provisioner/k8s.py`  | Namespace, secrets, release application          |
| `tenants/provisioner/chart.py`| Per-tenant Helm values rendering                 |
| `tenants/keys.py`             | Signing keypair and JWKS publication             |
| `tenants/routers/`            | REST surface                                     |
| `tenants/app.py`              | The FastAPI application                          |

## Prerequisite refactor

`aaiclick.auth` is not importable outside an orchestration context today:
`auth/store.py` reads the session through `orchestration.orch_context`, and
`auth/config.py` depends on `backend.is_local`. The control plane has its
own database and no orchestration state, so the shared session accessor
moves to a neutral module that both sides set — restructuring rather than
inline imports, per CLAUDE.md.

# Identity

The control plane owns identity. `users`, `tenants` and
`tenant_memberships` live in its database; an installation's own auth
tables stay empty.

Authorization splits cleanly: rules are **defined** centrally — a
membership grants one role per `(tenant, user)` exactly as the earlier
in-installation scheme did — and **enforced** locally, by each
installation's existing scope guards. This works without any lookup
because `Principal` is already built from JWT claims alone.

## Token shape

Access tokens are **audience-scoped**: `aud` names one tenant and the token
carries that tenant's role only. An installation rejects any token whose
audience is not its own.

A token enumerating the holder's roles across every tenant — as the
in-installation scheme's claims did — would disclose the fleet's shape to
each installation and let a compromised one replay the credential
elsewhere. One audience, one role.

## Signing

Single installations sign with HS256 and a shared secret. A fleet cannot:
every installation holding that secret could mint tokens for every other.

The control plane holds an asymmetric keypair (RS256 or EdDSA, via
`pyjwt[crypto]`), publishes the public half at a JWKS endpoint, and is the
only party that can sign. Installations verify only. HS256 remains the
default for single installations, so nothing changes for them.

The provisioner writes the current public key into each namespace as a
Secret, and installations also poll JWKS on a cache. Rotation is therefore
a control-plane action: publish the new key alongside the old, wait out the
access-token TTL, retire the old one — no fleet-wide redeploy.

## Request flow

A user authenticates once against the control plane and receives their
tenant list. Selecting a tenant mints a short-lived token for that
installation; the browser then talks **directly** to `<slug>.<domain>`.

The control plane is off the request path, so its outage does not take
tenants down. Refresh stays central, so revocation binds at the refresh
boundary — the same trust model, and the same bound, as a single
installation's.

# Data Model

Existing auth tables are reused unchanged. `Tenant` gains no columns;
provisioning state lives beside it, one row per tenant.

## `installations`

| Column             | Type                        | Notes                              |
|--------------------|-----------------------------|-------------------------------------|
| `tenant_id`        | `BigInteger` PK, FK         | 1:1 with `tenants.id`               |
| `namespace`        | `String`, unique            | `tenant-<slug>`                     |
| `status`           | `String`                    | `Status` literal, below             |
| `phase`            | `String`                    | Current step, for progress display  |
| `base_url`         | `String`, nullable          | The tenant's ingress URL            |
| `aaiclick_version` | `String`                    | Chart and image tag                 |
| `helm_release`     | `String`                    | Release name                        |
| `values_digest`    | `String`                    | Hash of applied values              |
| `error`            | `String`, nullable          | Terminal failure detail             |
| `created_at`       | `datetime`                  |                                     |
| `updated_at`       | `datetime`                  |                                     |

`status` is `Literal["pending", "provisioning", "ready", "failed",
"deleting", "deleted"]` on a plain `String` column — no CHECK constraint,
per CLAUDE.md.

`values_digest` makes re-application a no-op and makes drift detectable.

## `installation_events`

Append-only `(id, tenant_id, phase, level, message, created_at)`. Because
provisioning does not run through the job engine, this is what gives the UI
live progress and gives operators a post-mortem on a failed install.

# Provisioner

A dedicated background service in the control plane, claiming work under a
lease so only one replica acts on a given installation.

Provisioning is deliberately *not* an aaiclick job: infrastructure work
stays separate from tenant workloads, and a runaway provisioning step can
never be mistaken for a tenant's own.

## Phases

`namespace` → `secrets` → `databases` → `helm` → `migrate` → `health` → `ready`

Each phase is idempotent and individually resumable, so a provisioner
restart re-reads status and continues rather than restarting the install.

| Phase       | Action                                                             |
|-------------|--------------------------------------------------------------------|
| `namespace` | Create `tenant-<slug>` with labels identifying the tenant           |
| `secrets`   | Generate database credentials; write the control plane's public key |
| `databases` | Postgres and ClickHouse StatefulSets with persistent volumes        |
| `helm`      | `helm upgrade --install` with rendered per-tenant values            |
| `migrate`   | `aaiclick setup` and Alembic, as a Kubernetes Job, waited on        |
| `health`    | Poll the installation's health endpoint                             |

Failure records the error, emits an event, and retries with backoff. After
a bounded number of attempts the installation becomes `failed` and waits for
an administrator to retry or destroy it.

Destruction runs `helm uninstall`, then deletes the namespace and its
volumes. It is irreversible data loss and is gated behind explicit
confirmation.

## Chart gaps

The shipped chart (`aaiclick/deploy` — see `init_helm`) is a starter and is
not sufficient for a tenant as it stands:

- **No Ingress template.** Services are `ClusterIP` only, so direct routing
  needs an Ingress per tenant, with DNS and TLS.
- **Databases are evaluation-only.** `values.yaml` marks its in-cluster
  Postgres and ClickHouse as ephemeral with no persistence. A tenant needs
  StatefulSets with volumes, or per-tenant overrides pointing at managed
  databases.

# UI

A control-plane SPA, separate from an installation's: the fleet list with
per-tenant status, an installation detail view streaming
`installation_events` during provisioning, tenant creation and destruction,
and membership administration.

An installation's own SPA is unchanged — it never learns that a control
plane exists.
