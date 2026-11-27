Access
---

Local mode has no login: every request is allowed and the server binds
`127.0.0.1`. Distributed mode requires a signed-in user or an API token for
everything but sign-in itself.

# Configuration

<!-- req: auth-config -->
| Variable                      | Purpose                                                                 | Default          |
|-------------------------------|-------------------------------------------------------------------------|------------------|
| `AAICLICK_JWT_SECRET`         | Session signing secret. **Required** in distributed mode.               | unset            |
| `AAICLICK_JWT_ACCESS_TTL`     | Session access lifetime, seconds.                                       | `1800` (30 min)  |
| `AAICLICK_JWT_REFRESH_TTL`    | How long a sign-in lasts without re-entering the password, seconds.     | `1209600` (14 d) |
| `AAICLICK_ADMIN_USERNAME`     | First admin's username, created on startup when no users exist.         | `admin`          |
| `AAICLICK_ADMIN_PASSWORD`     | First admin's password; no seed admin without it.                       | unset            |
| `AAICLICK_PUBLIC_URL`         | Browser-facing origin, used in password-reset links.                    | unset            |
| `AAICLICK_PASSWORD_RESET_TTL` | Reset-link lifetime, seconds.                                           | `3600` (1 h)     |
| `AAICLICK_AUDIT_LOG`          | What the audit log records: `writes`, `all` or `off`.                   | `writes`         |

!!! warning "Distributed without a secret is a hard error"
    With `AAICLICK_JWT_SECRET` unset, or still the template's `change-me-...`
    placeholder, the server refuses to start. `local start --host 0.0.0.0`
    opens a local server to the network with no auth.

# Signing in

<!-- req: sign-in -->
Sign in with username and password; a session renews itself until the
refresh lifetime runs out or the user signs out. With multi-factor auth on,
sign-in asks for the 6-digit code from an authenticator app. Anyone may change
their own password from `@account`; doing so needs the current one.

# Roles

<!-- req: roles -->
| Role     | May                                                                     |
|----------|-------------------------------------------------------------------------|
| `viewer` | Every read: jobs, tasks, logs, objects, dashboards                      |
| `member` | Reads, plus saving and deleting dashboards                              |
| `admin`  | Everything: run, cancel and register jobs, objects, users, workers, audit |

A user created without a role is a `viewer`. Every role may submit to the
[sandbox](sandbox.md) from a signed-in session.

# API tokens

<!-- req: api-tokens -->
API tokens are for unattended clients (CI, SDK scripts, MCP agents). Create,
list and revoke your own from `@tokens`, or with the CLI:

```bash
python -m aaiclick token create <username> --name ci [--scope read|write|admin] [--expires-days N]
python -m aaiclick token list <username>
python -m aaiclick token revoke <id>
```

- A token looks like `aaic_…` and is shown **once**, when created.
- It expires when you choose (the UI defaults to 90 days) or never.
- Its scope is `read` (default), `write` or `admin`; each admits the ones
  below it. You can mint up to your own role's level.
- A token cannot create tokens, invite users or manage users: those need a
  signed-in session.

<!-- req: token-never-outranks-owner -->
!!! important "A token never outranks its owner"
    A token holds the lower of its own scope and its owner's current role.
    Demote the owner and an `admin` token drops at once; disable the owner and
    every token stops working.

# Multi-factor auth

<!-- req: mfa -->
Turn on multi-factor auth from `@account`: scan the QR code (or enter the
secret) in any authenticator app, then confirm with a code. Turning it off
needs both the password and a code. Each code works once. There are no
recovery codes: an admin resets a lost device from `@users`.

# Password reset and invites

<!-- req: password-reset -->
An admin mints a one-time reset link from `@users` or with
`aaiclick user reset-link <user_id>` and hands it over; it expires after
`AAICLICK_PASSWORD_RESET_TTL`. Opening it shows the new-password form, and
setting a password signs the user out everywhere. There is no "email me a
link": **Forgot password?** tells the user to ask an admin.

<!-- req: invites -->
An admin invites a user from `@invite` or with
`aaiclick user invite <username> [--role viewer|member|admin] [--email]`. The
invite creates the account and returns its link; the account grants nothing
until the link is used.

!!! warning "Admin lockout has no in-app recovery"
    If the only admin loses their password, recover on a host with database
    access: `aaiclick user passwd <user_id>`. Keep a second admin.

# Admin bootstrap

<!-- req: admin-bootstrap -->
The first admin comes from `AAICLICK_ADMIN_PASSWORD` (created on startup while
there are no users) or from the CLI, which runs with admin rights on a host
with database access:

```bash
python -m aaiclick user create <username> [--password] [--email] [--role]
python -m aaiclick user list | set-role | disable | enable | passwd | set-email | reset-mfa
```

# Account pages

| Prompt          | View                                                                        | Who       |
|-----------------|-----------------------------------------------------------------------------|-----------|
| `@account`      | Change password, set up or turn off multi-factor auth                       | any user  |
| `@tokens`       | List, create, revoke your API tokens                                        | any user  |
| `@users`        | Create users, set role, disable / enable, set password or email, reset MFA, mint a reset link | admin     |
| `@invite`       | Invite a user                                                               | admin     |
| `@audit`        | Audit log with user and path filters                                        | admin     |
| `reset <token>` | Set a new password from a reset link                                        | anyone    |
