# Security Policy

Kor is pre-1.0. There is no supported-versions matrix yet; fixes land on `main`.

## Reporting a vulnerability

Please do **not** open a public issue for security problems.

- Preferred: GitHub → Security → **Report a vulnerability** (private advisory)
  on this repository.
- Fallback: email **security@omelas.tech**.

You should receive an acknowledgement within a few days. Please include a
reproduction and the deployment shape (kord version, how it is exposed).

## Deployment expectations

kord has two endpoints.

The **client endpoint** (`-client-listen`, off by default) is the one meant
for untrusted callers: every request must carry a valid Firebase ID token (or
none, for signed-out access) and is decided by the security rules given with
`-rules`. A bug that lets a client endpoint read or write anything its rules
deny is a vulnerability — please report it. See
[docs/security.md](docs/security.md).

The **main endpoint** (`-listen`) binds to `127.0.0.1` by default and
performs **no authentication or authorization** — it is the trusted, admin
endpoint, like Firestore's service-account access, and trusts every
connection. Reachability is therefore authorization: any client
that can open a TCP connection has full read/write on every document, with no
identity and no per-collection rules.

That makes the listen address a security control rather than a config value,
so **kord refuses to start on a non-loopback address**. `0.0.0.0`, `::`, an
empty host (`:6565`) and any routable IP all abort with a non-zero exit and a
remedy in the log. Override only if you have put an authenticating layer in
front of it:

    kord -listen 0.0.0.0:6565 -i-know-this-is-unauthenticated
    # or KORD_ALLOW_PUBLIC_BIND=1

The flag is deliberately awkward and names the risk rather than the mechanism.
When it is set, every startup logs a warning saying what is exposed.

A firewall rule denying the port from anywhere but loopback is worth adding
regardless — defence against a future config edit, not against today's one.

Do not expose kord to untrusted networks; put it on loopback, a private
network, or behind an authenticating proxy. Reports that
amount to "an exposed main endpoint accepts writes" are working as
documented; serve apps from the client endpoint instead.
