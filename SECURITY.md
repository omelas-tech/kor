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

kord binds to `127.0.0.1` by default and currently performs **no
authentication or authorization** — it trusts every connection, like the
Firestore emulator does. Do not expose it to untrusted networks; put it on
loopback, a private network, or behind an authenticating proxy. Reports that
amount to "an exposed kord accepts writes" are working as documented (for
now — auth and security rules are on the roadmap).
