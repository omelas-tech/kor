# Contributing to Kor

Thanks for your interest! Kor is young and moving fast — issues, compatibility
reports, and focused PRs are all welcome.

## Development setup

- Go 1.26+
- PostgreSQL 15+ **binaries** on your PATH (`initdb`, `pg_ctl`) — the test
  suite spins up a throwaway cluster per test binary; no running server or
  configuration needed. On macOS: `brew install postgresql@16`. On
  Debian/Ubuntu the binaries live in `/usr/lib/postgresql/*/bin` (CI adds
  that to PATH).

```bash
go build ./...
go vet ./...
go test ./...        # includes the real-SDK e2e suite
```

## What correctness means here

Kor's contract is **bit-for-bit compatibility with Firestore semantics as the
official SDKs observe them**. That contract is enforced by layers of tests, and
changes are expected to keep or extend them:

1. `internal/value` property tests — the tagged-jsonb codec must round-trip
   `google.firestore.v1.Value` protos exactly, and the sortkey encoding's
   memcmp order must equal the reference comparator on random value pairs
   (including multi-field concatenation with mixed ASC/DESC).
2. `internal/store` tests — write semantics (merge masks, transforms,
   preconditions, same-document-twice-per-commit) against a real Postgres.
3. `e2e` — the unmodified official Go SDK (`cloud.google.com/go/firestore`)
   exercising a full in-process server.

A behavior change without a test demonstrating the Firestore-matching behavior
will not be merged. When in doubt about real Firestore semantics, the Google
emulator and the production service are the arbiters — cite which one you
checked (they occasionally disagree; production wins).

## Scope discipline

Kor deliberately implements the verified surface first and returns
`UNIMPLEMENTED` elsewhere. PRs that add protocol surface should come with a
concrete consumer story (which SDK path emits this RPC/feature?) rather than
speculative completeness.

## Style

Standard Go. `go vet` clean. Comments explain constraints the code can't
express — not what the next line does.
