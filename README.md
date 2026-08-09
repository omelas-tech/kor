# Kor

[![ci](https://github.com/omelas-tech/kor/actions/workflows/ci.yml/badge.svg)](https://github.com/omelas-tech/kor/actions/workflows/ci.yml)
[![license](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)

**An open-source, self-hostable Firestore.** Kor implements the
`google.firestore.v1` wire protocol on top of PostgreSQL, so the official
Firestore SDKs — Go, Node (`firebase-admin`), Flutter (`cloud_firestore`),
JS (`firebase`) — talk to it without application code changes:

```bash
kord -pg-dsn "postgres://kor@localhost/kor"

FIRESTORE_EMULATOR_HOST=localhost:6565 ./your-existing-app   # that's it
```

*Kor* is Turkish for **ember**: the fire, kept at home.

## Quick start

```bash
git clone https://github.com/omelas-tech/kor && cd kor
docker compose up          # postgres + kord on localhost:6565
```

or build from source (Go 1.26+):

```bash
go install github.com/omelas-tech/kor/cmd/kord@latest
go install github.com/omelas-tech/kor/cmd/kor@latest    # CLI: import/verify/bench/stats
kord -pg-dsn "postgres://user@localhost/kor"
```

Then point any Firestore SDK at it — `FIRESTORE_EMULATOR_HOST=localhost:6565`
for server SDKs and tests, or the SDK's custom-host setting for production use
(Flutter `Settings(host:, sslEnabled:)`, JS `initializeFirestore(app, {host, ssl})`).
Kor also makes a pleasant **persistent local emulator**: unlike the official
emulator, data survives restarts and lives in a database you can inspect with SQL.

> **Security note:** kord currently has no authentication (like the emulator).
> Keep it on loopback or a private network. See [SECURITY.md](SECURITY.md).

## Why

There is no production-grade, wire-compatible open-source Firestore.
Google's emulator is in-memory and dev-only. Supabase/Appwrite/PocketBase
are fine products but require rewriting your entire data layer. Kor's goal
is different: **your data migrates and your code doesn't** — including a
functions runtime that executes your existing `firebase-functions` v2
codebase against Kor's change-log.

## Status: early, honest alpha

Working today (verified by tests against the real Go SDK):

- Documents: create / set / merge (`MergeAll`) / update with field masks /
  delete, preconditions (`exists`, `update_time`) with correct error codes
- Field transforms: `serverTimestamp`, `increment` (saturating, mixed
  int/double), `maximum`/`minimum`, `arrayUnion`, `arrayRemove`
- `Commit`, `BatchWrite`, `BatchGetDocuments`, `GetDocument`,
  `ListCollectionIds`
- Full Firestore type fidelity through PostgreSQL jsonb: int64 beyond 2^53,
  `-0.0`/`NaN`/`±Inf` doubles, bytes, references, geopoints, nested
  arrays/maps — property-tested round-trips
- A sort-key encoding whose memcmp order equals Firestore's documented
  cross-type ordering (the foundation for composite indexes), fuzz-verified
  against a reference comparator

On the roadmap (in build order):

| Phase | Scope |
|---|---|
| 1 | Queries (`RunQuery`), count aggregations, composite indexes, transactions |
| 2 | Change-log + functions runtime (runs `firebase-functions` v2 code unchanged) + cron scheduler |
| 3 | Realtime: `Listen`/`Write` streams for the native mobile SDKs |
| 4 | Security rules interpreter (`firestore.rules` verbatim) + Firebase Auth token verification |
| 5 | WebChannel transport (browser `firebase-js` support) |
| 6 | Migration tooling: streaming importer, verifier, bidirectional mirror |

Not in scope: Firebase Auth, Storage, FCM — Kor interoperates with them
rather than replacing them.

## Architecture (short version)

- **Tagged jsonb values.** Every Firestore value is stored as a single-key
  tagged JSON object (`{"i":"-42"}`, `{"d":"NaN"}`, …) with all numerics as
  strings — PostgreSQL can never corrupt type fidelity, and equality
  filters use GIN containment.
- **Order implemented once, in Go.** A `bytea` sort-key encoding gives
  memcmp order == Firestore order (unified int64/double numerics exact past
  2^53, segment-wise reference order, prefix-free framing so multi-field
  index keys compare correctly).
- **Composite indexes as rows**, maintained transactionally in an
  `index_entries` table — the way Firestore itself works — giving a
  deterministic query planner instead of praying to PostgreSQL's.
- **A low-water-marked change-log** (single sequencer) will power realtime
  listeners, triggers, and migration mirrors without dropped events.

## Development

```bash
go build ./...
go test ./...        # needs initdb/pg_ctl on PATH; spins a throwaway cluster
```

The e2e suite (`e2e/`) runs the official `cloud.google.com/go/firestore`
client against a full in-process Kor server.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the compatibility-testing
philosophy and PR guidelines, and [SECURITY.md](SECURITY.md) for how to report
vulnerabilities.

## License

Apache-2.0. See [LICENSE](LICENSE).
