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
go install github.com/omelas-tech/kor/cmd/kor@latest    # CLI: import/export/verify/bench/stats
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

Working today (verified by tests against the real Go SDK — and by a
production application's full API test suite passing against Kor unchanged):

- Documents: create / set / merge (`MergeAll`) / update with field masks /
  delete, preconditions (`exists`, `update_time`) with correct error codes
- Field transforms: `serverTimestamp`, `increment` (saturating, mixed
  int/double), `maximum`/`minimum`, `arrayUnion`, `arrayRemove`
- **Structured queries** (`RunQuery`): all filter operators (`==`, ranges
  with Firestore's type-bucket semantics, `!=`, `in`/`not-in`,
  `array-contains[-any]`, unary null/NaN filters, AND/OR composites),
  Firestore-exact ordering rules (implicit inequality ordering, `__name__`
  tiebreaker, missing-orderBy-field exclusion), cursors
  (`startAt/After`, `endAt/Before`, snapshot cursors), offset/limit,
  projections and keys-only, collection-group queries
- **`count()` aggregations** (with `up_to`)
- **Optimistic transactions**: `BeginTransaction`/`Commit`/`Rollback` with
  read-version verification at commit under row + advisory locks — exact
  under contention, including concurrent create-if-absent races; conflicts
  return `ABORTED` so SDK `RunTransaction` retries transparently
- `Commit`, `BatchWrite`, `BatchGetDocuments`, `GetDocument`,
  `ListCollectionIds`
- Full Firestore type fidelity through PostgreSQL jsonb: int64 beyond 2^53,
  `-0.0`/`NaN`/`±Inf` doubles, bytes, references, geopoints, nested
  arrays/maps — property-tested round-trips
- A sort-key encoding whose memcmp order equals Firestore's documented
  cross-type ordering (the foundation for composite indexes), fuzz-verified
  against a reference comparator
- **A reversible migration path**: `kor import` copies a collection in from a
  live Firestore project (resumable, byte-aware batching), `kor verify` diffs
  the two, and `kor export` replays it back out — rate-limited to stay inside
  Firestore's write ceiling. Without the reverse direction a cutover is a
  one-way door: documents written after it exist only in Kor, so "point the app
  back at Firestore" silently loses them. Export replays writes, not deletes.

Query execution note: Postgres narrows candidates (collection bounds, jsonb
containment, `__name__`-ordered scans with cursor/limit pushdown) and full
Firestore semantics are re-evaluated in Go. Composite `index_entries`
execution for large hot query shapes is the next performance step — the
semantics above are the reference implementation it must match.

On the roadmap (in build order):

| Phase | Scope |
|---|---|
| 1 (remaining) | Composite `index_entries` execution for hot query shapes; differential fuzzing vs the Google emulator |
| 2 | Change-log + functions runtime (runs `firebase-functions` v2 code unchanged) + cron scheduler |
| 3 | Realtime: `Listen`/`Write` streams for the native mobile SDKs |
| 4 | Security rules interpreter (`firestore.rules` verbatim) + Firebase Auth token verification |
| 5 | WebChannel transport (browser `firebase-js` support) |
| 6 | Migration tooling: ~~streaming importer~~, ~~verifier~~, ~~reverse export~~ (all shipped, see [docs/cli.md](docs/cli.md)); bidirectional mirror remains |

Not in scope: Firebase Auth, Storage, FCM — Kor interoperates with them
rather than replacing them.

## Migrating an existing Firestore project

The CLI does the round trip. Full reference: **[docs/cli.md](docs/cli.md)**.

```bash
# 1. Copy a collection in. Resumable — re-run to continue after any interruption.
kor import -collection my_collection -project my-project -state /var/lib/kor/my_collection.state

# 2. Prove it matches, document by document.
kor verify -collection my_collection -project my-project
# VERIFY: source=102684 checked=102684 missing=0 mismatch=0

# 3. Point your application at Kor for that collection and watch it.

# 4. If you need to go back: replay Kor's writes to Firestore, then flip.
kor export -collection my_collection -project my-project -dry-run   # look first
kor export -collection my_collection -project my-project
```

Two things worth knowing before you plan a cutover.

**A long import outruns a clean verify.** `verify` compares against a *moving*
source: if the collection takes on writes faster than the import copies, it will
report `missing` no matter how correct the copy is. That is arithmetic, not a
bug. Either quiesce writes, or cap the write rate first and use `import -ids`
with the missing ids to close the remaining gap — which is seconds of work once
the gap is bounded.

**Migrate regenerable data first.** Caches and re-fetchable mirrors let you
treat a small window of lost writes as a non-event, which keeps early cutovers
cheap and reversible. Save the data you cannot reconstruct until you have
point-in-time recovery on Kor's Postgres *and* have actually restored from it
once. A backup nobody has restored is not a backup.

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
