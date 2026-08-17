# `kor` — command-line reference

One binary, five subcommands:

| | |
|---|---|
| [`import`](#kor-import) | copy a collection Firestore → Kor |
| [`export`](#kor-export) | replay a collection Kor → Firestore |
| [`verify`](#kor-verify) | diff the two, document by document |
| [`stats`](#kor-stats) | what Kor currently holds |
| [`bench`](#kor-bench) | interleaved point-read latency, Kor vs Firestore |

Flags shared by everything that talks to both stores:

| Flag | Meaning |
|---|---|
| `-collection` | collection id (required) |
| `-project` | GCP project id (required) |
| `-kor` | kord gRPC address, default `127.0.0.1:6565` |
| `-creds` | service-account JSON; defaults to `$GOOGLE_APPLICATION_CREDENTIALS` |

`kor` connects to kord over an explicit gRPC dial rather than
`FIRESTORE_EMULATOR_HOST`, so both clients coexist in one process — that is what
lets `verify` and `bench` compare the two stores directly.

---

## `kor import`

Copies a collection **Firestore → Kor**.

```bash
kor import -collection my_collection -project my-project \
           -state /var/lib/kor/my_collection.state
```

| Flag | Meaning |
|---|---|
| `-state` | cursor file, written after every committed batch, read on start |
| `-resume-after` | resume after this document id, overriding the state file |
| `-ids` | copy only the ids in this file, one per line |

**Resumability is not optional at scale.** A single long `Documents()` stream
over a large collection hits Firestore's "Query timed out" after roughly 60
seconds, so the importer pages with short-lived `OrderBy(__name__) + StartAfter`
queries instead. Transient page failures retry from the last *committed*
document rather than from the failed attempt's own progress, which may be behind
it — resuming from the attempt would silently restart the scan.

**Batches are capped by bytes as well as count.** Collections differ by two
orders of magnitude in document size; a fixed 100-document batch that is
comfortable for a text cache becomes a 20 MB commit on a mirror with large
documents, which exceeds gRPC's 4 MB default and surfaces as a deadline rather
than a size error.

`-ids` exists for the gap a long import leaves behind. Feed it the ids `verify`
reported as `missing` or `MISMATCH` and it reconciles just those, in seconds.

---

## `kor export`

Replays a collection **Kor → Firestore**. This is what makes a cutover
reversible: documents written after an application starts serving from Kor exist
only in Kor, so without a reverse path, "point it back at Firestore" silently
loses them.

```bash
kor export -collection my_collection -project my-project -dry-run   # look first
kor export -collection my_collection -project my-project
```

| Flag | Meaning |
|---|---|
| `-dry-run` | read and report, write nothing |
| `-rate` | max documents written per second, default `400`; `0` disables |
| `-state`, `-resume-after`, `-ids` | as `import` |

**`-rate` is the flag that matters.** Firestore throttles sustained writes to a
collection where Kor does not. Unthrottled, a large replay does not run fast —
it accepts a burst and then returns `RESOURCE_EXHAUSTED` partway through, which
is the worst possible failure for a tool you only reach for when something has
already gone wrong. The default sits inside Firestore's documented ~500/s
per-collection ceiling. Raise it only if you know the collection is small.

**It replays writes, not deletes.** A document deleted in Kor after cutover
survives in Firestore. Reconciling that would mean issuing deletes against the
store you are rolling back *to*, which is not a decision a replay tool should
make on its own. Run `verify` afterwards to see the difference.

---

## `kor verify`

Reads every document from the source and compares it against Kor's copy.

```bash
kor verify -collection my_collection -project my-project [-sample 10]
```

```
VERIFY: source=102684 checked=102684 missing=0 mismatch=0
```

`-sample N` checks every Nth document, for a fast signal on a large collection.

Read the two counters differently:

- **`missing`** means Kor does not have a document the source does. On a
  collection that is still taking writes this is expected — it counts the drift
  the import could not keep up with, not corruption. Close it with `import -ids`.
- **`mismatch`** means both stores have the document and the contents differ.
  This is the one that should be zero. A non-zero mismatch on a quiesced
  collection is a bug worth stopping for.

Comparison is structural, not textual. Timestamps compare by instant at
microsecond precision (Firestore's own resolution), bytes by content, references
by path, geopoints by coordinate, and `NaN` equals `NaN` — so int64 beyond 2^53
and the awkward doubles do not produce phantom mismatches.

---

## `kor stats`

What Kor is actually holding. Talks to Postgres directly, not through kord.

```bash
kor stats -pg-dsn "postgres://kor@localhost/kor"
```

```
COLLECTION                     DOCS         DATA
my_collection                102684       159 MB
```

The first thing to run after an import, and the cheapest way to notice that a
collection you believed was migrated is empty.

---

## `kor bench`

Point-read latency for the **same documents**, served by both stores,
interleaved from one process.

```bash
kor bench -collection my_collection -project my-project \
          -pg-dsn "postgres://kor@localhost/kor" -n 300
```

Interleaving matters: comparing two separately-timed runs measures the network
weather between them as much as the stores. Sampling real document ids from
Postgres rather than synthesising them means the numbers describe your data
shape, not a benchmark's.

Treat it as the per-operation ground truth beneath any endpoint-level
measurement — an endpoint blends caching, serialization and everything else, so
when the two disagree, this is the one that isolates the store.
