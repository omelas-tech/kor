# Differential testing against Firestore

Kor is a reimplementation of somebody else's API, and the failure mode that
matters is not a crash. It is agreeing with itself. A test suite written by the
same person who wrote the implementation encodes the same misunderstandings
twice and then reports green.

So the primary correctness evidence here is not assertions. It is a diff against
Firestore's own emulator.

```bash
firebase emulators:start --only firestore --project demo-kor
KOR_FUZZ_EMULATOR=127.0.0.1:8080 go test ./e2e/ -run Fuzz -count=1 -v
```

The test writes an identical corpus into both stores through the same unmodified
`cloud.google.com/go/firestore` client, generates queries, runs each against
both, and compares the returned document ids **in order**. It skips cleanly when
`KOR_FUZZ_EMULATOR` is unset, and runs in CI on every push with a seed derived
from the run number, so successive runs explore different corpora rather than
re-proving the same one. A failure prints its seed; `KOR_FUZZ_SEED` reproduces
it exactly.

## What it varies

The corpus spans Firestore's whole type order — null, booleans, NaN, ±Inf,
int64 past 2^53, negative zero, strings including non-ASCII, bytes, references,
geopoints, arrays, maps, nested maps — and omits fields from some documents,
because "the field is missing" is its own case in every ordering rule.

Queries vary by **shape**, not only by value: inequalities with their required
ordering, ordering with no filter, two-field ordering where ties in the first
are broken by the second, equality plus an unrelated ordering, `__name__`
ordering, `in` / `not-in` / `array-contains` / `array-contains-any` / `!=`,
nested field paths, and limit, offset and all four cursor senses on top.

The first version varied only the operand. It explored one shape several
hundred times and would have missed every shape bug.

It runs twice: once on the general path, once with composite indexes backfilled.
Without the second run, the index path's only evidence is that it agrees with
the general path — which proves consistency, not correctness. The run asserts a
non-zero count of index-served queries, because a run where the planner declined
everything would pass while silently duplicating the first.

## What it found

Four real divergences, on the first two runs, none of which I would have reached
by reasoning:

1. **`where x == 0` missed documents holding `-0.0`.** The stored encoding
   preserves the sign of a double zero, because a round trip must return what
   was written. Firestore equality does not distinguish them — and neither do
   Kor's own comparator and sort key, which both collapse `-0.0`. Containment
   probes were the single place the two spellings could drift apart, and they
   had.

2. **`in [null, x]` matched documents holding null.** Firestore matches none —
   even though `== null` matches. `not-in` already had this rule; `in` did not.

3. **`!=` and `not-in` excluded NaN.** Firestore excludes only null and a
   missing field. `a != 5` returns the NaN document; `a != NaN` does not. Two
   unit tests asserted the opposite, confidently. Relatedly `== NaN` *matches*
   NaN documents, so for filters NaN is a self-equal value rather than IEEE
   NaN — and `not-in` is not the complement of `in`: `in [NaN]` matches
   nothing while `not-in [NaN]` excludes nothing.

4. **The index planner ignored type buckets.** Firestore range comparisons apply
   only within the operand's type — `b <= "z"` matches strings, never the
   numbers that sort before them. The general path enforced that; the index
   path produced a byte range spanning type boundaries.

It also found Kor being quietly *more permissive* than Firestore, accepting a
duplicated `orderBy` field that Firestore rejects. That direction of divergence
is the one that hurts in a migration: code that works against Kor then fails
against Firestore.

Bug 4 is worth dwelling on. It did not produce wrong results — `runIndexed`
re-checks every fetched document against the query and returns an error when one
does not match, so it surfaced as a loud failure instead of a short result set.
That assertion looked redundant when it was written. It earned its place the
first day the fuzz exercised the code it guards.

## Reading the output

```
fuzz: 1200 queries, 120 docs, indexes=true — mismatches=0 korRefused=0 emuRefused=0
```

- **`mismatches`** — both stores answered, and the answers differ. Always a bug.
- **`korRefused`** — Firestore served a query Kor rejected. A real gap; fails the run.
- **`emuRefused`** — Kor served a query Firestore rejected. Not a wrong answer,
  but a compatibility divergence, so it is reported rather than passed silently.

## What it does not cover

Only `RunQuery`. Transactions, listeners and aggregations are tested
conventionally. Cross-collection-group queries, `OR` composites and
`array-contains` indexes are not yet in the generator — each is a place a bug
could still be hiding, and the honest reading of a green run is "no divergence
in the shapes generated", not "no divergence".
