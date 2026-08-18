# Composite indexes

A query with no index reads every matching document, sorts them in Go, and
throws away all but the page you asked for. At a few thousand documents that is
invisible. At a few hundred thousand it is the difference between a fast
endpoint and a broken one, and it gets worse the deeper a reader pages.

`index_entries` is one small row per (index, document), holding the indexed
field values concatenated into a single `key`:

```
index_id   key                       doc_name
…          [ann][score 9][p001]      posts/p001
…          [ann][score 3][p014]      posts/p014
…          [bob][score 7][p009]      posts/p009
```

`key` is built with the same sort-key encoding the rest of Kor uses, whose
memcmp order **is** Firestore's total order. So a range scan over `(index_id,
key)` traverses results already in query order — which is what lets `LIMIT` and
`OFFSET` push down into Postgres. Cost becomes O(limit) instead of O(matching
documents).

This is materialized rows rather than a Postgres expression index for two
reasons: Firestore's ordering across mixed types is not Postgres's, so
`ORDER BY data->'score'` gives a different answer; and it keeps the plan
deterministic — Kor chooses the index rather than hoping Postgres guesses well
about jsonb selectivity.

---

## Defining indexes

The definition source is your existing `firestore.indexes.json` — the same file
the Firebase CLI reads. Kor does not invent a format: the file already exists,
it is already the reviewed record of which composite indexes your code needs,
and a second source would drift from it.

```bash
kor index apply -pg-dsn "$KORD_PG_DSN" -file firestore.indexes.json
```

By default this registers only indexes for collections **Kor actually holds**.
A project's index file describes its whole Firestore; registering all of it
against a store serving four collections costs work on every write with no
query able to read it. `-collection a,b` and `-all` override the default, and
`-dry-run` prints the selection without writing.

Two kinds are skipped, with the reason printed:

| Kind | Why |
|---|---|
| `arrayConfig` (array-contains) | an inverted index over element values, not an ordering |
| `vectorConfig` | an approximate-nearest-neighbour structure, not an ordering |

Neither can be expressed as a sort-key range. Approximating one would produce an
index the planner accepts and then serves wrong results from, which is worse
than not having it.

---

## Registering is not enabling

`apply` registers. It does not backfill, and a registered index serves nothing:

```
STATUS         ENTRIES  SPEC
PENDING              0  user_activities|userId asc|timestamp desc
```

A newly registered index holds entries only for documents written *after*
registration. Serving reads from it would silently return fewer documents than
the collection holds — the worst failure available here, because nothing errors
and the result simply looks like a smaller result set. So readiness is tracked
separately from registration, and only a completed backfill sets it:

```bash
kor index backfill -pg-dsn "$KORD_PG_DSN"      # every pending index
```

The backfill clears the index first and marks it ready last, so an interrupted
run leaves it pending rather than partially complete.

kord re-reads `index_defs` every 30 seconds, so this takes effect on a running
server — and a backfill finishing elsewhere is picked up on its own, rather than
leaving an index that is complete in the database but still pending in memory.

---

## What the planner will and will not serve

It serves a query whose equality filters form a **prefix** of the index, whose
ordering matches the remaining fields, and whose directions agree either
entirely or entirely inverted (an ascending index serves a descending query by
scanning backwards). Cursors and one inequality on the first ordering field
become bounds on the same byte range.

It declines — falling back to the general path, which is the reference
implementation — disjunctions, a second inequality field, an inequality off the
first ordering field, `__name__` filters, and equality outside the prefix.

A declined query is a lost optimisation. A wrongly-served one is a silent bug,
so the planner refuses anything it cannot serve exactly.

Two directions meet in the range arithmetic, and conflating them is the easy
mistake:

- an **inequality** constrains values, so which end of the range it bounds
  follows the **index field's** encoding — on a descending field the bytes are
  complemented and `score > 5` becomes an upper bound;
- a **cursor** names a position in **query** order, so which end it bounds
  follows whether the scan runs with or against the index.

Neither mistake errors. It returns a page from the wrong end, or repeats
documents across a page boundary — visible only under pagination, in
production. Both are covered by differential tests against the general path and
against the Google emulator.

---

## Correctness properties

**Entries are written in the same transaction as the document.** Split them and
a crash in between leaves entries describing values the document no longer has,
so an index-backed query returns documents that do not match its own filters.

**Each document's entries are rewritten wholesale** (delete all, insert
current) rather than diffed. Computing which indexes actually changed means
comparing old and new values per definition, and getting that wrong strands a
stale entry that stays invisible until a query returns something impossible.
Narrowing this is an optimisation to make when a benchmark asks for it.

**Every fetched document is re-checked against the query.** On a correct plan
this never fires, so it is an assertion rather than filtering — affordable
because only the limit-sized page is fetched. A failure returns an error rather
than skipping the document: dropping it would turn a planner bug into a quietly
short result set. This caught a real planner bug on the first day the fuzz
exercised it.

**A document missing an indexed field gets no entry**, matching Firestore, which
excludes such documents from a query ordering by that field.

---

## Operating

```bash
kor index list     -pg-dsn "$KORD_PG_DSN"
kor index backfill -pg-dsn "$KORD_PG_DSN" [-spec '…'] [-force]
kor index drop     -pg-dsn "$KORD_PG_DSN" -spec 'posts|author asc|score desc'
```

`drop` removes the entries before the definition. An index_defs row without
entries is merely unusable; entries without a definition are unreachable rows
that keep paying write cost with nothing able to read them.

Every registered index costs work on **every write** to its collection. An index
no query uses is pure write amplification, so drop what you do not need — and
prefer registering per collection over applying a whole project file.
