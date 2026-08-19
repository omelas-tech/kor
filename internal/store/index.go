package store

import (
	"context"
	"fmt"
	"sync"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/jackc/pgx/v5"

	"github.com/omelas-tech/kor/internal/index"
	"github.com/omelas-tech/kor/internal/value"
)

// Index registry and entry maintenance.
//
// The registry is empty by default, which makes every hook below a no-op: a
// deployment defining no indexes behaves exactly as it did before this existed
// and pays nothing for it. Indexes are opt-in, per collection.

type indexRegistry struct {
	mu     sync.RWMutex
	byColl map[string][]index.Def
	// ready is cached in memory: readiness changes only via MarkIndexReady or
	// a restart, and querying index_defs per query would put a database
	// round-trip on the hot path — which is precisely the cost this whole
	// feature exists to remove.
	ready map[int64]bool
}

func (r *indexRegistry) forCollection(collectionID string) []index.Def {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byColl[collectionID]
}

func (r *indexRegistry) readySet() map[int64]bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.ready
}

func (r *indexRegistry) markReady(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.ready == nil {
		r.ready = map[int64]bool{}
	}
	r.ready[id] = true
}

// SetIndexes replaces the in-memory registry and records each definition in
// index_defs.
//
// Registering does NOT backfill. Entries exist only for documents written
// afterwards, so a newly registered index is incomplete and must not serve
// reads until a backfill has run — an index-backed query over a partial index
// silently returns fewer documents than the collection holds, which is the
// worst failure available here. Readiness is tracked separately in index_defs
// (see MarkIndexReady / ReadyIndexes); registration alone never enables a read
// path.
func (s *Store) SetIndexes(ctx context.Context, defs []index.Def) error {
	byColl := map[string][]index.Def{}
	for _, d := range defs {
		if len(d.Fields) == 0 {
			return fmt.Errorf("store: index %q has no fields", d.Spec())
		}
		byColl[d.CollectionID] = append(byColl[d.CollectionID], d)
	}

	for _, d := range defs {
		if _, err := s.Pool.Exec(ctx, `
			INSERT INTO index_defs (index_id, collection_id, spec, is_group)
			VALUES ($1, $2, $3, $4)
			ON CONFLICT (index_id) DO NOTHING`,
			d.ID(), d.CollectionID, d.Spec(), d.Group); err != nil {
			return fmt.Errorf("store: register index %q: %w", d.Spec(), err)
		}
	}

	ready, err := s.ReadyIndexes(ctx)
	if err != nil {
		return fmt.Errorf("store: load index readiness: %w", err)
	}

	s.indexes.mu.Lock()
	s.indexes.byColl = byColl
	s.indexes.ready = ready
	s.indexes.mu.Unlock()
	return nil
}

// refreshIndexEntries rewrites one document's index entries inside the
// caller's transaction.
//
// Correctness rests on sharing that transaction with the document write. Split
// them, and a crash in between leaves entries describing values the document no
// longer has — an index-backed query would then return documents that do not
// match its own filters, which is far worse than being slow.
//
// The whole-document rewrite (delete all, insert current) is deliberate.
// Computing which indexes actually changed means diffing old and new values per
// definition, and getting that wrong strands a stale entry that stays invisible
// until a query returns something impossible. Delete-then-insert is always
// correct; narrowing it is an optimisation to make when a benchmark asks for it.
func (s *Store) refreshIndexEntries(ctx context.Context, tx pgx.Tx, name, collectionID string, fields map[string]*pb.Value, deleted bool) error {
	defs := s.indexes.forCollection(collectionID)
	if len(defs) == 0 {
		return nil
	}

	if _, err := tx.Exec(ctx, `DELETE FROM index_entries WHERE doc_name = $1`, name); err != nil {
		return fmt.Errorf("store: clear index entries for %s: %w", name, err)
	}
	if deleted {
		return nil
	}

	for _, d := range defs {
		key, ok := d.Key(name, fields)
		if !ok {
			// Missing an indexed field: Firestore omits the document from the
			// index entirely, which is why an orderBy excludes documents
			// lacking that field. Reproduced so index-backed results match the
			// general path exactly.
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO index_entries (index_id, key, doc_name) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, d.ID(), key, name); err != nil {
			return fmt.Errorf("store: write index entry %s/%s: %w", d.Spec(), name, err)
		}
	}
	return nil
}

// MarkIndexReady records that an index has been fully backfilled and may serve
// reads. Separate from registration on purpose: the gap between the two is
// exactly the window in which the index exists but is incomplete.
func (s *Store) MarkIndexReady(ctx context.Context, d index.Def) error {
	if _, err := s.Pool.Exec(ctx,
		`UPDATE index_defs SET ready_at = now() WHERE index_id = $1`, d.ID()); err != nil {
		return err
	}
	s.indexes.markReady(d.ID())
	return nil
}

// BackfillIndex populates an index from the documents already stored, then
// marks it ready.
//
// Until this runs, an index holds entries only for documents written since it
// was registered — so it is missing most of the collection while looking
// perfectly healthy. Readiness is set only at the end, and only on success:
// a partial backfill must never leave an index eligible for reads.
func (s *Store) BackfillIndex(ctx context.Context, d index.Def) (int64, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT name, data FROM documents WHERE collection_id = $1 ORDER BY name`, d.CollectionID)
	if err != nil {
		return 0, fmt.Errorf("store: backfill scan: %w", err)
	}
	type pending struct {
		name string
		key  []byte
	}
	var batch []pending
	for rows.Next() {
		var name string
		var raw []byte
		if err := rows.Scan(&name, &raw); err != nil {
			rows.Close()
			return 0, err
		}
		fields, err := value.UnmarshalFields(raw)
		if err != nil {
			rows.Close()
			return 0, fmt.Errorf("store: backfill decode %s: %w", name, err)
		}
		if key, ok := d.Key(name, fields); ok {
			batch = append(batch, pending{name, key})
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Clear first: a re-run after a definition change or a partial attempt must
	// not merge old entries with new ones.
	if _, err := tx.Exec(ctx, `DELETE FROM index_entries WHERE index_id = $1`, d.ID()); err != nil {
		return 0, fmt.Errorf("store: backfill clear: %w", err)
	}
	for _, e := range batch {
		if _, err := tx.Exec(ctx, `
			INSERT INTO index_entries (index_id, key, doc_name) VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING`, d.ID(), e.key, e.name); err != nil {
			return 0, fmt.Errorf("store: backfill insert %s: %w", e.name, err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("store: backfill commit: %w", err)
	}

	if err := s.MarkIndexReady(ctx, d); err != nil {
		return 0, err
	}
	return int64(len(batch)), nil
}

// ReadyIndexes returns the ids of indexes that have completed a backfill.
func (s *Store) ReadyIndexes(ctx context.Context) (map[int64]bool, error) {
	rows, err := s.Pool.Query(ctx,
		`SELECT index_id FROM index_defs WHERE ready_at IS NOT NULL`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[int64]bool{}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out[id] = true
	}
	return out, rows.Err()
}

// CountIndexEntries reports how many entries an index holds — the cheapest way
// to notice that a backfill never ran or a definition changed underneath one.
func (s *Store) CountIndexEntries(ctx context.Context, d index.Def) (int64, error) {
	var n int64
	err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM index_entries WHERE index_id = $1`, d.ID()).Scan(&n)
	return n, err
}

// LoadIndexes rebuilds the in-memory registry from index_defs.
//
// Postgres is the source of truth at runtime, not a config file on the server:
// the CLI registers and backfills definitions, and kord picks them up here. That
// keeps the activation of an index — which must not happen before its backfill
// completes — a property of the database rather than of which file a process
// happened to read at boot.
//
// A malformed spec is skipped rather than fatal. Refusing to start because one
// row cannot be parsed would take the whole store down for a problem that costs
// only a missed optimisation.
func (s *Store) LoadIndexes(ctx context.Context) (int, error) {
	rows, err := s.Pool.Query(ctx, `SELECT spec, ready_at IS NOT NULL FROM index_defs`)
	if err != nil {
		return 0, fmt.Errorf("store: load index defs: %w", err)
	}
	defer rows.Close()

	byColl := map[string][]index.Def{}
	ready := map[int64]bool{}
	bad := 0
	for rows.Next() {
		var spec string
		var isReady bool
		if err := rows.Scan(&spec, &isReady); err != nil {
			return 0, err
		}
		d, err := index.ParseSpec(spec)
		if err != nil {
			bad++
			continue
		}
		byColl[d.CollectionID] = append(byColl[d.CollectionID], d)
		if isReady {
			ready[d.ID()] = true
		}
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}

	s.indexes.mu.Lock()
	s.indexes.byColl = byColl
	s.indexes.ready = ready
	s.indexes.mu.Unlock()

	if bad > 0 {
		return len(ready), fmt.Errorf("store: %d index definition(s) could not be parsed and were ignored", bad)
	}
	return len(ready), nil
}

// DropIndex removes a definition and every entry it produced.
//
// Entries are deleted first: an index_defs row without entries is merely
// unusable, while entries without a definition are unreachable rows that keep
// paying write cost on every document change with nothing able to read them.
func (s *Store) DropIndex(ctx context.Context, d index.Def) (int64, error) {
	tag, err := s.Pool.Exec(ctx, `DELETE FROM index_entries WHERE index_id = $1`, d.ID())
	if err != nil {
		return 0, fmt.Errorf("store: drop index entries: %w", err)
	}
	if _, err := s.Pool.Exec(ctx, `DELETE FROM index_defs WHERE index_id = $1`, d.ID()); err != nil {
		return 0, fmt.Errorf("store: drop index def: %w", err)
	}
	return tag.RowsAffected(), nil
}

// IndexStatus is one row of `kor index list`.
type IndexStatus struct {
	Spec    string
	Ready   bool
	Entries int64
}

// ListIndexes reports every registered definition with its entry count.
func (s *Store) ListIndexes(ctx context.Context) ([]IndexStatus, error) {
	rows, err := s.Pool.Query(ctx, `
		SELECT d.spec, d.ready_at IS NOT NULL,
		       (SELECT count(*) FROM index_entries e WHERE e.index_id = d.index_id)
		FROM index_defs d ORDER BY d.collection_id, d.spec`)
	if err != nil {
		return nil, fmt.Errorf("store: list indexes: %w", err)
	}
	defer rows.Close()
	var out []IndexStatus
	for rows.Next() {
		var st IndexStatus
		if err := rows.Scan(&st.Spec, &st.Ready, &st.Entries); err != nil {
			return nil, err
		}
		out = append(out, st)
	}
	return out, rows.Err()
}

// CollectionsWithDocuments lists the collections Kor actually holds.
//
// `kor index apply` uses this to avoid registering the whole of a project's
// firestore.indexes.json against a store serving a handful of collections.
// Every registered index costs work on every write to its collection, so an
// index for a collection Kor does not serve is pure write amplification.
func (s *Store) CollectionsWithDocuments(ctx context.Context) (map[string]bool, error) {
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT collection_id FROM documents`)
	if err != nil {
		return nil, fmt.Errorf("store: list collections: %w", err)
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out[c] = true
	}
	return out, rows.Err()
}

// IndexedQueries counts queries served from index_entries rather than the
// general path. Exposed because "the index is registered" and "the index is
// being used" are different claims, and only the second one matters: a test or
// a dashboard that cannot tell them apart will report success while every query
// quietly falls back.
func (s *Store) IndexedQueries() int64 { return s.indexedQueries.Load() }

// IndexedMergedQueries counts index-served queries whose plan spanned more than
// one range, i.e. those that went through the k-way merge.
func (s *Store) IndexedMergedQueries() int64 { return s.indexedMerged.Load() }
