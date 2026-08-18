package store

import (
	"context"
	"fmt"
	"sync"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/jackc/pgx/v5"

	"github.com/omelas-tech/kor/internal/index"
)

// Index registry and entry maintenance.
//
// The registry is empty by default, which makes every hook below a no-op: a
// deployment defining no indexes behaves exactly as it did before this existed
// and pays nothing for it. Indexes are opt-in, per collection.

type indexRegistry struct {
	mu     sync.RWMutex
	byColl map[string][]index.Def
}

func (r *indexRegistry) forCollection(collectionID string) []index.Def {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byColl[collectionID]
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

	s.indexes.mu.Lock()
	s.indexes.byColl = byColl
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
	_, err := s.Pool.Exec(ctx,
		`UPDATE index_defs SET ready_at = now() WHERE index_id = $1`, d.ID())
	return err
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
