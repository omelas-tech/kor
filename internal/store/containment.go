package store

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// The containment index (documents_gin) approximates Firestore's automatic
// single-field indexes so equality filters without a composite index can be
// narrowed in Postgres rather than scanned in Go.
//
// It is also, measured, ~90% of write-ahead volume for large nested documents:
// jsonb_path_ops stores one entry per distinct path/value pair, so a document
// with hundreds of nested leaves dirties hundreds of index pages, and each of
// those takes a full-page image after every checkpoint. Because the index is
// global to the documents table, a collection that is only ever read by
// document id pays all of that and gets nothing back.
//
// Excluding such a collection makes the index partial. Nothing about query
// RESULTS changes — an excluded collection's equality filters still evaluate
// correctly, they just scan instead of probing an index.

const containmentIndex = "documents_gin"

// ContainmentExclusions lists the collections currently exempt.
func (s *Store) ContainmentExclusions(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT collection_id FROM containment_exclusions ORDER BY collection_id`)
	if err != nil {
		return nil, fmt.Errorf("store: read containment exclusions: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// SetContainmentExclusions replaces the exemption set and rebuilds the index to
// match.
//
// The rebuild is CONCURRENTLY and builds under a temporary name before swapping,
// because a plain CREATE INDEX takes a lock that blocks every write to
// documents for as long as it runs — minutes on a multi-gigabyte GIN index, on
// a table serving live traffic. The cost of that caution is that this cannot
// run inside a transaction, so the swap is ordered to leave a usable index at
// every point: build the new one, drop the old, rename into place.
func (s *Store) SetContainmentExclusions(ctx context.Context, colls []string, log func(string)) error {
	if log == nil {
		log = func(string) {}
	}
	norm := normalizeCollections(colls)

	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM containment_exclusions`); err != nil {
		_ = tx.Rollback(ctx)
		return err
	}
	for _, c := range norm {
		if _, err := tx.Exec(ctx,
			`INSERT INTO containment_exclusions (collection_id) VALUES ($1)`, c); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return s.rebuildContainmentIndex(ctx, norm, log)
}

func (s *Store) rebuildContainmentIndex(ctx context.Context, exclude []string, log func(string)) error {
	tmp := containmentIndex + "_rebuild"
	// A previous run may have been interrupted, leaving an invalid index behind.
	// Postgres will not create over it, and an invalid index is never used, so
	// clearing it is always safe.
	if _, err := s.Pool.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+tmp); err != nil {
		return fmt.Errorf("store: clear stale rebuild index: %w", err)
	}

	create := fmt.Sprintf(
		`CREATE INDEX CONCURRENTLY %s ON documents USING gin (data jsonb_path_ops)`, tmp)
	if len(exclude) > 0 {
		create += " WHERE collection_id NOT IN (" + quoteList(exclude) + ")"
	}
	log("building index (this reads the whole table; writes keep working)")
	if _, err := s.Pool.Exec(ctx, create); err != nil {
		return fmt.Errorf("store: build containment index: %w", err)
	}
	if _, err := s.Pool.Exec(ctx, `DROP INDEX CONCURRENTLY IF EXISTS `+containmentIndex); err != nil {
		return fmt.Errorf("store: drop old containment index: %w", err)
	}
	if _, err := s.Pool.Exec(ctx,
		fmt.Sprintf(`ALTER INDEX %s RENAME TO %s`, tmp, containmentIndex)); err != nil {
		return fmt.Errorf("store: rename containment index: %w", err)
	}
	return nil
}

// ContainmentIndexPredicate returns the index's current WHERE clause, or "" if
// it covers every collection. Used to confirm the physical index matches the
// configuration — they can only diverge if a rebuild failed halfway.
func (s *Store) ContainmentIndexPredicate(ctx context.Context) (string, error) {
	var def string
	err := s.Pool.QueryRow(ctx,
		`SELECT indexdef FROM pg_indexes WHERE indexname = $1`, containmentIndex).Scan(&def)
	if err != nil {
		return "", fmt.Errorf("store: read containment index: %w", err)
	}
	if i := strings.Index(def, " WHERE "); i >= 0 {
		return strings.TrimSpace(def[i+7:]), nil
	}
	return "", nil
}

func normalizeCollections(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, c := range in {
		c = strings.TrimSpace(c)
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

// quoteList renders SQL string literals. The values are collection ids, which
// cannot be parameterised here: a partial index predicate must be a constant
// expression, so there is no bind-parameter form available.
func quoteList(vals []string) string {
	parts := make([]string, len(vals))
	for i, v := range vals {
		parts[i] = "'" + strings.ReplaceAll(v, "'", "''") + "'"
	}
	return strings.Join(parts, ", ")
}
