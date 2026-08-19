package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// Is a partial GIN index actually usable by Kor's queries?
//
// A partial index only helps if the planner can PROVE, from the query's own
// predicate, that every row it wants is inside the index's WHERE clause. Kor
// passes the collection as a bind parameter (collection_id = $2), and proof
// happens at planning time — so with a generic plan the value is unknown and
// the proof cannot be made. That would make the index silently unused, which is
// the worst outcome: all of the write savings, none of the reads.
//
//	KOR_WAL_COST=1 go test ./internal/store/ -run TestPartialGin -v
func TestPartialGinUsability(t *testing.T) {
	if os.Getenv("KOR_WAL_COST") == "" {
		t.Skip("set KOR_WAL_COST=1 (diagnostic, not an assertion)")
	}
	s := openStore(t)
	ctx := context.Background()

	for i := 0; i < 400; i++ {
		coll := "artworks"
		if i%2 == 0 {
			coll = "tmdb_movies_v3"
		}
		setDoc(t, s, fmt.Sprintf("%s/%s/d%04d", idxParent, coll, i),
			map[string]*pb.Value{"genre": sval(fmt.Sprintf("g%d", i%7))})
	}
	if _, err := s.Pool.Exec(ctx, `DROP INDEX documents_gin`); err != nil {
		t.Fatal(err)
	}
	// Positive list: proving `collection_id = 'artworks'` implies membership is
	// the easiest form for the planner. A NOT IN exclusion is harder still.
	// Exclusion is the natural config shape ("these mirrors do not need it"),
	// but it is the harder proof: the planner must show `collection_id = 'x'`
	// implies `x` is none of the excluded constants.
	if _, err := s.Pool.Exec(ctx, `
		CREATE INDEX documents_gin_partial ON documents USING gin (data jsonb_path_ops)
		WHERE collection_id NOT IN ('tmdb_movies_v3','tmdb_tv_series_v3','tmdb_persons_v3')`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `ANALYZE documents`); err != nil {
		t.Fatal(err)
	}

	probe := `{"genre": "g3"}`
	explain := func(label, sql string, args ...any) {
		rows, err := s.Pool.Query(ctx, "EXPLAIN (COSTS OFF) "+sql, args...)
		if err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		defer rows.Close()
		var plan []string
		for rows.Next() {
			var line string
			if err := rows.Scan(&line); err != nil {
				t.Fatal(err)
			}
			plan = append(plan, strings.TrimSpace(line))
		}
		used := strings.Contains(strings.Join(plan, " "), "documents_gin_partial")
		t.Logf("%-28s partial index used: %v   | %s", label, used, strings.Join(plan, " / "))
	}

	explain("literal collection",
		`SELECT name FROM documents WHERE parent_path = $1 AND collection_id = 'artworks' AND data @> $2`,
		idxParent, probe)
	explain("bind-param collection",
		`SELECT name FROM documents WHERE parent_path = $1 AND collection_id = $2 AND data @> $3`,
		idxParent, "artworks", probe)
	explain("excluded collection",
		`SELECT name FROM documents WHERE parent_path = $1 AND collection_id = 'tmdb_movies_v3' AND data @> $2`,
		idxParent, probe)
}
