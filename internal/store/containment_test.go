package store

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestContainmentExclusionsRebuildThePartialIndex(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	pred, err := s.ContainmentIndexPredicate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if pred != "" {
		t.Fatalf("a fresh store must index every collection, got predicate %q", pred)
	}

	if err := s.SetContainmentExclusions(ctx, []string{"tmdb_movies_v3", " tmdb_persons_v3 ", "tmdb_movies_v3"}, nil); err != nil {
		t.Fatal(err)
	}
	got, err := s.ContainmentExclusions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Duplicates collapse and whitespace is trimmed: the set is configuration,
	// and a duplicate would otherwise appear twice in the index predicate.
	if len(got) != 2 || got[0] != "tmdb_movies_v3" || got[1] != "tmdb_persons_v3" {
		t.Fatalf("exclusions = %v", got)
	}
	pred, err = s.ContainmentIndexPredicate(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"tmdb_movies_v3", "tmdb_persons_v3"} {
		if !strings.Contains(pred, want) {
			t.Errorf("predicate %q missing %q", pred, want)
		}
	}

	// Clearing must restore full coverage, or a deployment could never undo an
	// exemption it regretted.
	if err := s.SetContainmentExclusions(ctx, nil, nil); err != nil {
		t.Fatal(err)
	}
	if pred, _ := s.ContainmentIndexPredicate(ctx); pred != "" {
		t.Errorf("after clearing, predicate = %q, want none", pred)
	}
}

// The point of the exemption is write cost, so measure write cost. Anything
// else — the predicate is present, the index is smaller — is a proxy for the
// thing that actually mattered.
func TestExemptCollectionCostsFarLessToWrite(t *testing.T) {
	ctx := context.Background()

	// Documents shaped like a TMDB payload: many nested leaves, which is what
	// makes a jsonb_path_ops entry per path/value pair expensive.
	doc := func(i int) map[string]*pb.Value {
		seasons := make([]*pb.Value, 0, 10)
		for sn := 0; sn < 10; sn++ {
			eps := make([]*pb.Value, 0, 12)
			for e := 0; e < 12; e++ {
				eps = append(eps, &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: map[string]*pb.Value{
					"name":     sval(fmt.Sprintf("ep %d-%d-%d", i, sn, e)),
					"overview": sval(fmt.Sprintf("text %d %d %d", i, sn, e)),
					"runtime":  ival(int64(20 + e)),
				}}}})
			}
			seasons = append(seasons, &pb.Value{ValueType: &pb.Value_ArrayValue{
				ArrayValue: &pb.ArrayValue{Values: eps}}})
		}
		return map[string]*pb.Value{
			"name":    sval(fmt.Sprintf("Series %d", i)),
			"seasons": {ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: seasons}}},
		}
	}

	walPerDoc := func(t *testing.T, exempt bool) float64 {
		t.Helper()
		s := openStore(t)
		if exempt {
			if err := s.SetContainmentExclusions(ctx, []string{"mirror"}, nil); err != nil {
				t.Fatal(err)
			}
		}
		const n = 120
		var before, after string
		if err := s.Pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&before); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			setDoc(t, s, fmt.Sprintf("%s/mirror/m%04d", idxParent, i), doc(i))
		}
		if err := s.Pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text`).Scan(&after); err != nil {
			t.Fatal(err)
		}
		var b int64
		if err := s.Pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1,$2)::bigint`, after, before).Scan(&b); err != nil {
			t.Fatal(err)
		}
		return float64(b) / float64(n) / 1024
	}

	indexed := walPerDoc(t, false)
	exempt := walPerDoc(t, true)
	t.Logf("WAL per document: indexed %.1f kB, exempt %.1f kB (%.0f%% less)",
		indexed, exempt, (indexed-exempt)/indexed*100)

	// The measured production ratio was ~10x. Assert something well inside it
	// so the test reports a real regression rather than hardware noise.
	if exempt >= indexed/2 {
		t.Errorf("exempting a collection should roughly halve its write cost at worst: indexed %.1f kB, exempt %.1f kB",
			indexed, exempt)
	}
}
