package store

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// What does one document write actually cost in WAL?
//
// A bulk import of 38 kB documents produced ~1.5 MB of WAL per document — 50x
// the data — and filled a production disk. The suspect is documents_gin, a GIN
// index over the WHOLE document jsonb: it exists to approximate Firestore's
// automatic single-field indexes, but it is global to the documents table, so
// every collection pays its write cost including pure point-read mirrors that
// never issue an equality filter.
//
// This measures with and without it. Run manually:
//
//	KOR_WAL_COST=1 go test ./internal/store/ -run TestWALCost -v
func TestWALCostOfTheGinIndex(t *testing.T) {
	if os.Getenv("KOR_WAL_COST") == "" {
		t.Skip("set KOR_WAL_COST=1 (measurement, not an assertion)")
	}
	ctx := context.Background()

	measure := func(t *testing.T, dropGin bool) (perDoc float64, fpi int64) {
		s := openStore(t)
		if dropGin {
			if _, err := s.Pool.Exec(ctx, `DROP INDEX documents_gin`); err != nil {
				t.Fatal(err)
			}
		}
		// Documents shaped like a TMDB payload: deeply nested, many distinct
		// path/value pairs, which is what makes a jsonb_path_ops GIN entry per
		// pair expensive.
		rnd := rand.New(rand.NewSource(7))
		doc := func(i int) map[string]*pb.Value {
			seasons := make([]*pb.Value, 0, 12)
			for s := 0; s < 12; s++ {
				eps := make([]*pb.Value, 0, 14)
				for e := 0; e < 14; e++ {
					eps = append(eps, &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: map[string]*pb.Value{
						"name":     sval(fmt.Sprintf("Episode %d-%d-%d", i, s, e)),
						"overview": sval(fmt.Sprintf("overview text %d %d %d %d", i, s, e, rnd.Intn(1<<30))),
						"runtime":  ival(int64(20 + rnd.Intn(40))),
						"still":    sval(fmt.Sprintf("/still_%d_%d_%d.jpg", i, s, e)),
					}}}})
				}
				seasons = append(seasons, &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: map[string]*pb.Value{
					"season_number": ival(int64(s)),
					"episodes":      {ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: eps}}},
				}}}})
			}
			return map[string]*pb.Value{
				"name":     sval(fmt.Sprintf("Series %d", i)),
				"overview": sval(fmt.Sprintf("series overview %d %d", i, rnd.Intn(1<<30))),
				"seasons":  {ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: seasons}}},
			}
		}

		const n = 300
		var lsnBefore, lsnAfter string
		var fpiBefore, fpiAfter int64
		if err := s.Pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text, (SELECT wal_fpi FROM pg_stat_wal)`).Scan(&lsnBefore, &fpiBefore); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < n; i++ {
			setDoc(t, s, fmt.Sprintf("%s/series/s%04d", idxParent, i), doc(i))
		}
		if err := s.Pool.QueryRow(ctx, `SELECT pg_current_wal_lsn()::text, (SELECT wal_fpi FROM pg_stat_wal)`).Scan(&lsnAfter, &fpiAfter); err != nil {
			t.Fatal(err)
		}
		var bytes int64
		if err := s.Pool.QueryRow(ctx, `SELECT pg_wal_lsn_diff($1,$2)::bigint`, lsnAfter, lsnBefore).Scan(&bytes); err != nil {
			t.Fatal(err)
		}
		var docBytes int64
		if err := s.Pool.QueryRow(ctx, `SELECT sum(pg_column_size(data)) FROM documents`).Scan(&docBytes); err != nil {
			t.Fatal(err)
		}
		t.Logf("gin=%v: %d docs, %.1f kB/doc stored, WAL %.1f kB/doc (%.1fx), fpi +%d",
			!dropGin, n, float64(docBytes)/float64(n)/1024,
			float64(bytes)/float64(n)/1024,
			float64(bytes)/float64(docBytes), fpiAfter-fpiBefore)
		return float64(bytes) / float64(n) / 1024, fpiAfter - fpiBefore
	}

	withGin, _ := measure(t, false)
	withoutGin, _ := measure(t, true)
	t.Logf("RESULT: with GIN %.1f kB/doc, without %.1f kB/doc — GIN accounts for %.0f%% of WAL",
		withGin, withoutGin, (withGin-withoutGin)/withGin*100)
}
