package e2e

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/omelas-tech/kor/internal/index"
	"github.com/omelas-tech/kor/internal/pgtest"
	"github.com/omelas-tech/kor/internal/rpc"
	"github.com/omelas-tech/kor/internal/store"
)

// G2: differential fuzz against Google's own Firestore emulator.
//
// Every other test in this repo asserts what I believe Firestore does. This one
// asks Firestore. The same corpus is written to both stores through the same
// unmodified SDK, then randomly generated queries run against both and the
// results are diffed — document order included, because ordering across mixed
// types is exactly where a reimplementation drifts and exactly what no
// hand-written test covers exhaustively.
//
// Run it with the emulator up:
//
//	java -jar ~/.cache/firebase/emulators/cloud-firestore-emulator-*.jar \
//	     --host=127.0.0.1 --port=8432
//	KOR_FUZZ_EMULATOR=127.0.0.1:8432 go test ./e2e/ -run Fuzz -count=1
//
// Both clients use the same project id so DocumentRef values — which sort by
// full path — are byte-identical between the two stores.
const fuzzProject = "demo-kor"

func emulatorAddr(t *testing.T) string {
	t.Helper()
	addr := os.Getenv("KOR_FUZZ_EMULATOR")
	if addr == "" {
		t.Skip("set KOR_FUZZ_EMULATOR=host:port with the Firestore emulator running")
	}
	return addr
}

func dial(t *testing.T, addr string) *firestore.Client {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	c, err := firestore.NewClient(context.Background(), fuzzProject, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("client %s: %v", addr, err)
	}
	t.Cleanup(func() { c.Close(); conn.Close() })
	return c
}

// startKordConn runs kord in-process and returns a client dialled explicitly,
// so both stores can be talked to from one test without FIRESTORE_EMULATOR_HOST
// pointing every client at the same place.
func startKordConn(t *testing.T) (*firestore.Client, *store.Store) {
	t.Helper()
	st, err := store.Open(context.Background(), pgtest.DSN(t))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(st.Close)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	rpc.New(st).Register(g)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return dial(t, lis.Addr().String()), st
}

// refPath marks a value that must be materialized as a DocumentRef belonging to
// whichever client is doing the writing.
type refPath string

func materialize(c *firestore.Client, v any) any {
	switch t := v.(type) {
	case refPath:
		return c.Doc(string(t))
	case []any:
		out := make([]any, len(t))
		for i, e := range t {
			out[i] = materialize(c, e)
		}
		return out
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, e := range t {
			out[k] = materialize(c, e)
		}
		return out
	default:
		return v
	}
}

// fuzzValues spans Firestore's type order: null < bool < number < timestamp <
// string < bytes < reference < geopoint < array < map. The awkward members of
// each are deliberate — NaN sorts below every number, -0.0 and 0.0 compare
// equal, and int64 past 2^53 is where a JSON round trip would quietly lose
// precision.
func fuzzValues(rnd *rand.Rand) []any {
	negZero := math.Copysign(0, -1)
	base := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	return []any{
		nil,
		false, true,
		math.NaN(),
		math.Inf(-1), math.Inf(1),
		int64(-1), int64(0), negZero, 0.0, 0.5, int64(1), int64(2),
		int64(9007199254740993), // 2^53+1
		base, base.Add(time.Duration(rnd.Intn(1000)) * time.Hour),
		"", "a", "b", "ab", "Z", "é", "日本",
		[]byte{}, []byte{0x00}, []byte{0x01, 0x02},
		refPath("things/a"), refPath("things/b"),
		&latlng.LatLng{Latitude: 1, Longitude: 2},
		&latlng.LatLng{Latitude: 1, Longitude: 3},
		[]any{}, []any{int64(1)}, []any{int64(1), "x"},
		map[string]any{}, map[string]any{"k": int64(1)},
	}
}

func writeCorpus(t *testing.T, c *firestore.Client, coll string, docs []map[string]any) {
	t.Helper()
	ctx := context.Background()
	for i, d := range docs {
		id := fmt.Sprintf("d%03d", i)
		if _, err := c.Collection(coll).Doc(id).Set(ctx, materialize(c, d)); err != nil {
			t.Fatalf("seed %s: %v", id, err)
		}
	}
}

func runIDs(ctx context.Context, q firestore.Query) ([]string, error) {
	it := q.Documents(ctx)
	defer it.Stop()
	var out []string
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, snap.Ref.ID)
	}
}

// spec describes one generated query, applied identically to both clients.
type spec struct {
	desc     string
	build    func(firestore.CollectionRef) firestore.Query
	orderLen int
}

func TestFuzzAgainstFirestoreEmulator(t *testing.T) {
	t.Run("general path", func(t *testing.T) { runFuzz(t, false) })
	// The same corpus and the same queries, but with composite indexes
	// backfilled so the planner serves what it can from index_entries. Without
	// this, the fuzz only ever exercises runGeneral, and the index path's only
	// evidence is that it agrees with runGeneral — which proves consistency,
	// not correctness. Here both are measured against Firestore itself.
	t.Run("index path", func(t *testing.T) { runFuzz(t, true) })
}

func runFuzz(t *testing.T, withIndexes bool) {
	emuAddr := emulatorAddr(t)
	ctx := context.Background()

	kor, st := startKordConn(t)
	emu := dial(t, emuAddr)

	// A fresh collection per run: the emulator persists for its process
	// lifetime, and a leftover corpus would silently change the comparison.
	coll := fmt.Sprintf("fuzz_%d", time.Now().UnixNano())

	seed := int64(20260818)
	if v := os.Getenv("KOR_FUZZ_SEED"); v != "" {
		fmt.Sscan(v, &seed)
	}
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("fuzz seed %d (override with KOR_FUZZ_SEED)", seed)
	vals := fuzzValues(rnd)
	fields := []string{"a", "b", "c"}

	const nDocs = 120
	docs := make([]map[string]any, nDocs)
	for i := range docs {
		d := map[string]any{}
		nested := map[string]any{}
		for _, f := range fields {
			// Some documents omit a field entirely: Firestore excludes those
			// from any query ordering by it, and that exclusion is easy to get
			// wrong in a reimplementation.
			if rnd.Intn(8) == 0 {
				continue
			}
			d[f] = vals[rnd.Intn(len(vals))]
			if rnd.Intn(3) != 0 {
				nested[f] = vals[rnd.Intn(len(vals))]
			}
		}
		if len(nested) > 0 {
			d["nested"] = nested
		}
		docs[i] = d
	}
	if withIndexes {
		// Register BEFORE writing, so entries are maintained on write, then
		// backfill anyway — that is the order an operator would use and it
		// exercises both maintenance paths.
		var defs []index.Def
		for _, f := range fields {
			defs = append(defs,
				index.Def{CollectionID: coll, Fields: []index.Field{{Path: f}}},
				index.Def{CollectionID: coll, Fields: []index.Field{{Path: f, Desc: true}}},
				index.Def{CollectionID: coll, Fields: []index.Field{{Path: "nested." + f}}},
			)
			for _, g := range fields {
				if f != g {
					defs = append(defs,
						index.Def{CollectionID: coll, Fields: []index.Field{{Path: f}, {Path: g}}},
						index.Def{CollectionID: coll, Fields: []index.Field{{Path: f}, {Path: g, Desc: true}}},
					)
				}
			}
		}
		if err := st.SetIndexes(ctx, defs); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			for _, d := range defs {
				_, _ = st.DropIndex(context.Background(), d)
			}
		})
		writeCorpus(t, kor, coll, docs)
		for _, d := range defs {
			if _, err := st.BackfillIndex(ctx, d); err != nil {
				t.Fatal(err)
			}
		}
	} else {
		writeCorpus(t, kor, coll, docs)
	}
	writeCorpus(t, emu, coll, docs)

	ops := []string{"==", ">", ">=", "<", "<="}
	specs := generateSpecs(rnd, fields, vals, ops, 1200)

	before := st.IndexedQueries()
	var mismatches, korOnlyErr, emuOnlyErr int
	for i, sp := range specs {
		korIDs, korErr := runIDs(ctx, sp.build(*kor.Collection(coll)))
		emuIDs, emuErr := runIDs(ctx, sp.build(*emu.Collection(coll)))

		switch {
		case korErr != nil && emuErr != nil:
			// Both refuse: agreement on rejection is agreement.
		case korErr != nil:
			korOnlyErr++
			if korOnlyErr <= 5 {
				t.Errorf("case %d: Firestore served it, Kor refused\n  %s\n  kor err: %v", i, sp.desc, korErr)
			}
		case emuErr != nil:
			// Kor is more permissive. Not a wrong answer, but a divergence
			// worth seeing, so it is reported rather than silently passed.
			emuOnlyErr++
			if emuOnlyErr <= 5 {
				t.Logf("case %d: Kor served a query Firestore rejected\n  %s\n  emu err: %v", i, sp.desc, emuErr)
			}
		default:
			if strings.Join(korIDs, ",") != strings.Join(emuIDs, ",") {
				mismatches++
				if mismatches <= 8 {
					t.Errorf("case %d: results differ\n  %s\n  kor: %v\n  emu: %v", i, sp.desc, korIDs, emuIDs)
				}
			}
		}
	}
	served := st.IndexedQueries() - before
	if withIndexes && served == 0 {
		t.Fatal("no query was served from an index, so this run duplicates the general-path run " +
			"and proves nothing about the index path")
	}
	if !withIndexes && served != 0 {
		t.Fatalf("%d queries hit an index with none registered", served)
	}
	t.Logf("index-served queries: %d", served)
	t.Logf("fuzz: %d queries, %d docs, indexes=%v — mismatches=%d korRefused=%d emuRefused=%d",
		len(specs), nDocs, withIndexes, mismatches, korOnlyErr, emuOnlyErr)
	if mismatches > 8 || korOnlyErr > 5 {
		t.Errorf("additional divergences suppressed: mismatches=%d korRefused=%d", mismatches, korOnlyErr)
	}
}

// generateSpecs produces a spread of query SHAPES, not just a spread of values.
// The first version of this fuzz varied only the operand, so 400 cases explored
// one shape 400 times; the negative-zero bug it did find was a value bug, and a
// shape bug would have slipped through.
func generateSpecs(rnd *rand.Rand, fields []string, vals []any, ops []string, n int) []spec {
	var specs []spec
	pick := func(xs []string) string { return xs[rnd.Intn(len(xs))] }
	val := func() any { return vals[rnd.Intn(len(vals))] }
	dir := func() firestore.Direction {
		if rnd.Intn(2) == 0 {
			return firestore.Asc
		}
		return firestore.Desc
	}

	for i := 0; i < n; i++ {
		shape := rnd.Intn(8)
		d1, d2 := dir(), dir()
		f1, f2 := pick(fields), pick(fields)
		op, v := pick(ops), val()
		limit, offset := 0, 0
		if rnd.Intn(3) == 0 {
			limit = 1 + rnd.Intn(5)
		}
		if rnd.Intn(5) == 0 {
			offset = rnd.Intn(4)
		}
		cur, cur2 := val(), val()
		useCursor, curKind := rnd.Intn(2) == 0, rnd.Intn(4)

		applyCursor := func(q firestore.Query, args ...any) firestore.Query {
			if !useCursor {
				return q
			}
			switch curKind {
			case 0:
				return q.StartAt(args...)
			case 1:
				return q.StartAfter(args...)
			case 2:
				return q.EndAt(args...)
			default:
				return q.EndBefore(args...)
			}
		}
		tail := func(q firestore.Query) firestore.Query {
			if offset > 0 {
				q = q.Offset(offset)
			}
			if limit > 0 {
				q = q.Limit(limit)
			}
			return q
		}

		var desc string
		var build func(firestore.CollectionRef) firestore.Query
		switch shape {
		case 0: // inequality, ordered by the filtered field (Firestore's rule)
			desc = fmt.Sprintf("where %s %s %v | orderBy %s | lim %d off %d cur %v/%d/%v", f1, op, v, f1, limit, offset, cur, curKind, useCursor)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(applyCursor(c.Where(f1, op, v).OrderBy(f1, d1), cur))
			}
		case 1: // ordering only, no filter — pure comparator exercise
			desc = fmt.Sprintf("orderBy %s | lim %d off %d cur %v/%d/%v", f1, limit, offset, cur, curKind, useCursor)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(applyCursor(c.OrderBy(f1, d1), cur))
			}
		case 2: // two-field ordering, where ties in the first are broken by the second
			desc = fmt.Sprintf("orderBy %s,%s | lim %d off %d cur %v,%v/%d/%v", f1, f2, limit, offset, cur, cur2, curKind, useCursor)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(applyCursor(c.OrderBy(f1, d1).OrderBy(f2, d2), cur, cur2))
			}
		case 3: // equality plus an ordering on another field
			desc = fmt.Sprintf("where %s == %v | orderBy %s | lim %d off %d", f1, v, f2, limit, offset)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(c.Where(f1, "==", v).OrderBy(f2, d2))
			}
		case 4: // ordering by document name, the implicit terminator made explicit
			desc = fmt.Sprintf("where %s == %v | orderBy __name__ | lim %d off %d", f1, v, limit, offset)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(c.Where(f1, "==", v).OrderBy(firestore.DocumentID, d1))
			}
		case 5: // not-in and array-contains-any: null handling differs per operator
			a, b := val(), val()
			if rnd.Intn(2) == 0 {
				desc = fmt.Sprintf("where %s not-in [%v %v] | orderBy %s", f1, a, b, f1)
				build = func(c firestore.CollectionRef) firestore.Query {
					return tail(c.Where(f1, "not-in", []any{a, b}).OrderBy(f1, d1))
				}
			} else {
				desc = fmt.Sprintf("where %s array-contains-any [%v %v] | orderBy __name__", f1, a, b)
				build = func(c firestore.CollectionRef) firestore.Query {
					return tail(c.Where(f1, "array-contains-any", []any{a, b}).OrderBy(firestore.DocumentID, d1))
				}
			}
		case 6: // nested field path: a different parse and a different jsonb probe
			desc = fmt.Sprintf("where nested.%s == %v | orderBy nested.%s", f1, v, f1)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(c.Where("nested."+f1, "==", v).OrderBy("nested."+f1, d1))
			}
		case 7: // != , which excludes null and NaN on top of the comparison
			desc = fmt.Sprintf("where %s != %v | orderBy %s | lim %d", f1, v, f1, limit)
			build = func(c firestore.CollectionRef) firestore.Query {
				return tail(c.Where(f1, "!=", v).OrderBy(f1, d1))
			}
		default: // array-contains and in, which take different evaluation paths
			if rnd.Intn(2) == 0 {
				desc = fmt.Sprintf("where %s array-contains %v | orderBy __name__", f1, v)
				build = func(c firestore.CollectionRef) firestore.Query {
					return tail(c.Where(f1, "array-contains", v).OrderBy(firestore.DocumentID, d1))
				}
			} else {
				a, b := val(), val()
				desc = fmt.Sprintf("where %s in [%v %v] | orderBy __name__", f1, a, b)
				build = func(c firestore.CollectionRef) firestore.Query {
					return tail(c.Where(f1, "in", []any{a, b}).OrderBy(firestore.DocumentID, d1))
				}
			}
		}
		specs = append(specs, spec{desc: desc, build: build})
	}
	return specs
}
