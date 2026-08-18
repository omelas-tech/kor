package store

import (
	"context"
	"fmt"
	"math/rand"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/omelas-tech/kor/internal/index"
	"github.com/omelas-tech/kor/internal/query"
)

// The contract for composite indexes is not "fast" — it is "identical".
// runGeneral is the reference implementation, so every query an index serves
// must return exactly the documents runGeneral would, in exactly that order.
// These tests run the same query both ways and diff the result.

const idxParent = "projects/p/databases/(default)/documents"

func runNames(t *testing.T, s *Store, q *query.Query) []string {
	t.Helper()
	var out []string
	if err := s.RunQuery(context.Background(), q, func(d *Doc) error {
		out = append(out, d.Name)
		return nil
	}); err != nil {
		t.Fatalf("RunQuery: %v", err)
	}
	return out
}

// bothWays runs q with indexes active, then with the registry emptied so the
// general path serves it, and requires the two to agree.
func bothWays(t *testing.T, s *Store, defs []index.Def, q *query.Query) {
	t.Helper()

	// Assert the index is actually chosen. Without this the comparison is
	// vacuous: if the planner declines, BOTH sides run the general path and the
	// test passes while proving nothing. That is exactly what happened on the
	// first run of this suite — the planner matched no index because it ignored
	// the implicit __name__ terminator, and every case still went green.
	if _, ok := index.For(q, defs, s.indexes.readySet()); !ok {
		t.Fatal("planner declined this query, so the comparison would prove nothing")
	}

	withIndex := runNames(t, s, q)

	saved := s.indexes.byColl
	s.indexes.mu.Lock()
	s.indexes.byColl = nil
	s.indexes.mu.Unlock()
	general := runNames(t, s, q)
	s.indexes.mu.Lock()
	s.indexes.byColl = saved
	s.indexes.mu.Unlock()

	if len(withIndex) != len(general) {
		t.Fatalf("count differs: index %d, general %d\n index:   %v\n general: %v",
			len(withIndex), len(general), withIndex, general)
	}
	for i := range general {
		if withIndex[i] != general[i] {
			t.Fatalf("order differs at %d: index %s, general %s\n index:   %v\n general: %v",
				i, withIndex[i], general[i], withIndex, general)
		}
	}
}

func mkQuery(t *testing.T, sq *pb.StructuredQuery) *query.Query {
	t.Helper()
	q, err := query.Parse(idxParent, sq)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return q
}

func from(coll string) []*pb.StructuredQuery_CollectionSelector {
	return []*pb.StructuredQuery_CollectionSelector{{CollectionId: coll}}
}

func eqFilter(path string, v *pb.Value) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
		FieldFilter: &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: path},
			Op:    pb.StructuredQuery_FieldFilter_EQUAL,
			Value: v,
		}}}
}

func orderBy(path string, dir pb.StructuredQuery_Direction) *pb.StructuredQuery_Order {
	return &pb.StructuredQuery_Order{
		Field:     &pb.StructuredQuery_FieldReference{FieldPath: path},
		Direction: dir,
	}
}

// seedPosts writes a deterministic but unordered corpus, so any accidental
// reliance on insertion order shows up as a diff.
func seedPosts(t *testing.T, s *Store, n int) {
	t.Helper()
	authors := []string{"ann", "bob", "cat"}
	rnd := rand.New(rand.NewSource(11))
	for i := 0; i < n; i++ {
		fields := map[string]*pb.Value{
			"author": sval(authors[i%len(authors)]),
			"score":  ival(int64(rnd.Intn(20))),
		}
		// A few documents lack the ordering field entirely: Firestore excludes
		// them from the results, and both paths must agree on that.
		if i%13 == 0 {
			delete(fields, "score")
		}
		setDoc(t, s, fmt.Sprintf("%s/posts/p%03d", idxParent, i), fields)
	}
}

func TestIndexedResultsMatchGeneralPath(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "posts", Fields: []index.Field{
		{Path: "author"}, {Path: "score"},
	}}
	dDesc := index.Def{CollectionID: "posts", Fields: []index.Field{
		{Path: "author"}, {Path: "score", Desc: true},
	}}
	defs := []index.Def{d, dDesc}
	if err := s.SetIndexes(ctx, defs); err != nil {
		t.Fatal(err)
	}
	seedPosts(t, s, 60)
	for _, def := range defs {
		if _, err := s.BackfillIndex(ctx, def); err != nil {
			t.Fatal(err)
		}
	}

	cases := []struct {
		name string
		sq   *pb.StructuredQuery
	}{
		{"equality + ascending order", &pb.StructuredQuery{
			From: from("posts"), Where: eqFilter("author", sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
		{"equality + descending order", &pb.StructuredQuery{
			From: from("posts"), Where: eqFilter("author", sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_DESCENDING)},
		}},
		{"with limit", &pb.StructuredQuery{
			From: from("posts"), Where: eqFilter("author", sval("bob")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
			Limit:   wrapperspb.Int32(5),
		}},
		{"no matching documents", &pb.StructuredQuery{
			From: from("posts"), Where: eqFilter("author", sval("nobody")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { bothWays(t, s, defs, mkQuery(t, tc.sq)) })
	}
}
