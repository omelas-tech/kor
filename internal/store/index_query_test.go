package store

import (
	"context"
	"fmt"
	"math"
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

func cmpFilter(path string, op pb.StructuredQuery_FieldFilter_Operator, v *pb.Value) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
		FieldFilter: &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: path}, Op: op, Value: v,
		}}}
}

func andFilter(subs ...*pb.StructuredQuery_Filter) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_CompositeFilter{
		CompositeFilter: &pb.StructuredQuery_CompositeFilter{
			Op: pb.StructuredQuery_CompositeFilter_AND, Filters: subs,
		}}}
}

// Cursors are where an index most easily goes wrong, because a start cursor
// names a position in QUERY order: when the scan runs against the index it must
// bound the HIGH end of the key range, and the inclusive/exclusive sense flips
// with it. A mistake here does not error — it returns a page from the wrong end
// or repeats documents across page boundaries.
func TestIndexedCursorsMatchGeneralPath(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score"}}}
	dDesc := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score", Desc: true}}}
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

	q := func(dir pb.StructuredQuery_Direction, start, end *pb.Cursor, limit int32) *pb.StructuredQuery {
		sq := &pb.StructuredQuery{
			From: from("posts"), Where: eqFilter("author", sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", dir)},
			StartAt: start, EndAt: end,
		}
		if limit > 0 {
			sq.Limit = wrapperspb.Int32(limit)
		}
		return sq
	}
	at := func(n int64, before bool) *pb.Cursor {
		return &pb.Cursor{Values: []*pb.Value{ival(n)}, Before: before}
	}

	cases := []struct {
		name string
		sq   *pb.StructuredQuery
	}{
		// before=true on a start cursor is startAt (inclusive); false is startAfter.
		{"startAt asc", q(pb.StructuredQuery_ASCENDING, at(7, true), nil, 0)},
		{"startAfter asc", q(pb.StructuredQuery_ASCENDING, at(7, false), nil, 0)},
		// before=true on an end cursor is endBefore (exclusive); false is endAt.
		{"endBefore asc", q(pb.StructuredQuery_ASCENDING, nil, at(12, true), 0)},
		{"endAt asc", q(pb.StructuredQuery_ASCENDING, nil, at(12, false), 0)},
		{"bounded both ends asc", q(pb.StructuredQuery_ASCENDING, at(4, true), at(15, false), 0)},

		// The same cursors against a descending query, where the ascending index
		// is scanned in reverse and every bound swaps ends.
		{"startAt desc", q(pb.StructuredQuery_DESCENDING, at(7, true), nil, 0)},
		{"startAfter desc", q(pb.StructuredQuery_DESCENDING, at(7, false), nil, 0)},
		{"endBefore desc", q(pb.StructuredQuery_DESCENDING, nil, at(12, true), 0)},
		{"endAt desc", q(pb.StructuredQuery_DESCENDING, nil, at(12, false), 0)},
		{"bounded both ends desc", q(pb.StructuredQuery_DESCENDING, at(15, true), at(4, false), 0)},

		{"cursor with limit", q(pb.StructuredQuery_ASCENDING, at(5, true), nil, 4)},
		// A cursor past every value: the empty range must stay empty, not wrap.
		{"cursor past the end", q(pb.StructuredQuery_ASCENDING, at(999, true), nil, 0)},
		{"cursor before the start", q(pb.StructuredQuery_ASCENDING, nil, at(-999, true), 0)},
		// Ties matter: several documents share a score, so an inclusive bound
		// must take the whole group and an exclusive one must skip all of it.
		{"inverted range yields nothing", q(pb.StructuredQuery_ASCENDING, at(15, true), at(4, false), 0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { bothWays(t, s, defs, mkQuery(t, tc.sq)) })
	}
}

// An inequality bounds the range by VALUE, so which end it moves follows the
// index field's own direction — the opposite axis from cursors. Both are
// exercised together here because they narrow the same range and a sign error
// in either is invisible until the results are compared.
func TestIndexedInequalitiesMatchGeneralPath(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score"}}}
	dDesc := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score", Desc: true}}}
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

	ops := map[string]pb.StructuredQuery_FieldFilter_Operator{
		"gt":  pb.StructuredQuery_FieldFilter_GREATER_THAN,
		"gte": pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
		"lt":  pb.StructuredQuery_FieldFilter_LESS_THAN,
		"lte": pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
	}
	dirs := map[string]pb.StructuredQuery_Direction{
		"asc":  pb.StructuredQuery_ASCENDING,
		"desc": pb.StructuredQuery_DESCENDING,
	}
	for opName, op := range ops {
		for dirName, dir := range dirs {
			t.Run(opName+"/"+dirName, func(t *testing.T) {
				bothWays(t, s, defs, mkQuery(t, &pb.StructuredQuery{
					From: from("posts"),
					Where: andFilter(
						eqFilter("author", sval("ann")),
						cmpFilter("score", op, ival(9)),
					),
					OrderBy: []*pb.StructuredQuery_Order{orderBy("score", dir)},
				}))
			})
		}
	}

	// An inequality and a cursor narrowing the same range at once.
	t.Run("inequality plus cursor", func(t *testing.T) {
		bothWays(t, s, defs, mkQuery(t, &pb.StructuredQuery{
			From: from("posts"),
			Where: andFilter(
				eqFilter("author", sval("ann")),
				cmpFilter("score", pb.StructuredQuery_FieldFilter_GREATER_THAN, ival(3)),
			),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
			StartAt: &pb.Cursor{Values: []*pb.Value{ival(8)}, Before: true},
			Limit:   wrapperspb.Int32(3),
		}))
	})
}

// Hand-picked cases cover the shapes I thought of. This covers the ones I did
// not: random combinations of direction, operator, cursor sense, limit and
// offset, all diffed against the reference path.
func TestIndexedRandomizedAgainstGeneralPath(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score"}}}
	dDesc := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score", Desc: true}}}
	defs := []index.Def{d, dDesc}
	if err := s.SetIndexes(ctx, defs); err != nil {
		t.Fatal(err)
	}
	seedPosts(t, s, 120)
	for _, def := range defs {
		if _, err := s.BackfillIndex(ctx, def); err != nil {
			t.Fatal(err)
		}
	}

	ops := []pb.StructuredQuery_FieldFilter_Operator{
		pb.StructuredQuery_FieldFilter_GREATER_THAN,
		pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
		pb.StructuredQuery_FieldFilter_LESS_THAN,
		pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
	}
	authors := []string{"ann", "bob", "cat"}
	rnd := rand.New(rand.NewSource(2026))

	for i := 0; i < 300; i++ {
		sq := &pb.StructuredQuery{From: from("posts")}
		where := []*pb.StructuredQuery_Filter{
			eqFilter("author", sval(authors[rnd.Intn(len(authors))])),
		}
		if rnd.Intn(2) == 0 {
			where = append(where, cmpFilter("score", ops[rnd.Intn(len(ops))], ival(int64(rnd.Intn(24)-2))))
		}
		if len(where) == 1 {
			sq.Where = where[0]
		} else {
			sq.Where = andFilter(where...)
		}
		dir := pb.StructuredQuery_ASCENDING
		if rnd.Intn(2) == 0 {
			dir = pb.StructuredQuery_DESCENDING
		}
		sq.OrderBy = []*pb.StructuredQuery_Order{orderBy("score", dir)}
		if rnd.Intn(2) == 0 {
			sq.StartAt = &pb.Cursor{Values: []*pb.Value{ival(int64(rnd.Intn(24) - 2))}, Before: rnd.Intn(2) == 0}
		}
		if rnd.Intn(2) == 0 {
			sq.EndAt = &pb.Cursor{Values: []*pb.Value{ival(int64(rnd.Intn(24) - 2))}, Before: rnd.Intn(2) == 0}
		}
		if rnd.Intn(3) == 0 {
			sq.Limit = wrapperspb.Int32(int32(1 + rnd.Intn(6)))
		}
		if rnd.Intn(4) == 0 {
			sq.Offset = int32(rnd.Intn(4))
		}

		q := mkQuery(t, sq)
		if _, ok := index.For(q, defs, s.indexes.readySet()); !ok {
			continue // a shape the planner declines; the general path already serves it
		}
		t.Run(fmt.Sprintf("case%03d", i), func(t *testing.T) { bothWays(t, s, defs, q) })
	}
}

// A range comparison applies only within the operand's type: `score > 4`
// matches numbers, never the strings that sort after them in the total order.
// An index range is bytes, so it spans type boundaries unless clamped — and the
// seeded corpus above is all integers, which made this suite blind to it. The
// emulator fuzz caught it; this keeps it caught without an emulator.
func TestIndexedInequalityStaysInsideTheOperandType(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "mixed", Fields: []index.Field{{Path: "v"}}}
	dDesc := index.Def{CollectionID: "mixed", Fields: []index.Field{{Path: "v", Desc: true}}}
	defs := []index.Def{d, dDesc}
	if err := s.SetIndexes(ctx, defs); err != nil {
		t.Fatal(err)
	}

	// One document per type bucket, spanning the whole order.
	vals := []*pb.Value{
		{ValueType: &pb.Value_NullValue{}},
		{ValueType: &pb.Value_BooleanValue{BooleanValue: true}},
		{ValueType: &pb.Value_DoubleValue{DoubleValue: math.NaN()}},
		ival(-5), ival(0), ival(7), {ValueType: &pb.Value_DoubleValue{DoubleValue: 7.5}},
		sval(""), sval("m"), sval("zzz"),
		{ValueType: &pb.Value_BytesValue{BytesValue: []byte{0x01}}},
	}
	for i, v := range vals {
		setDoc(t, s, fmt.Sprintf("%s/mixed/m%02d", idxParent, i), map[string]*pb.Value{"v": v})
	}
	for _, def := range defs {
		if _, err := s.BackfillIndex(ctx, def); err != nil {
			t.Fatal(err)
		}
	}

	ops := []pb.StructuredQuery_FieldFilter_Operator{
		pb.StructuredQuery_FieldFilter_GREATER_THAN,
		pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
		pb.StructuredQuery_FieldFilter_LESS_THAN,
		pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
	}
	operands := map[string]*pb.Value{
		"int":    ival(0),
		"double": {ValueType: &pb.Value_DoubleValue{DoubleValue: 7.5}},
		"string": sval("m"),
		"bool":   {ValueType: &pb.Value_BooleanValue{BooleanValue: true}},
		"nan":    {ValueType: &pb.Value_DoubleValue{DoubleValue: math.NaN()}},
	}
	for _, dir := range []pb.StructuredQuery_Direction{
		pb.StructuredQuery_ASCENDING, pb.StructuredQuery_DESCENDING,
	} {
		for name, operand := range operands {
			for _, op := range ops {
				t.Run(fmt.Sprintf("%s/%v/%v", name, op, dir), func(t *testing.T) {
					bothWays(t, s, defs, mkQuery(t, &pb.StructuredQuery{
						From:    from("mixed"),
						Where:   cmpFilter("v", op, operand),
						OrderBy: []*pb.StructuredQuery_Order{orderBy("v", dir)},
					}))
				})
			}
		}
	}
}

// An `in` filter is served as one scan range per value, merged back into query
// order. The merge is the risky part: the ranges differ precisely in their
// equality prefix, so comparing whole keys would group results by the in-list
// value instead of interleaving them — which is wrong, and looks plausible
// because each group is internally well ordered.
func TestIndexedInFilterMatchesGeneralPath(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score"}}}
	dDesc := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score", Desc: true}}}
	defs := []index.Def{d, dDesc}
	if err := s.SetIndexes(ctx, defs); err != nil {
		t.Fatal(err)
	}
	seedPosts(t, s, 90)
	for _, def := range defs {
		if _, err := s.BackfillIndex(ctx, def); err != nil {
			t.Fatal(err)
		}
	}

	inFilter := func(path string, vals ...*pb.Value) *pb.StructuredQuery_Filter {
		return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
			FieldFilter: &pb.StructuredQuery_FieldFilter{
				Field: &pb.StructuredQuery_FieldReference{FieldPath: path},
				Op:    pb.StructuredQuery_FieldFilter_IN,
				Value: &pb.Value{ValueType: &pb.Value_ArrayValue{
					ArrayValue: &pb.ArrayValue{Values: vals}}},
			}}}
	}

	cases := []struct {
		name string
		sq   *pb.StructuredQuery
	}{
		{"two values ascending", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("bob")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
		{"two values descending", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("bob")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_DESCENDING)},
		}},
		{"three values", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("bob"), sval("cat")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
		// The page must be drawn after merging, not per range.
		{"with limit", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("bob"), sval("cat")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
			Limit:   wrapperspb.Int32(7),
		}},
		{"with limit and offset", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("bob"), sval("cat")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_DESCENDING)},
			Limit:   wrapperspb.Int32(5), Offset: 4,
		}},
		// A repeated value must not return its documents twice.
		{"duplicate values", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
		{"one matching value only", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("nobody")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
		{"no matching values", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("nobody"), sval("neither")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
		}},
		{"cursor across ranges", &pb.StructuredQuery{
			From: from("posts"), Where: inFilter("author", sval("ann"), sval("bob")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("score", pb.StructuredQuery_ASCENDING)},
			StartAt: &pb.Cursor{Values: []*pb.Value{ival(8)}, Before: true},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { bothWays(t, s, defs, mkQuery(t, tc.sq)) })
	}
}

// An array-contains definition fans each document out into one entry per
// distinct element, which turns `array-contains x` into an ordinary prefix
// lookup. The failure modes are specific: an array holding a value twice must
// not return the document twice, and a document whose field is not an array
// must not be indexed at all.
func TestIndexedArrayContainsMatchesGeneralPath(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "chats", Fields: []index.Field{
		{Path: "participants", Contains: true}, {Path: "updatedAt", Desc: true},
	}}
	defs := []index.Def{d}
	if err := s.SetIndexes(ctx, defs); err != nil {
		t.Fatal(err)
	}

	arr := func(vs ...*pb.Value) *pb.Value {
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vs}}}
	}
	docs := []map[string]*pb.Value{
		{"participants": arr(sval("ann"), sval("bob")), "updatedAt": ival(5)},
		{"participants": arr(sval("ann")), "updatedAt": ival(9)},
		{"participants": arr(sval("bob"), sval("cat")), "updatedAt": ival(1)},
		// The same value twice: one entry, one result.
		{"participants": arr(sval("ann"), sval("ann")), "updatedAt": ival(7)},
		{"participants": arr(), "updatedAt": ival(3)},                     // empty array: indexes nothing
		{"participants": sval("ann"), "updatedAt": ival(4)},               // not an array at all
		{"updatedAt": ival(2)},                                            // field missing
		{"participants": arr(sval("ann"), ival(3)), "updatedAt": ival(8)}, // mixed element types
	}
	for i, doc := range docs {
		setDoc(t, s, fmt.Sprintf("%s/chats/c%02d", idxParent, i), doc)
	}
	if _, err := s.BackfillIndex(ctx, d); err != nil {
		t.Fatal(err)
	}

	contains := func(v *pb.Value) *pb.StructuredQuery_Filter {
		return cmpFilter("participants", pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, v)
	}
	cases := []struct {
		name string
		sq   *pb.StructuredQuery
	}{
		{"string element desc", &pb.StructuredQuery{
			From: from("chats"), Where: contains(sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("updatedAt", pb.StructuredQuery_DESCENDING)},
		}},
		{"string element asc", &pb.StructuredQuery{
			From: from("chats"), Where: contains(sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("updatedAt", pb.StructuredQuery_ASCENDING)},
		}},
		{"element in several documents", &pb.StructuredQuery{
			From: from("chats"), Where: contains(sval("bob")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("updatedAt", pb.StructuredQuery_DESCENDING)},
		}},
		{"numeric element", &pb.StructuredQuery{
			From: from("chats"), Where: contains(ival(3)),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("updatedAt", pb.StructuredQuery_DESCENDING)},
		}},
		{"no such element", &pb.StructuredQuery{
			From: from("chats"), Where: contains(sval("nobody")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("updatedAt", pb.StructuredQuery_DESCENDING)},
		}},
		{"with limit", &pb.StructuredQuery{
			From: from("chats"), Where: contains(sval("ann")),
			OrderBy: []*pb.StructuredQuery_Order{orderBy("updatedAt", pb.StructuredQuery_DESCENDING)},
			Limit:   wrapperspb.Int32(2),
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { bothWays(t, s, defs, mkQuery(t, tc.sq)) })
	}
}

// An equality index and an array-contains index are different shapes — one
// entry per document versus one per element — so neither may serve the other's
// query. Confusing them returns documents whose field merely EQUALS the array,
// or misses every document whose array contains the value.
func TestContainsAndEqualityIndexesAreNotInterchangeable(t *testing.T) {
	eqDef := index.Def{CollectionID: "chats", Fields: []index.Field{{Path: "participants"}}}
	cDef := index.Def{CollectionID: "chats", Fields: []index.Field{{Path: "participants", Contains: true}}}
	ready := map[int64]bool{eqDef.ID(): true, cDef.ID(): true}

	eqQuery := mkQuery(t, &pb.StructuredQuery{
		From: from("chats"), Where: eqFilter("participants", sval("ann")),
	})
	containsQuery := mkQuery(t, &pb.StructuredQuery{
		From:  from("chats"),
		Where: cmpFilter("participants", pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, sval("ann")),
	})

	if _, ok := index.For(eqQuery, []index.Def{cDef}, ready); ok {
		t.Error("an array-contains index must not serve an == query")
	}
	if _, ok := index.For(containsQuery, []index.Def{eqDef}, ready); ok {
		t.Error("an equality index must not serve an array-contains query")
	}
	if _, ok := index.For(containsQuery, []index.Def{cDef}, ready); !ok {
		t.Error("the array-contains index should serve its own query")
	}
}
