package index

import (
	"bytes"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/omelas-tech/kor/internal/query"
)

const parent = "projects/p/databases/(default)/documents"

func from(coll string) []*pb.StructuredQuery_CollectionSelector {
	return []*pb.StructuredQuery_CollectionSelector{{CollectionId: coll}}
}

func eqf(path string, v *pb.Value) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
		FieldFilter: &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: path},
			Op:    pb.StructuredQuery_FieldFilter_EQUAL, Value: v,
		}}}
}

func gtf(path string, v *pb.Value) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
		FieldFilter: &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: path},
			Op:    pb.StructuredQuery_FieldFilter_GREATER_THAN, Value: v,
		}}}
}

func cmpf(path string, op pb.StructuredQuery_FieldFilter_Operator, v *pb.Value) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
		FieldFilter: &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: path}, Op: op, Value: v,
		}}}
}

func andf(subs ...*pb.StructuredQuery_Filter) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_CompositeFilter{
		CompositeFilter: &pb.StructuredQuery_CompositeFilter{
			Op: pb.StructuredQuery_CompositeFilter_AND, Filters: subs,
		}}}
}

func strv(s string) *pb.Value { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}} }
func intv(n int64) *pb.Value  { return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: n}} }

func ord(path string, dir pb.StructuredQuery_Direction) *pb.StructuredQuery_Order {
	return &pb.StructuredQuery_Order{
		Field: &pb.StructuredQuery_FieldReference{FieldPath: path}, Direction: dir}
}

func parse(t *testing.T, sq *pb.StructuredQuery) *query.Query {
	t.Helper()
	q, err := query.Parse(parent, sq)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return q
}

var (
	authorScore = Def{CollectionID: "posts", Fields: []Field{{Path: "author"}, {Path: "score"}}}
	allReady    = map[int64]bool{authorScore.ID(): true}
)

func TestPlannerSelectsAMatchingIndex(t *testing.T) {
	// Guards the differential test from being vacuous: if the planner declined
	// these shapes, both sides would run the general path and the comparison
	// would prove nothing.
	q := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", &pb.Value{ValueType: &pb.Value_StringValue{StringValue: "ann"}}),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
		Limit:   wrapperspb.Int32(5),
	})
	plan, ok := For(q, []Def{authorScore}, allReady)
	if !ok {
		t.Fatal("planner must serve equality + matching orderBy")
	}
	if plan.Reversed {
		t.Error("ascending query against an ascending index should not reverse the scan")
	}
	if len(plan.Ranges[0].Lo) == 0 {
		t.Error("prefix must bound the scan to the equality group")
	}
}

func TestPlannerReversesForOppositeDirection(t *testing.T) {
	q := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", &pb.Value{ValueType: &pb.Value_StringValue{StringValue: "ann"}}),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_DESCENDING)},
	})
	plan, ok := For(q, []Def{authorScore}, allReady)
	if !ok {
		t.Fatal("a descending query is servable by scanning an ascending index backwards")
	}
	if !plan.Reversed {
		t.Error("descending query against an ascending index must reverse the scan")
	}
}

func TestPlannerDeclinesWhatItCannotServeExactly(t *testing.T) {
	str := func(s string) *pb.Value { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}} }

	cases := map[string]*pb.StructuredQuery{
		// Needs a range bound, not a prefix.
		"inequality filter": {
			From: from("posts"), Where: gtf("score", str("1")),
			OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)}},
		// Equality on a field the index does not lead with: the prefix would
		// not isolate the matching group.
		"equality outside the index prefix": {
			From: from("posts"), Where: eqf("tag", str("x")),
			OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)}},
		// Ordering term the index does not carry.
		"orderBy an unindexed field": {
			From: from("posts"), Where: eqf("author", str("ann")),
			OrderBy: []*pb.StructuredQuery_Order{ord("views", pb.StructuredQuery_ASCENDING)}},
		// Different collection entirely.
		"wrong collection": {
			From: from("comments"), Where: eqf("author", str("ann")),
			OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)}},
	}
	for name, sq := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := For(parse(t, sq), []Def{authorScore}, allReady); ok {
				t.Error("planner must decline — falling back to the general path is " +
					"a lost optimisation, serving it wrongly is a silent bug")
			}
		})
	}
}

func TestPlannerRefusesAnIndexThatIsNotBackfilled(t *testing.T) {
	q := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", &pb.Value{ValueType: &pb.Value_StringValue{StringValue: "ann"}}),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
	})
	if _, ok := For(q, []Def{authorScore}, map[int64]bool{}); ok {
		t.Error("an index with no completed backfill holds entries only for documents " +
			"written since it was registered, so serving reads from it returns fewer " +
			"documents than the collection holds")
	}
}

func TestCursorNarrowsTheScanRange(t *testing.T) {
	base := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", strv("ann")),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
	})
	withCursor := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", strv("ann")),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
		StartAt: &pb.Cursor{Values: []*pb.Value{intv(3)}, Before: true},
	})
	ready := map[int64]bool{authorScore.ID(): true}

	b, ok := For(base, []Def{authorScore}, ready)
	if !ok {
		t.Fatal("the uncursored query should plan")
	}
	c, ok := For(withCursor, []Def{authorScore}, ready)
	if !ok {
		t.Fatal("a cursor on the ordering field is a key bound, so it should plan")
	}
	if bytes.Compare(c.Ranges[0].Lo, b.Ranges[0].Lo) <= 0 {
		t.Errorf("a start cursor must raise the lower bound: base=%x cursor=%x", b.Ranges[0].Lo, c.Ranges[0].Lo)
	}
	if !bytes.Equal(c.Ranges[0].Hi, b.Ranges[0].Hi) {
		t.Errorf("a start cursor must not move the upper bound: base=%x cursor=%x", b.Ranges[0].Hi, c.Ranges[0].Hi)
	}
}

// A start cursor names a position in QUERY order. When the scan runs against
// the index it bounds the HIGH end of the byte range, not the low one — the
// case most likely to be written backwards, because it reads as "start".
func TestStartCursorBoundsTheHighEndWhenReversed(t *testing.T) {
	q := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", strv("ann")),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_DESCENDING)},
		StartAt: &pb.Cursor{Values: []*pb.Value{intv(3)}, Before: true},
	})
	base := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", strv("ann")),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_DESCENDING)},
	})
	ready := map[int64]bool{authorScore.ID(): true}

	b, _ := For(base, []Def{authorScore}, ready)
	p, ok := For(q, []Def{authorScore}, ready)
	if !ok {
		t.Fatal("expected a reversed plan")
	}
	if !p.Reversed {
		t.Fatal("an ascending index serving a descending query must scan reversed")
	}
	if bytes.Compare(p.Ranges[0].Hi, b.Ranges[0].Hi) >= 0 {
		t.Errorf("a start cursor on a reversed scan must lower the upper bound: base=%x got=%x", b.Ranges[0].Hi, p.Ranges[0].Hi)
	}
	if !bytes.Equal(p.Ranges[0].Lo, b.Ranges[0].Lo) {
		t.Errorf("it must not move the lower bound: base=%x got=%x", b.Ranges[0].Lo, p.Ranges[0].Lo)
	}
}

// An inequality constrains values, so which end it bounds follows the INDEX
// FIELD's direction — the opposite axis from the cursor case above.
func TestInequalityBoundsFollowFieldDirectionNotScanDirection(t *testing.T) {
	mk := func(dir pb.StructuredQuery_Direction) *query.Query {
		return parse(t, &pb.StructuredQuery{
			From: from("posts"),
			Where: andf(
				eqf("author", strv("ann")),
				cmpf("score", pb.StructuredQuery_FieldFilter_GREATER_THAN, intv(3)),
			),
			OrderBy: []*pb.StructuredQuery_Order{ord("score", dir)},
		})
	}
	ready := map[int64]bool{authorScore.ID(): true}

	asc, ok := For(mk(pb.StructuredQuery_ASCENDING), []Def{authorScore}, ready)
	if !ok {
		t.Fatal("an inequality on the first ordering field should plan")
	}
	desc, ok := For(mk(pb.StructuredQuery_DESCENDING), []Def{authorScore}, ready)
	if !ok {
		t.Fatal("the same filter with reversed ordering should also plan")
	}
	// Same index field (ascending), so "score > 3" is the same byte range in
	// both; only the walk direction differs.
	if !bytes.Equal(asc.Ranges[0].Lo, desc.Ranges[0].Lo) || !bytes.Equal(asc.Ranges[0].Hi, desc.Ranges[0].Hi) {
		t.Errorf("scan direction must not change which bytes qualify:\n asc  [%x,%x)\n desc [%x,%x)",
			asc.Ranges[0].Lo, asc.Ranges[0].Hi, desc.Ranges[0].Lo, desc.Ranges[0].Hi)
	}
	if !desc.Reversed {
		t.Error("the descending query should still scan reversed")
	}
}

func TestPlannerStillDeclinesUnservableShapes(t *testing.T) {
	ready := map[int64]bool{authorScore.ID(): true}
	cases := map[string]*pb.StructuredQuery{
		"two inequality fields": {
			From: from("posts"),
			Where: andf(
				cmpf("score", pb.StructuredQuery_FieldFilter_GREATER_THAN, intv(3)),
				cmpf("views", pb.StructuredQuery_FieldFilter_LESS_THAN, intv(9)),
			),
			OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
		},
		"inequality off the first ordering field": {
			From:    from("posts"),
			Where:   cmpf("views", pb.StructuredQuery_FieldFilter_GREATER_THAN, intv(3)),
			OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
		},
	}
	for name, sq := range cases {
		t.Run(name, func(t *testing.T) {
			q := parse(t, sq)
			if _, ok := For(q, []Def{authorScore}, ready); ok {
				t.Error("planner served a shape whose results would not be a contiguous key range")
			}
		})
	}
}
