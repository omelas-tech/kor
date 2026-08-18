package index

import (
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
	if len(plan.Prefix) == 0 {
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

func TestCursorsFallBack(t *testing.T) {
	q := parse(t, &pb.StructuredQuery{
		From: from("posts"), Where: eqf("author", &pb.Value{ValueType: &pb.Value_StringValue{StringValue: "ann"}}),
		OrderBy: []*pb.StructuredQuery_Order{ord("score", pb.StructuredQuery_ASCENDING)},
		StartAt: &pb.Cursor{Values: []*pb.Value{{ValueType: &pb.Value_IntegerValue{IntegerValue: 3}}}},
	})
	if Eligible(q) {
		t.Error("cursors are not translated to key bounds yet; serving them would skip " +
			"or repeat documents at page boundaries")
	}
}
