package query

import (
	"math"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

const parent = "projects/p/databases/(default)/documents"

func fv(kind string, v any) *pb.Value {
	switch kind {
	case "null":
		return &pb.Value{ValueType: &pb.Value_NullValue{}}
	case "bool":
		return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: v.(bool)}}
	case "int":
		return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(v.(int))}}
	case "double":
		return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: v.(float64)}}
	case "string":
		return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: v.(string)}}
	case "array":
		return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: v.([]*pb.Value)}}}
	}
	panic("bad kind")
}

func fieldFilter(path string, op pb.StructuredQuery_FieldFilter_Operator, operand *pb.Value) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{
		FieldFilter: &pb.StructuredQuery_FieldFilter{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: path},
			Op:    op,
			Value: operand,
		},
	}}
}

func unaryFilter(path string, op pb.StructuredQuery_UnaryFilter_Operator) *pb.StructuredQuery_Filter {
	return &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_UnaryFilter{
		UnaryFilter: &pb.StructuredQuery_UnaryFilter{
			OperandType: &pb.StructuredQuery_UnaryFilter_Field{
				Field: &pb.StructuredQuery_FieldReference{FieldPath: path},
			},
			Op: op,
		},
	}}
}

func sq(where *pb.StructuredQuery_Filter, order ...*pb.StructuredQuery_Order) *pb.StructuredQuery {
	return &pb.StructuredQuery{
		From:    []*pb.StructuredQuery_CollectionSelector{{CollectionId: "c"}},
		Where:   where,
		OrderBy: order,
	}
}

func mustParse(t *testing.T, s *pb.StructuredQuery) *Query {
	t.Helper()
	q, err := Parse(parent, s)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func TestFilterSemantics(t *testing.T) {
	docs := map[string]map[string]*pb.Value{
		"int5":    {"x": fv("int", 5)},
		"dbl5":    {"x": fv("double", 5.0)},
		"int9":    {"x": fv("int", 9)},
		"str":     {"x": fv("string", "zzz")},
		"null":    {"x": fv("null", nil)},
		"nan":     {"x": fv("double", math.NaN())},
		"missing": {"y": fv("int", 1)},
		"arr":     {"x": fv("array", []*pb.Value{fv("int", 5), fv("string", "a")})},
	}
	cases := []struct {
		name   string
		filter *pb.StructuredQuery_Filter
		want   []string // matching doc keys
	}{
		{"gt-int-excludes-other-types",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_GREATER_THAN, fv("int", 4)),
			[]string{"int5", "dbl5", "int9"}},
		{"lt-string-only-strings",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL, fv("string", "zzz")),
			[]string{"str"}},
		{"eq-cross-numeric",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_EQUAL, fv("double", 5.0)),
			[]string{"int5", "dbl5"}},
		// Expectations below were corrected against Google's Firestore emulator
		// (see e2e.TestFuzzAgainstFirestoreEmulator). They previously asserted
		// that != and not-in exclude NaN, which is what I assumed; Firestore
		// excludes only null and a missing field, and lets NaN participate.
		{"neq-excludes-null-and-missing-but-not-nan",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_NOT_EQUAL, fv("int", 5)),
			[]string{"int9", "str", "arr", "nan"}},
		{"eq-nan-matches-nan",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_EQUAL, fv("double", math.NaN())),
			[]string{"nan"}},
		{"in",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_IN,
				fv("array", []*pb.Value{fv("int", 9), fv("string", "zzz")})),
			[]string{"int9", "str"}},
		{"not-in-with-null-matches-nothing",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_NOT_IN,
				fv("array", []*pb.Value{fv("int", 5), fv("null", nil)})),
			nil},
		{"not-in-excludes-null-and-missing-but-not-nan",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_NOT_IN,
				fv("array", []*pb.Value{fv("int", 5)})),
			[]string{"int9", "str", "arr", "nan"}},
		// not-in is not the complement of in: `in [NaN]` matches nothing, yet
		// `not-in [NaN]` excludes nothing, NaN included.
		{"not-in-nan-excludes-nothing",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_NOT_IN,
				fv("array", []*pb.Value{fv("double", math.NaN())})),
			[]string{"int5", "dbl5", "int9", "str", "arr", "nan"}},
		{"in-nan-matches-nothing",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_IN,
				fv("array", []*pb.Value{fv("double", math.NaN())})),
			nil},
		{"array-contains",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS, fv("int", 5)),
			[]string{"arr"}},
		{"array-contains-any",
			fieldFilter("x", pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY,
				fv("array", []*pb.Value{fv("string", "a"), fv("string", "nope")})),
			[]string{"arr"}},
		{"is-null", unaryFilter("x", pb.StructuredQuery_UnaryFilter_IS_NULL), []string{"null"}},
		{"is-nan", unaryFilter("x", pb.StructuredQuery_UnaryFilter_IS_NAN), []string{"nan"}},
		{"is-not-null-excludes-missing",
			unaryFilter("x", pb.StructuredQuery_UnaryFilter_IS_NOT_NULL),
			[]string{"int5", "dbl5", "int9", "str", "nan", "arr"}},
		{"is-not-nan-excludes-null-and-missing",
			unaryFilter("x", pb.StructuredQuery_UnaryFilter_IS_NOT_NAN),
			[]string{"int5", "dbl5", "int9", "str", "arr"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			q := mustParse(t, sq(tc.filter))
			var got []string
			for key, fields := range docs {
				if q.Matches(parent+"/c/"+key, fields) {
					got = append(got, key)
				}
			}
			if len(got) != len(tc.want) {
				t.Fatalf("matched %v, want %v", got, tc.want)
			}
			wantSet := map[string]bool{}
			for _, w := range tc.want {
				wantSet[w] = true
			}
			for _, g := range got {
				if !wantSet[g] {
					t.Fatalf("matched %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestEffectiveOrdering(t *testing.T) {
	// Inequality without explicit orderBy: implicit field ASC + __name__ ASC.
	q := mustParse(t, sq(fieldFilter("x", pb.StructuredQuery_FieldFilter_GREATER_THAN, fv("int", 1))))
	if len(q.Order) != 2 || q.Order[0].IsName || q.Order[0].Dir != Asc || !q.Order[1].IsName {
		t.Fatalf("implicit inequality ordering wrong: %+v", q.Order)
	}
	// Explicit DESC: __name__ inherits DESC.
	q = mustParse(t, sq(nil, &pb.StructuredQuery_Order{
		Field:     &pb.StructuredQuery_FieldReference{FieldPath: "n"},
		Direction: pb.StructuredQuery_DESCENDING,
	}))
	if len(q.Order) != 2 || !q.Order[1].IsName || q.Order[1].Dir != Desc {
		t.Fatalf("name tiebreaker direction wrong: %+v", q.Order)
	}
	// Docs missing the orderBy field are excluded.
	if q.Matches(parent+"/c/d", map[string]*pb.Value{"other": fv("int", 1)}) {
		t.Fatal("doc missing orderBy field must not match")
	}
}

func TestCursorSemantics(t *testing.T) {
	mk := func(start, end *pb.Cursor) *Query {
		s := sq(nil, &pb.StructuredQuery_Order{
			Field: &pb.StructuredQuery_FieldReference{FieldPath: "n"},
		})
		s.StartAt = start
		s.EndAt = end
		return mustParse(t, s)
	}
	key := func(q *Query, n int, id string) []*pb.Value {
		return q.OrderKey(parent+"/c/"+id, map[string]*pb.Value{"n": fv("int", n)})
	}

	// startAt(2) includes 2; startAfter(2) excludes it.
	startAt := mk(&pb.Cursor{Values: []*pb.Value{fv("int", 2)}, Before: true}, nil)
	startAfter := mk(&pb.Cursor{Values: []*pb.Value{fv("int", 2)}, Before: false}, nil)
	if !startAt.InCursorRange(key(startAt, 2, "b")) {
		t.Fatal("startAt should include equal position")
	}
	if startAfter.InCursorRange(key(startAfter, 2, "b")) {
		t.Fatal("startAfter should exclude equal position")
	}
	if !startAfter.InCursorRange(key(startAfter, 3, "c")) {
		t.Fatal("startAfter should include later positions")
	}

	// endAt(2) includes 2; endBefore(2) excludes it.
	endAt := mk(nil, &pb.Cursor{Values: []*pb.Value{fv("int", 2)}, Before: false})
	endBefore := mk(nil, &pb.Cursor{Values: []*pb.Value{fv("int", 2)}, Before: true})
	if !endAt.InCursorRange(key(endAt, 2, "b")) || endAt.InCursorRange(key(endAt, 3, "c")) {
		t.Fatal("endAt bounds wrong")
	}
	if endBefore.InCursorRange(key(endBefore, 2, "b")) || !endBefore.InCursorRange(key(endBefore, 1, "a")) {
		t.Fatal("endBefore bounds wrong")
	}
}
