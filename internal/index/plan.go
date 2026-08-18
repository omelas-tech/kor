package index

import (
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/query"
)

// Planning: decide whether a composite index can serve a query, and with what
// scan bounds.
//
// The bar is deliberately high. An index-backed result that differs from the
// general path by even one document is a silent correctness bug, and the query
// looks fine while returning the wrong answer. So this refuses anything it
// cannot serve exactly, and the caller falls back to runGeneral — which stays
// the reference implementation. Every shape rejected here is a performance
// opportunity, never a correctness risk.

// Plan is a chosen index and the range to scan.
type Plan struct {
	Def      Def
	Prefix   []byte // equality prefix; the scan is [Prefix, PrefixEnd(Prefix))
	Reversed bool   // scan descending
}

// Eligible reports whether a query is a shape this package can serve at all,
// independent of which indexes exist. Split out so the reasons are testable and
// nameable rather than buried in one boolean.
func Eligible(q *query.Query) bool {
	// Cursors are not handled yet: resuming mid-index means translating cursor
	// values into a key bound, and getting that subtly wrong skips or repeats
	// documents at page boundaries — the kind of bug that only shows up under
	// pagination in production.
	if q.Start != nil || q.End != nil {
		return false
	}
	// Only conjunctions of equality filters. Inequalities need a range bound
	// rather than a prefix, and disjunctions need a union of scans; both are
	// worth doing, neither is done here.
	if _, ok := equalityFields(q.Where); !ok {
		return false
	}
	return true
}

// For selects an index that can serve q, or reports false.
//
// ready gates on backfill completion: an index registered but not yet
// backfilled is missing entries for every document written before it existed,
// so serving reads from it silently returns fewer results than the collection
// holds.
func For(q *query.Query, defs []Def, ready map[int64]bool) (*Plan, bool) {
	if !Eligible(q) {
		return nil, false
	}
	eq, _ := equalityFields(q.Where)

	for _, d := range defs {
		if d.CollectionID != q.CollectionID || d.Group != q.AllDescendants {
			continue
		}
		if !ready[d.ID()] {
			continue
		}
		prefix, reversed, ok := match(d, eq, q.Order)
		if !ok {
			continue
		}
		return &Plan{Def: d, Prefix: prefix, Reversed: reversed}, true
	}
	return nil, false
}

// match tests one definition against a query's equality set and ordering.
//
// The index must begin with exactly the equality-filtered fields — as a set,
// since their relative order in the prefix does not affect which documents fall
// in the range — and continue with exactly the query's ordering terms, in
// order. Directions must either all agree or all oppose: a scan can run
// backwards, but it cannot reverse one field and not another.
func match(d Def, eq map[string]*pb.Value, order []query.OrderSpec) (prefix []byte, reversed bool, ok bool) {
	// Match against the index's EFFECTIVE fields, which include the implicit
	// __name__ terminator Key() appends. query.Parse likewise appends __name__
	// to every effective ordering, so without this the two lists are always
	// off by one and no index ever matches — the planner declines everything
	// and silently falls back, which looks like working software.
	fields := effectiveFields(d)
	if len(fields) < len(eq)+len(order) {
		return nil, false, false
	}

	// Leading fields must be precisely the equality set — no more, no fewer.
	// A superset would leave an unconstrained field inside the prefix, so the
	// scan range would not isolate the matching group.
	eqValues := make([]*pb.Value, 0, len(eq))
	for i := 0; i < len(eq); i++ {
		f := fields[i]
		v, isEq := eq[f.Path]
		if !isEq {
			return nil, false, false
		}
		eqValues = append(eqValues, v)
	}

	// Remaining index fields must line up with the ordering terms.
	rest := fields[len(eq):]
	if len(rest) < len(order) {
		return nil, false, false
	}
	for i, spec := range order {
		f := rest[i]
		want := NameField
		if !spec.IsName {
			want = spec.Path.String()
		}
		if f.Path != want {
			return nil, false, false
		}
		descWanted := spec.Dir == query.Desc
		if i == 0 {
			reversed = f.Desc != descWanted
		} else if (f.Desc != descWanted) != reversed {
			// Mixed agreement: this index orders some terms with the query and
			// others against it, so no single scan direction produces the
			// requested order.
			return nil, false, false
		}
	}
	// Any index fields beyond the ordering terms are harmless: they only refine
	// the order within groups the query does not distinguish.

	return d.PrefixKey(eqValues), reversed, true
}

// equalityFields collects field==value filters from a pure AND tree.
//
// Returns ok=false for anything else — an inequality, IN, array-contains, a
// disjunction, or a __name__ filter — because each needs bounds this planner
// does not build, and guessing would produce a scan that quietly omits results.
func equalityFields(f *pb.StructuredQuery_Filter) (map[string]*pb.Value, bool) {
	out := map[string]*pb.Value{}
	if f == nil {
		return out, true
	}
	if !collectEquality(f, out) {
		return nil, false
	}
	return out, true
}

func collectEquality(f *pb.StructuredQuery_Filter, out map[string]*pb.Value) bool {
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		if t.CompositeFilter.GetOp() != pb.StructuredQuery_CompositeFilter_AND {
			return false
		}
		for _, sub := range t.CompositeFilter.GetFilters() {
			if !collectEquality(sub, out) {
				return false
			}
		}
		return true
	case *pb.StructuredQuery_Filter_FieldFilter:
		ff := t.FieldFilter
		if ff.GetOp() != pb.StructuredQuery_FieldFilter_EQUAL {
			return false
		}
		path := ff.GetField().GetFieldPath()
		if path == NameField {
			return false // a name filter bounds the scan differently
		}
		if _, dup := out[path]; dup {
			return false // two equalities on one field: degenerate, let the general path handle it
		}
		out[path] = ff.GetValue()
		return true
	default:
		// Unary filters (IS NULL / IS NAN) are not equality in the index sense.
		return false
	}
}

// effectiveFields is a definition's fields plus the implicit __name__
// terminator, in the direction of the last field — the exact shape Key()
// encodes, and the shape a query's effective ordering is expressed in.
func effectiveFields(d Def) []Field {
	if len(d.Fields) > 0 && d.Fields[len(d.Fields)-1].Path == NameField {
		return d.Fields
	}
	out := make([]Field, 0, len(d.Fields)+1)
	out = append(out, d.Fields...)
	out = append(out, Field{Path: NameField, Desc: d.trailingDirection()})
	return out
}
