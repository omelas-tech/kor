package index

import (
	"bytes"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/query"
	"github.com/omelas-tech/kor/internal/value"
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

// Plan is a chosen index and the half-open byte range to scan.
//
// Everything — the equality prefix, cursors, and inequality filters — collapses
// into [Lo, Hi). That is possible because the key encoding is prefix-free: a
// partial key (fewer fields than the index has) is a prefix of every full key
// under it, so "everything at or after this position" is a byte comparison and
// "everything strictly after this group" is PrefixEnd of it. A nil Hi means
// unbounded above.
type Plan struct {
	Def Def
	// Ranges is normally one. An `in` filter on a prefix field produces one
	// range per value: each is a separate contiguous group in the index, and
	// together they are the query's result set. They are disjoint, so no
	// document can appear twice.
	Ranges   []Range
	Reversed bool // scan descending
}

// Range is one contiguous scan [Lo, Hi). SuffixFrom is where this range's
// equality prefix ends, which is what makes multiple ranges comparable: their
// prefixes differ (and differ in LENGTH, since the encoding is variable-width),
// so ordering across them means comparing key[SuffixFrom:], never whole keys.
type Range struct {
	Lo, Hi     []byte
	SuffixFrom int
}

// Single reports the only range, for the common case.
func (p *Plan) Single() (Range, bool) {
	if len(p.Ranges) == 1 {
		return p.Ranges[0], true
	}
	return Range{}, false
}

// Eligible reports whether a query is a shape this package can serve at all,
// independent of which indexes exist. Split out so the reasons are testable and
// nameable rather than buried in one boolean.
func Eligible(q *query.Query) bool {
	// A conjunction of equalities, optionally with ONE inequality on the first
	// ordering field — which is the only place Firestore allows one, and the
	// only position a byte range can express. Disjunctions need a union of
	// scans and are still declined.
	if _, _, _, ok := splitFilters(q); !ok {
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
	eq, _, in, _ := splitFilters(q)

	for _, d := range defs {
		if d.CollectionID != q.CollectionID || d.Group != q.AllDescendants {
			continue
		}
		if !ready[d.ID()] {
			continue
		}
		ranges, reversed, ok := planRanges(d, eq, in, q)
		if !ok {
			continue
		}
		return &Plan{Def: d, Ranges: ranges, Reversed: reversed}, true
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
func match(d Def, eq map[string]*pb.Value, cPath string, order []query.OrderSpec) (prefix []byte, reversed bool, ok bool) {
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
		// A contains component and an equality component are different index
		// shapes — one entry per element versus one per document — so a query
		// filtering with `==` must not be served by an array-contains index,
		// nor the reverse.
		if f.Contains != (cPath != "" && f.Path == cPath) {
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

// Comparison operators this planner can turn into a byte bound.
type ineqOp int

const (
	gt ineqOp = iota
	gte
	lt
	lte
)

type inequality struct {
	path  string
	op    ineqOp
	value *pb.Value
}

// bounds folds the equality prefix, an optional inequality, and any cursors
// into one half-open byte range.
//
// Two different notions of "direction" meet here, and conflating them is the
// easy mistake:
//
//   - An INEQUALITY constrains values, so which side of the range it bounds
//     depends on how the INDEX FIELD is encoded. On an ascending field, score>5
//     is a lower byte bound; on a descending field the bytes are complemented,
//     so the same filter becomes an upper one.
//   - A CURSOR names a position in QUERY order, so which end it bounds depends
//     on whether the scan runs with or against the index — the Reversed flag.
//
// Getting either backwards does not error. It returns a page from the wrong end
// of the range, which is why every case is written out instead of folded into
// arithmetic.
func bounds(d Def, eqCount int, prefix []byte, reversed bool, q *query.Query) (lo, hi []byte, ok bool) {
	lo, hi = prefix, PrefixEnd(prefix)

	fields := effectiveFields(d)
	if eqCount >= len(fields) {
		return lo, hi, true
	}
	fieldDesc := fields[eqCount].Desc

	if _, ineq, _, valid := splitFilters(q); valid && ineq != nil {
		// A range comparison applies only within the operand's type. Without
		// this clamp the scan spans type boundaries and returns, say, strings
		// for `score > 4` — which runIndexed's re-check catches as an error
		// rather than a wrong answer, but only because that check exists.
		// NaN needs no special case: it carries its own type tag, so a numeric
		// operand's bucket already excludes it.
		bucket := append(append([]byte(nil), prefix...), value.TypeBucket(nil, ineq.value, fieldDesc)...)
		lo = maxBound(lo, bucket)
		hi = minBound(hi, PrefixEnd(bucket))
		if value.IsNaN(ineq.value) {
			// NaN matches no inequality on either side, so the range is empty.
			return lo, lo, true
		}

		k := cursorKey(d, eqCount, prefix, []*pb.Value{ineq.value})
		switch ineq.op {
		case gt:
			if fieldDesc {
				hi = minBound(hi, k)
			} else {
				lo = maxBound(lo, PrefixEnd(k))
			}
		case gte:
			if fieldDesc {
				hi = minBound(hi, PrefixEnd(k))
			} else {
				lo = maxBound(lo, k)
			}
		case lt:
			if fieldDesc {
				lo = maxBound(lo, PrefixEnd(k))
			} else {
				hi = minBound(hi, k)
			}
		case lte:
			if fieldDesc {
				lo = maxBound(lo, k)
			} else {
				hi = minBound(hi, PrefixEnd(k))
			}
		}
	}

	if q.Start != nil {
		k := cursorKey(d, eqCount, prefix, capVals(q.Start.Values, len(q.Order)))
		if reversed {
			hi = minBound(hi, endBoundFor(k, q.Start.Before))
		} else {
			lo = maxBound(lo, startBoundFor(k, q.Start.Before))
		}
	}
	if q.End != nil {
		k := cursorKey(d, eqCount, prefix, capVals(q.End.Values, len(q.Order)))
		inclusive := !q.End.Before
		if reversed {
			lo = maxBound(lo, startBoundFor(k, inclusive))
		} else {
			hi = minBound(hi, endBoundFor(k, inclusive))
		}
	}

	// An empty range is legal and simply yields nothing.
	if hi != nil && bytes.Compare(lo, hi) >= 0 {
		return lo, lo, true
	}
	return lo, hi, true
}

// startBoundFor is the inclusive lower bound for a position: at the group when
// inclusive, past all of it when not.
func startBoundFor(k []byte, inclusive bool) []byte {
	if inclusive {
		return k
	}
	return PrefixEnd(k)
}

// endBoundFor is the exclusive upper bound for a position: past the whole group
// when inclusive, at its start when not.
func endBoundFor(k []byte, inclusive bool) []byte {
	if inclusive {
		return PrefixEnd(k)
	}
	return k
}

// cursorKey encodes values at the ordering positions after the equality
// prefix, in the index's own directions.
func cursorKey(d Def, eqCount int, prefix []byte, vals []*pb.Value) []byte {
	key := append([]byte(nil), prefix...)
	fields := effectiveFields(d)
	for i, v := range vals {
		pos := eqCount + i
		if pos >= len(fields) {
			break
		}
		if fields[pos].Desc {
			key = value.AppendSortKeyDesc(key, v)
		} else {
			key = value.AppendSortKey(key, v)
		}
	}
	return key
}

func maxBound(a, b []byte) []byte {
	if b == nil {
		return a
	}
	if a == nil || bytes.Compare(b, a) > 0 {
		return b
	}
	return a
}

// minBound treats nil as unbounded above.
func minBound(a, b []byte) []byte {
	if b == nil {
		return a
	}
	if a == nil || bytes.Compare(b, a) < 0 {
		return b
	}
	return a
}

// splitFilters separates a pure AND tree into equalities plus at most one
// inequality, which Firestore requires to be on the first ordering field.
//
// Returns ok=false for anything else — disjunctions, IN, array-contains, unary
// null/NaN filters, __name__ filters, a second inequality field — because each
// needs bounds this planner does not build, and guessing produces a scan that
// quietly omits results.
func splitFilters(q *query.Query) (eq map[string]*pb.Value, ineq *inequality, in *inList, ok bool) {
	eq = map[string]*pb.Value{}
	var contains *containsFilter
	if !collect(q.Where, eq, &ineq, &in, &contains) {
		return nil, nil, nil, false
	}
	if contains != nil {
		// An array-contains value occupies a prefix position exactly like an
		// equality; match() then requires the definition to mark that field as
		// a contains component, so the two cannot be confused.
		if _, dup := eq[contains.path]; dup {
			return nil, nil, nil, false
		}
		eq[contains.path] = contains.value
	}
	if ineq != nil {
		// The inequality field must be the first ordering term, or the range it
		// describes is not contiguous in this index.
		if len(q.Order) == 0 || q.Order[0].IsName || q.Order[0].Path.String() != ineq.path {
			return nil, nil, nil, false
		}
	}
	if in != nil {
		// The in-field joins the equality prefix, so it must not also be
		// filtered by equality or carry the inequality.
		if _, dup := eq[in.path]; dup {
			return nil, nil, nil, false
		}
		if ineq != nil && ineq.path == in.path {
			return nil, nil, nil, false
		}
		if len(in.values) == 0 {
			return nil, nil, nil, false
		}
	}
	return eq, ineq, in, true
}

// inList is a single `in` filter, which the planner turns into one scan range
// per value.
type inList struct {
	path   string
	values []*pb.Value
}

// containsFilter is a single array-contains filter.
type containsFilter struct {
	path  string
	value *pb.Value
}

// containsPath reports the array-contains field of a query, if any.
func containsPath(q *query.Query) (string, bool) {
	var ineq *inequality
	var in *inList
	var c *containsFilter
	if !collect(q.Where, map[string]*pb.Value{}, &ineq, &in, &c) || c == nil {
		return "", false
	}
	return c.path, true
}

// planRanges builds the scan ranges for one definition.
//
// An `in` filter is exactly N equality filters unioned, so each value is
// planned by the ordinary equality path with that value substituted. That reuse
// is the point: the bound arithmetic, the type clamping and the cursor
// handling are the parts most likely to be got wrong, and they stay in one
// place rather than being reimplemented for the multi-range case.
func planRanges(d Def, eq map[string]*pb.Value, in *inList, q *query.Query) ([]Range, bool, bool) {
	cPath, _ := containsPath(q)
	if in == nil {
		prefix, reversed, ok := match(d, eq, cPath, q.Order)
		if !ok {
			return nil, false, false
		}
		lo, hi, ok := bounds(d, len(eq), prefix, reversed, q)
		if !ok {
			return nil, false, false
		}
		return []Range{{Lo: lo, Hi: hi, SuffixFrom: len(prefix)}}, reversed, true
	}

	sub := make(map[string]*pb.Value, len(eq)+1)
	for k, v := range eq {
		sub[k] = v
	}
	var ranges []Range
	var reversed bool
	seen := map[string]bool{}
	for _, v := range in.values {
		// Firestore matches neither null nor NaN through `in`, so a range for
		// either would return documents the general path excludes.
		if value.IsNull(v) || value.IsNaN(v) {
			continue
		}
		sub[in.path] = v
		prefix, rev, ok := match(d, sub, cPath, q.Order)
		if !ok {
			return nil, false, false
		}
		lo, hi, ok := bounds(d, len(sub), prefix, rev, q)
		if !ok {
			return nil, false, false
		}
		// `in [x, x]` is legal and must not return x twice. Ranges are keyed by
		// their prefix, so duplicates collapse here rather than becoming
		// duplicate rows the caller would have to dedupe.
		if k := string(prefix); seen[k] {
			continue
		} else {
			seen[k] = true
		}
		reversed = rev
		ranges = append(ranges, Range{Lo: lo, Hi: hi, SuffixFrom: len(prefix)})
	}
	if len(ranges) == 0 {
		// Every value was null or NaN: the query matches nothing, and an empty
		// range set says so without a scan.
		return []Range{}, false, true
	}
	return ranges, reversed, true
}

func collect(f *pb.StructuredQuery_Filter, eq map[string]*pb.Value, ineq **inequality, in **inList, contains **containsFilter) bool {
	if f == nil {
		return true
	}
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		if t.CompositeFilter.GetOp() != pb.StructuredQuery_CompositeFilter_AND {
			return false
		}
		for _, sub := range t.CompositeFilter.GetFilters() {
			if !collect(sub, eq, ineq, in, contains) {
				return false
			}
		}
		return true
	case *pb.StructuredQuery_Filter_FieldFilter:
		ff := t.FieldFilter
		path := ff.GetField().GetFieldPath()
		if path == NameField {
			return false
		}
		switch ff.GetOp() {
		case pb.StructuredQuery_FieldFilter_EQUAL:
			if _, dup := eq[path]; dup {
				return false
			}
			eq[path] = ff.GetValue()
			return true
		case pb.StructuredQuery_FieldFilter_GREATER_THAN,
			pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_LESS_THAN,
			pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
			if *ineq != nil {
				return false // one inequality field only
			}
			var op ineqOp
			switch ff.GetOp() {
			case pb.StructuredQuery_FieldFilter_GREATER_THAN:
				op = gt
			case pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
				op = gte
			case pb.StructuredQuery_FieldFilter_LESS_THAN:
				op = lt
			default:
				op = lte
			}
			*ineq = &inequality{path: path, op: op, value: ff.GetValue()}
			return true
		case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
			if *contains != nil {
				return false // one array-contains field only, as Firestore requires
			}
			*contains = &containsFilter{path: path, value: ff.GetValue()}
			return true
		case pb.StructuredQuery_FieldFilter_IN:
			if *in != nil {
				return false // one in-field only: two would need a cross product
			}
			vals := ff.GetValue().GetArrayValue().GetValues()
			if len(vals) == 0 {
				return false
			}
			*in = &inList{path: path, values: vals}
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// capVals mirrors query.compareToCursor, which ignores cursor values beyond the
// ordering length rather than treating them as additional constraints.
func capVals(vals []*pb.Value, n int) []*pb.Value {
	if len(vals) > n {
		return vals[:n]
	}
	return vals
}

// GetRanges is a nil-safe accessor used by diagnostics.
func (p *Plan) GetRanges() []Range {
	if p == nil {
		return nil
	}
	return p.Ranges
}
