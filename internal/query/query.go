// Package query implements Firestore's StructuredQuery semantics: filter
// evaluation, effective ordering, cursors, and projections. The store layer
// decides how to fetch candidate documents; this package decides what
// matches and in which order — it is the single place where Firestore query
// behavior is defined.
package query

import (
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/omelas-tech/kor/internal/value"
)

// Direction of an ordering term.
type Direction int8

const (
	Asc  Direction = 1
	Desc Direction = -1
)

// OrderSpec is one effective ordering term.
type OrderSpec struct {
	Path   value.FieldPath // nil when IsName
	IsName bool
	Dir    Direction
}

// Cursor is a start or end position.
type Cursor struct {
	Values []*pb.Value
	Before bool
}

// Query is a parsed StructuredQuery bound to a parent.
type Query struct {
	Parent         string // resource name the query runs under
	CollectionID   string
	AllDescendants bool

	Where *pb.StructuredQuery_Filter
	Order []OrderSpec // effective order: explicit + implicit inequality + __name__

	Start, End *Cursor
	Offset     int32
	Limit      int32 // -1 = unlimited

	Projection []value.FieldPath // nil = full document
	KeysOnly   bool
}

// Parse validates a StructuredQuery and computes the effective ordering.
func Parse(parent string, sq *pb.StructuredQuery) (*Query, error) {
	if sq == nil {
		return nil, status.Error(codes.InvalidArgument, "missing structured query")
	}
	if len(sq.GetFrom()) != 1 {
		return nil, status.Errorf(codes.InvalidArgument, "queries must have exactly one collection selector, got %d", len(sq.GetFrom()))
	}
	sel := sq.GetFrom()[0]
	q := &Query{
		Parent:         parent,
		CollectionID:   sel.GetCollectionId(),
		AllDescendants: sel.GetAllDescendants(),
		Where:          sq.GetWhere(),
		Offset:         sq.GetOffset(),
		Limit:          -1,
	}
	if sq.GetLimit() != nil {
		q.Limit = sq.GetLimit().GetValue()
	}

	// Explicit ordering.
	nameOrdered := false
	lastDir := Asc
	// Firestore rejects a duplicated orderBy field ("order by clause cannot
	// contain duplicate fields x"). Accepting it would make Kor quietly more
	// permissive than the API it implements: a client that works against Kor
	// would then fail against Firestore, which is the direction of divergence
	// that hurts most in a migration.
	seenOrder := map[string]bool{}
	for _, ob := range sq.GetOrderBy() {
		if fp := ob.GetField().GetFieldPath(); seenOrder[fp] {
			return nil, status.Errorf(codes.InvalidArgument,
				"order by clause cannot contain duplicate fields %s", fp)
		} else {
			seenOrder[fp] = true
		}
		dir := Asc
		if ob.GetDirection() == pb.StructuredQuery_DESCENDING {
			dir = Desc
		}
		lastDir = dir
		fp := ob.GetField().GetFieldPath()
		if fp == "__name__" {
			q.Order = append(q.Order, OrderSpec{IsName: true, Dir: dir})
			nameOrdered = true
			continue
		}
		path, err := value.ParseFieldPath(fp)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad orderBy field %q: %v", fp, err)
		}
		q.Order = append(q.Order, OrderSpec{Path: path, Dir: dir})
	}

	// Implicit ordering: with no explicit orderBy, an inequality field sorts
	// first (ascending), exactly as Firestore does. An inequality on
	// __name__ needs no extra term — the always-appended name tiebreaker IS
	// that ordering.
	if len(q.Order) == 0 {
		if ineq := firstInequalityField(q.Where); ineq != "" && ineq != "__name__" {
			path, err := value.ParseFieldPath(ineq)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "bad filter field %q: %v", ineq, err)
			}
			q.Order = append(q.Order, OrderSpec{Path: path, Dir: Asc})
			lastDir = Asc
		}
	}
	// __name__ is always the final tiebreaker, inheriting the last direction.
	if !nameOrdered {
		q.Order = append(q.Order, OrderSpec{IsName: true, Dir: lastDir})
	}

	if c := sq.GetStartAt(); c != nil {
		q.Start = &Cursor{Values: c.GetValues(), Before: c.GetBefore()}
	}
	if c := sq.GetEndAt(); c != nil {
		q.End = &Cursor{Values: c.GetValues(), Before: c.GetBefore()}
	}

	canonicalizeNameOperands(q)

	if p := sq.GetSelect(); p != nil {
		fields := p.GetFields()
		if len(fields) == 1 && fields[0].GetFieldPath() == "__name__" {
			q.KeysOnly = true
		} else {
			for _, f := range fields {
				if f.GetFieldPath() == "__name__" {
					continue // name is always present on returned documents
				}
				path, err := value.ParseFieldPath(f.GetFieldPath())
				if err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "bad select field %q: %v", f.GetFieldPath(), err)
				}
				q.Projection = append(q.Projection, path)
			}
		}
	}
	return q, nil
}

// canonicalizeNameOperands rewrites string operands of __name__ filters and
// cursors into full document references. SDKs (the Go client among them)
// send Where(DocumentID, op, "docid") with a bare string_value; the backend
// resolves it relative to the query target. Collection-group queries resolve
// relative to the parent instead (the string then carries the full relative
// path).
func canonicalizeNameOperands(q *Query) {
	base := q.Parent
	if !q.AllDescendants {
		base = q.Parent + "/" + q.CollectionID
	}
	toRef := func(v *pb.Value) *pb.Value {
		if s, ok := v.GetValueType().(*pb.Value_StringValue); ok {
			return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: base + "/" + s.StringValue}}
		}
		return v
	}
	var walk func(f *pb.StructuredQuery_Filter)
	walk = func(f *pb.StructuredQuery_Filter) {
		switch t := f.GetFilterType().(type) {
		case *pb.StructuredQuery_Filter_CompositeFilter:
			for _, sub := range t.CompositeFilter.GetFilters() {
				walk(sub)
			}
		case *pb.StructuredQuery_Filter_FieldFilter:
			ff := t.FieldFilter
			if ff.GetField().GetFieldPath() != "__name__" || ff.Value == nil {
				return
			}
			if arr, ok := ff.Value.GetValueType().(*pb.Value_ArrayValue); ok {
				for i, el := range arr.ArrayValue.GetValues() {
					arr.ArrayValue.Values[i] = toRef(el)
				}
				return
			}
			ff.Value = toRef(ff.Value)
		}
	}
	if q.Where != nil {
		walk(q.Where)
	}
	for _, cur := range []*Cursor{q.Start, q.End} {
		if cur == nil {
			continue
		}
		for i, v := range cur.Values {
			if i < len(q.Order) && q.Order[i].IsName {
				cur.Values[i] = toRef(v)
			}
		}
	}
}

// firstInequalityField returns the raw field path of the first
// range/not-equal filter in the tree, or "".
func firstInequalityField(f *pb.StructuredQuery_Filter) string {
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		for _, sub := range t.CompositeFilter.GetFilters() {
			if p := firstInequalityField(sub); p != "" {
				return p
			}
		}
	case *pb.StructuredQuery_Filter_FieldFilter:
		switch t.FieldFilter.GetOp() {
		case pb.StructuredQuery_FieldFilter_LESS_THAN,
			pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_GREATER_THAN,
			pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL,
			pb.StructuredQuery_FieldFilter_NOT_EQUAL,
			pb.StructuredQuery_FieldFilter_NOT_IN:
			return t.FieldFilter.GetField().GetFieldPath()
		}
	case *pb.StructuredQuery_Filter_UnaryFilter:
		switch t.UnaryFilter.GetOp() {
		case pb.StructuredQuery_UnaryFilter_IS_NOT_NAN, pb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
			return t.UnaryFilter.GetField().GetFieldPath()
		}
	}
	return ""
}

// Matches reports whether a document satisfies the query's filters AND has
// every orderBy field present (Firestore excludes documents missing an
// ordering field).
func (q *Query) Matches(name string, fields map[string]*pb.Value) bool {
	for _, o := range q.Order {
		if o.IsName {
			continue
		}
		if _, ok := value.GetField(fields, o.Path); !ok {
			return false
		}
	}
	if q.Where == nil {
		return true
	}
	return evalFilter(q.Where, name, fields)
}

func evalFilter(f *pb.StructuredQuery_Filter, name string, fields map[string]*pb.Value) bool {
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		isOr := t.CompositeFilter.GetOp() == pb.StructuredQuery_CompositeFilter_OR
		for _, sub := range t.CompositeFilter.GetFilters() {
			ok := evalFilter(sub, name, fields)
			if isOr && ok {
				return true
			}
			if !isOr && !ok {
				return false
			}
		}
		return !isOr
	case *pb.StructuredQuery_Filter_FieldFilter:
		return evalFieldFilter(t.FieldFilter, name, fields)
	case *pb.StructuredQuery_Filter_UnaryFilter:
		return evalUnaryFilter(t.UnaryFilter, fields)
	default:
		return false
	}
}

func evalUnaryFilter(u *pb.StructuredQuery_UnaryFilter, fields map[string]*pb.Value) bool {
	path, err := value.ParseFieldPath(u.GetField().GetFieldPath())
	if err != nil {
		return false
	}
	v, ok := value.GetField(fields, path)
	if !ok {
		return false // missing fields match no unary filter, including IS_NOT_*
	}
	switch u.GetOp() {
	case pb.StructuredQuery_UnaryFilter_IS_NULL:
		return value.IsNull(v)
	case pb.StructuredQuery_UnaryFilter_IS_NOT_NULL:
		return !value.IsNull(v)
	case pb.StructuredQuery_UnaryFilter_IS_NAN:
		return value.IsNaN(v)
	case pb.StructuredQuery_UnaryFilter_IS_NOT_NAN:
		// Firestore's != NaN also excludes null.
		return !value.IsNaN(v) && !value.IsNull(v)
	}
	return false
}

func evalFieldFilter(ff *pb.StructuredQuery_FieldFilter, name string, fields map[string]*pb.Value) bool {
	var v *pb.Value
	if ff.GetField().GetFieldPath() == "__name__" {
		// The document-key pseudo-field: compares as a reference, which the
		// SDKs also use for Where(DocumentID, ...) prefix range scans.
		v = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: name}}
	} else {
		path, err := value.ParseFieldPath(ff.GetField().GetFieldPath())
		if err != nil {
			return false
		}
		var ok bool
		v, ok = value.GetField(fields, path)
		if !ok {
			return false // missing fields never match any field filter
		}
	}
	op := ff.GetOp()
	operand := ff.GetValue()

	switch op {
	case pb.StructuredQuery_FieldFilter_EQUAL:
		// NaN is NOT the IEEE NaN here. Firestore's emulator answers
		// `where a == NaN` with the documents holding NaN, so equality treats
		// NaN as a single self-equal value — which is also what makes it
		// orderable at all. Compare already collapses NaN to NaN, so this is
		// just the comparison.
		return value.Compare(v, operand) == 0

	case pb.StructuredQuery_FieldFilter_NOT_EQUAL:
		// Excludes a missing field and an explicit null, but NOT NaN: the
		// emulator answers `where a != 5` with the NaN document included, and
		// `where a != NaN` with it excluded. So NaN participates normally and
		// only null is special. (Verified against the emulator; the previous
		// code excluded NaN and silently dropped documents.)
		if value.IsNull(v) {
			return false
		}
		return value.Compare(v, operand) != 0

	case pb.StructuredQuery_FieldFilter_LESS_THAN,
		pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
		pb.StructuredQuery_FieldFilter_GREATER_THAN,
		pb.StructuredQuery_FieldFilter_GREATER_THAN_OR_EQUAL:
		// Range comparisons only apply within the operand's type bucket.
		if value.TypeOrder(v) != value.TypeOrder(operand) || value.IsNaN(v) || value.IsNaN(operand) {
			return false
		}
		c := value.Compare(v, operand)
		switch op {
		case pb.StructuredQuery_FieldFilter_LESS_THAN:
			return c < 0
		case pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL:
			return c <= 0
		case pb.StructuredQuery_FieldFilter_GREATER_THAN:
			return c > 0
		default:
			return c >= 0
		}

	case pb.StructuredQuery_FieldFilter_IN:
		for _, el := range operand.GetArrayValue().GetValues() {
			if value.IsNull(el) {
				// A null in the in-list matches nothing, not even a null field
				// — the same asymmetry NOT_IN already has, and the opposite of
				// `field == null`, which does match. Confirmed against the
				// Firestore emulator, which returns no documents for
				// `where a in [null, 1]` that hold null.
				continue
			}
			if !value.IsNaN(el) && !value.IsNaN(v) && value.Compare(v, el) == 0 {
				return true
			}
		}
		return false

	case pb.StructuredQuery_FieldFilter_NOT_IN:
		if value.IsNull(v) {
			return false // as with !=, null and a missing field are excluded
		}
		for _, el := range operand.GetArrayValue().GetValues() {
			if value.IsNull(el) {
				return false // a null in the not-in list matches nothing
			}
			if value.IsNaN(el) {
				// not-in is not the complement of in: `in [NaN]` matches
				// nothing, yet `not-in [NaN]` excludes nothing — including the
				// NaN document itself, which the emulator returns.
				continue
			}
			if value.Compare(v, el) == 0 {
				return false
			}
		}
		return true

	case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
		// Neither null nor NaN can be searched for inside an array. The SDKs
		// reject it client-side, but the server must not rely on that: a
		// direct RPC would otherwise match documents whose array holds null.
		if value.IsNull(operand) || value.IsNaN(operand) {
			return false
		}
		return arrayContains(v, operand)

	case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS_ANY:
		for _, el := range operand.GetArrayValue().GetValues() {
			// A null or NaN in the list is IGNORED, not fatal to the query:
			// the emulator answers `array-contains-any ["a", null]` with the
			// documents holding "a", and `array-contains-any [null]` with
			// none. Same asymmetry as `in`.
			if value.IsNull(el) || value.IsNaN(el) {
				continue
			}
			if arrayContains(v, el) {
				return true
			}
		}
		return false
	}
	return false
}

func arrayContains(field, el *pb.Value) bool {
	arr, ok := field.GetValueType().(*pb.Value_ArrayValue)
	if !ok || value.IsNaN(el) {
		return false
	}
	for _, m := range arr.ArrayValue.GetValues() {
		if !value.IsNaN(m) && value.Compare(m, el) == 0 {
			return true
		}
	}
	return false
}

// OrderKey builds the ordering tuple for a document.
func (q *Query) OrderKey(name string, fields map[string]*pb.Value) []*pb.Value {
	key := make([]*pb.Value, len(q.Order))
	for i, o := range q.Order {
		if o.IsName {
			key[i] = &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: name}}
			continue
		}
		v, _ := value.GetField(fields, o.Path) // presence guaranteed by Matches
		key[i] = v
	}
	return key
}

// CompareKeys orders two ordering tuples under the query's directions.
func (q *Query) CompareKeys(a, b []*pb.Value) int {
	for i, o := range q.Order {
		if c := value.Compare(a[i], b[i]); c != 0 {
			return c * int(o.Dir)
		}
	}
	return 0
}

// compareToCursor compares a document tuple to a (possibly prefix) cursor.
func (q *Query) compareToCursor(key []*pb.Value, cur *Cursor) int {
	n := len(cur.Values)
	if n > len(q.Order) {
		n = len(q.Order)
	}
	for i := 0; i < n; i++ {
		if c := value.Compare(key[i], cur.Values[i]); c != 0 {
			return c * int(q.Order[i].Dir)
		}
	}
	return 0
}

// InCursorRange applies start/end cursor semantics: a start cursor with
// before=true includes equal positions (startAt) and before=false excludes
// them (startAfter); an end cursor with before=true excludes equals
// (endBefore) and before=false includes them (endAt).
func (q *Query) InCursorRange(key []*pb.Value) bool {
	if q.Start != nil {
		c := q.compareToCursor(key, q.Start)
		if c < 0 || (c == 0 && !q.Start.Before) {
			return false
		}
	}
	if q.End != nil {
		c := q.compareToCursor(key, q.End)
		if c > 0 || (c == 0 && q.End.Before) {
			return false
		}
	}
	return true
}

// NameOnlyOrder reports whether the effective ordering is just __name__,
// letting the store push ordering and cursors down to SQL.
func (q *Query) NameOnlyOrder() (Direction, bool) {
	if len(q.Order) == 1 && q.Order[0].IsName {
		return q.Order[0].Dir, true
	}
	return Asc, false
}

// NameCursorBounds translates pure-__name__ cursors into resource-name
// bounds for SQL pushdown. Returns ok=false when cursors aren't name-only.
func (q *Query) NameCursorBounds() (start, end string, startIncl, endIncl bool, ok bool) {
	get := func(c *Cursor) (string, bool) {
		if c == nil {
			return "", true
		}
		if len(c.Values) != 1 {
			return "", false
		}
		r, isRef := c.Values[0].GetValueType().(*pb.Value_ReferenceValue)
		if !isRef {
			return "", false
		}
		return r.ReferenceValue, true
	}
	s, ok1 := get(q.Start)
	e, ok2 := get(q.End)
	if !ok1 || !ok2 {
		return "", "", false, false, false
	}
	startIncl = q.Start == nil || q.Start.Before
	endIncl = q.End != nil && !q.End.Before
	return s, e, startIncl, endIncl, true
}

// ApplyProjection reduces a document's fields to the selected paths.
// Returns the input map unchanged for full-document queries.
func (q *Query) ApplyProjection(fields map[string]*pb.Value) map[string]*pb.Value {
	if q.KeysOnly {
		return map[string]*pb.Value{}
	}
	if len(q.Projection) == 0 {
		return fields
	}
	out := map[string]*pb.Value{}
	for _, p := range q.Projection {
		if v, ok := value.GetField(fields, p); ok {
			value.SetField(out, p, v)
		}
	}
	return out
}
