package rules

import (
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/query"
	"github.com/omelas-tech/kor/internal/value"
)

// Rules are not filters. A query is allowed only when the rule holds for
// every document the query could return, so a client can never learn, from a
// denial, that documents it may not read match its filter.
//
// Kor decides that symbolically. `resource` is a document whose fields are
// known exactly where the query pins them — a top-level `field == v` makes
// resource.data.field v, `field array-contains v` makes v in it true — and
// unknown everywhere else. Operators propagate unknown, logical ones absorb
// it the way they absorb errors (false && unknown is false), and only a
// definite true allows. A rule that depends on a field the query does not
// pin is therefore denied for queries, as Firestore denies it.
//
// This covers the patterns rules are written for (owner == uid filters,
// array-contains membership, rules that ignore resource). It is never more
// permissive than Firestore; it can be stricter where Firestore's solver
// proves more (range or in constraints, for instance).

type symMap struct {
	known    map[string]any // value or *symMap
	contains map[string]any // field -> a value the array is known to contain
}

func (m *symMap) field(k string) (any, bool) {
	if v, ok := m.known[k]; ok {
		return v, true
	}
	if v, ok := m.contains[k]; ok {
		return &symList{contains: v}, true
	}
	return nil, false
}

// symList is an array known only to contain one value.
type symList struct{ contains any }

// symResource is the document a query might return.
type symResource struct{ data *symMap }

// QueryRequest builds the list request for a query. collectionPath is the
// rules path of the query's parent (".../documents" or ".../documents/a/x").
func QueryRequest(q *query.Query, auth map[string]any, now time.Time) *Request {
	parent := PathFromName(q.Parent)
	var segs []pseg
	for _, s := range parent.segments() {
		segs = append(segs, pseg{s: s})
	}
	if q.AllDescendants {
		segs = append(segs, pseg{any: true})
	}
	segs = append(segs, pseg{s: q.CollectionID}, pseg{unknown: true})

	data := &symMap{known: map[string]any{}, contains: map[string]any{}}
	pin(data, q.Where)

	qm := map[string]any{"limit": nil, "offset": nil, "orderBy": nil}
	if q.Limit >= 0 {
		qm["limit"] = int64(q.Limit)
	}
	if q.Offset > 0 {
		qm["offset"] = int64(q.Offset)
	}
	path := Path(strings.TrimSuffix(string(parent), "/") + "/" + q.CollectionID + "/{document}")
	return &Request{
		Method: "list", Path: path, segs: segs, Auth: auth, Time: now,
		Resource: &symResource{data: data}, Query: qm,
	}
}

// pin records the constraints every result of a conjunction satisfies.
func pin(m *symMap, f *pb.StructuredQuery_Filter) {
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		if t.CompositeFilter.GetOp() != pb.StructuredQuery_CompositeFilter_AND {
			return // OR pins nothing on its own
		}
		for _, sub := range t.CompositeFilter.GetFilters() {
			pin(m, sub)
		}
	case *pb.StructuredQuery_Filter_FieldFilter:
		ff := t.FieldFilter
		fp := ff.GetField().GetFieldPath()
		if fp == "__name__" {
			return
		}
		path, err := value.ParseFieldPath(fp)
		if err != nil || len(path) == 0 {
			return
		}
		v := FromProto(ff.GetValue())
		switch ff.GetOp() {
		case pb.StructuredQuery_FieldFilter_EQUAL:
			if f, ok := v.(float64); ok && f != f {
				return // == NaN pins nothing
			}
			leaf(m, path).known[path[len(path)-1]] = v
		case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
			leaf(m, path).contains[path[len(path)-1]] = v
		}
	}
}

// leaf walks (creating) the nested maps above a dotted field path.
func leaf(m *symMap, path value.FieldPath) *symMap {
	for _, seg := range path[:len(path)-1] {
		next, ok := m.known[seg].(*symMap)
		if !ok {
			next = &symMap{known: map[string]any{}, contains: map[string]any{}}
			m.known[seg] = next
		}
		m = next
	}
	return m
}
