package store

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/index"
	"github.com/omelas-tech/kor/internal/query"
	"github.com/omelas-tech/kor/internal/value"
)

// ErrStop can be returned by a RunQuery yield callback to end iteration
// early without error.
var ErrStop = fmt.Errorf("kor: stop iteration")

// RunQuery streams matching documents to yield in query order.
//
// Execution strategy: Postgres narrows the candidate set (parent/collection
// bounds, jsonb containment for equality and array-contains filters, and —
// when the effective order is pure __name__ — ordering, cursors and limits),
// while full Firestore filter/order/cursor semantics are always re-evaluated
// in Go via the query package. Narrowing may over-include, never exclude.
func (s *Store) RunQuery(ctx context.Context, q *query.Query, yield func(*Doc) error) error {
	where, args := s.baseConditions(q)

	if probes, ok := containmentProbes(q.Where); ok {
		for _, group := range probes {
			or := "("
			for i, p := range group {
				if i > 0 {
					or += " OR "
				}
				args = append(args, p)
				or += fmt.Sprintf("data @> $%d", len(args))
			}
			or += ")"
			where += " AND " + or
		}
	}

	dir, nameOnly := q.NameOnlyOrder()
	if nameOnly {
		return s.runNameOrdered(ctx, q, where, args, dir, yield)
	}
	// A composite index serves this only if one exists, is backfilled, and
	// covers the shape exactly. Anything else falls back to runGeneral, which
	// remains the reference implementation — every shape the planner declines
	// is a performance opportunity, never a correctness risk.
	if defs := s.indexes.forCollection(q.CollectionID); len(defs) > 0 {
		if plan, ok := index.For(q, defs, s.indexes.readySet()); ok {
			return s.runIndexed(ctx, q, plan, yield)
		}
	}
	return s.runGeneral(ctx, q, where, args, yield)
}

// runNameOrdered streams in __name__ order straight from Postgres — the
// batch-scan fast path (OrderBy(DocumentID) + StartAfter + Limit).
func (s *Store) runNameOrdered(ctx context.Context, q *query.Query, where string, args []any, dir query.Direction, yield func(*Doc) error) error {
	order := "ASC"
	cmpStart, cmpEnd := ">", "<"
	if dir == query.Desc {
		order = "DESC"
		cmpStart, cmpEnd = "<", ">"
	}
	if start, end, startIncl, endIncl, ok := q.NameCursorBounds(); ok {
		if start != "" {
			op := cmpStart
			if startIncl {
				op += "="
			}
			args = append(args, start)
			where += fmt.Sprintf(" AND name %s $%d", op, len(args))
		}
		if end != "" {
			op := cmpEnd
			if endIncl {
				op += "="
			}
			args = append(args, end)
			where += fmt.Sprintf(" AND name %s $%d", op, len(args))
		}
	}
	sql := `SELECT name, data, create_time, update_time FROM documents WHERE ` + where +
		` ORDER BY name ` + order
	// LIMIT pushdown is only safe when Go-side evaluation cannot drop rows.
	if q.Where == nil && q.Offset == 0 && q.Limit >= 0 {
		sql += fmt.Sprintf(" LIMIT %d", q.Limit)
	}

	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("store: query: %w", err)
	}
	defer rows.Close()

	skipped := int32(0)
	emitted := int32(0)
	for rows.Next() {
		doc, err := scanDoc(rows)
		if err != nil {
			return err
		}
		if !q.Matches(doc.Name, doc.Fields) {
			continue
		}
		key := q.OrderKey(doc.Name, doc.Fields)
		if !q.InCursorRange(key) {
			continue
		}
		if skipped < q.Offset {
			skipped++
			continue
		}
		if q.Limit >= 0 && emitted >= q.Limit {
			break
		}
		if err := yield(doc); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
		emitted++
	}
	return rows.Err()
}

// runGeneral loads candidates, then filters/sorts/paginates in Go. Composite
// index_entries execution replaces this for hot query shapes later; the
// semantics here are the reference implementation.
func (s *Store) runGeneral(ctx context.Context, q *query.Query, where string, args []any, yield func(*Doc) error) error {
	rows, err := s.Pool.Query(ctx,
		`SELECT name, data, create_time, update_time FROM documents WHERE `+where, args...)
	if err != nil {
		return fmt.Errorf("store: query: %w", err)
	}
	type entry struct {
		doc *Doc
		key []*pb.Value
	}
	var entries []entry
	for rows.Next() {
		doc, err := scanDoc(rows)
		if err != nil {
			rows.Close()
			return err
		}
		if !q.Matches(doc.Name, doc.Fields) {
			continue
		}
		key := q.OrderKey(doc.Name, doc.Fields)
		if !q.InCursorRange(key) {
			continue
		}
		entries = append(entries, entry{doc, key})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	sort.SliceStable(entries, func(i, j int) bool {
		return q.CompareKeys(entries[i].key, entries[j].key) < 0
	})

	start := int(q.Offset)
	if start > len(entries) {
		start = len(entries)
	}
	end := len(entries)
	if q.Limit >= 0 && start+int(q.Limit) < end {
		end = start + int(q.Limit)
	}
	for _, e := range entries[start:end] {
		if err := yield(e.doc); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
	return nil
}

// RunCount executes the query counting matches, honoring limit and upTo.
func (s *Store) RunCount(ctx context.Context, q *query.Query, upTo int64) (int64, error) {
	// Filter-less, cursor-less counts (the "how many documents in this
	// collection" shape) reduce exactly to SQL count(*) — no need to stream
	// and decode every document.
	if q.Where == nil && q.Start == nil && q.End == nil && q.Offset == 0 {
		where, args := s.baseConditions(q)
		var n int64
		if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM documents WHERE `+where, args...).Scan(&n); err != nil {
			return 0, fmt.Errorf("store: count: %w", err)
		}
		if q.Limit >= 0 && n > int64(q.Limit) {
			n = int64(q.Limit)
		}
		if upTo > 0 && n > upTo {
			n = upTo
		}
		return n, nil
	}

	bounded := *q
	if upTo > 0 && (bounded.Limit < 0 || upTo < int64(bounded.Limit)) {
		bounded.Limit = int32(upTo)
	}
	var n int64
	err := s.RunQuery(ctx, &bounded, func(*Doc) error {
		n++
		return nil
	})
	return n, err
}

func (s *Store) baseConditions(q *query.Query) (string, []any) {
	if q.AllDescendants {
		// Collection-group scan under parent: any depth, matching collection id.
		return "collection_id = $1 AND name >= $2 AND name < $3",
			[]any{q.CollectionID, q.Parent + "/", q.Parent + "0"}
	}
	return "parent_path = $1 AND collection_id = $2", []any{q.Parent, q.CollectionID}
}

// containmentProbes extracts jsonb containment probe groups from an AND-only
// filter tree. Each group is a set of alternatives (OR) for one filter —
// numeric equality probes both the int64 and double encodings when the
// operand is integral. Returns ok=false when the tree contains OR (no safe
// narrowing).
func containmentProbes(f *pb.StructuredQuery_Filter) ([][][]byte, bool) {
	if f == nil {
		return nil, true
	}
	var out [][][]byte
	switch t := f.GetFilterType().(type) {
	case *pb.StructuredQuery_Filter_CompositeFilter:
		if t.CompositeFilter.GetOp() != pb.StructuredQuery_CompositeFilter_AND {
			return nil, false
		}
		for _, sub := range t.CompositeFilter.GetFilters() {
			probes, ok := containmentProbes(sub)
			if !ok {
				return nil, false
			}
			out = append(out, probes...)
		}
	case *pb.StructuredQuery_Filter_FieldFilter:
		ff := t.FieldFilter
		if ff.GetField().GetFieldPath() == "__name__" {
			return out, true // pseudo-field: not a data field, no containment probe
		}
		path, err := value.ParseFieldPath(ff.GetField().GetFieldPath())
		if err != nil {
			return out, true
		}
		switch ff.GetOp() {
		case pb.StructuredQuery_FieldFilter_EQUAL:
			if group := equalityProbes(path, ff.GetValue(), value.ContainmentJSON); group != nil {
				out = append(out, group)
			}
		case pb.StructuredQuery_FieldFilter_ARRAY_CONTAINS:
			if group := equalityProbes(path, ff.GetValue(), value.ArrayContainmentJSON); group != nil {
				out = append(out, group)
			}
		}
	}
	return out, true
}

func equalityProbes(path value.FieldPath, operand *pb.Value, build func(value.FieldPath, *pb.Value) ([]byte, error)) [][]byte {
	if value.IsNaN(operand) {
		return nil
	}
	var group [][]byte
	add := func(v *pb.Value) {
		if p, err := build(path, v); err == nil {
			group = append(group, p)
		}
	}
	add(operand)
	// Firestore numeric equality crosses int64/double; probe both encodings
	// when the operand is exactly representable in the other type.
	switch t := operand.GetValueType().(type) {
	case *pb.Value_IntegerValue:
		f := float64(t.IntegerValue)
		if int64(f) == t.IntegerValue && f != math.MaxInt64 {
			add(&pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}})
		}
	case *pb.Value_DoubleValue:
		f := t.DoubleValue
		if f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
			add(&pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: int64(f)}})
		}
	}
	if len(group) == 0 {
		return nil
	}
	return group
}

// scanDoc reads one row from a documents SELECT.
func scanDoc(rows interface {
	Scan(dest ...any) error
}) (*Doc, error) {
	var (
		name                   string
		data                   []byte
		createTime, updateTime time.Time
	)
	if err := rows.Scan(&name, &data, &createTime, &updateTime); err != nil {
		return nil, err
	}
	fields, err := value.UnmarshalFields(data)
	if err != nil {
		return nil, fmt.Errorf("store: document %s: %w", name, err)
	}
	return &Doc{Name: name, Fields: fields, CreateTime: createTime.UTC(), UpdateTime: updateTime.UTC()}, nil
}

// runIndexed streams a query from a composite index.
//
// The scan walks index_entries in key order, which is exactly query order, so
// Postgres applies OFFSET and LIMIT and only the surviving documents are
// fetched. That is the whole point: cost becomes O(limit) instead of O(matching
// documents).
//
// Every fetched document is still re-checked with q.Matches. On a correct plan
// that check never fails — the index prefix already encodes the equality
// filters — so it is an assertion, not filtering. It is affordable precisely
// because only the limit-sized page is fetched, and a failure means the planner
// chose an index that does not actually serve the query. That is returned as an
// error rather than silently skipped: dropping the document would turn a
// planner bug into a quietly short result set, which is the failure mode this
// whole design exists to avoid.
func (s *Store) runIndexed(ctx context.Context, q *query.Query, plan *index.Plan, yield func(*Doc) error) error {
	lo := plan.Prefix
	hi := index.PrefixEnd(lo)

	order := "ASC"
	if plan.Reversed {
		order = "DESC"
	}

	args := []any{plan.Def.ID(), lo}
	sql := `SELECT e.doc_name FROM index_entries e WHERE e.index_id = $1 AND e.key >= $2`
	if hi != nil {
		args = append(args, hi)
		sql += fmt.Sprintf(" AND e.key < $%d", len(args))
	}
	sql += " ORDER BY e.key " + order
	if q.Limit >= 0 {
		args = append(args, int64(q.Limit))
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if q.Offset > 0 {
		args = append(args, int64(q.Offset))
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}

	rows, err := s.Pool.Query(ctx, sql, args...)
	if err != nil {
		return fmt.Errorf("store: index scan: %w", err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		names = append(names, n)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(names) == 0 {
		return nil
	}

	docs, err := s.GetDocuments(ctx, names)
	if err != nil {
		return err
	}
	for _, n := range names {
		doc, ok := docs[n]
		if !ok {
			// An entry without its document means maintenance and the document
			// write diverged, which the shared transaction is supposed to make
			// impossible.
			return fmt.Errorf("store: index %s references missing document %s",
				index.EncodeID(plan.Def.ID()), n)
		}
		if !q.Matches(doc.Name, doc.Fields) {
			return fmt.Errorf("store: index %s returned %s which does not match the query — planner chose an index that does not serve it",
				index.EncodeID(plan.Def.ID()), n)
		}
		if err := yield(doc); err != nil {
			if err == ErrStop {
				return nil
			}
			return err
		}
	}
	return nil
}
