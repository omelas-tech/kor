// Package index implements composite indexes as materialized rows, the way
// Firestore itself does, rather than as PostgreSQL expression indexes.
//
// Why rows. A query like Where(a==x).OrderBy(b).Limit(20) must return the
// first 20 documents in b-order without touching the rest. Kor's general path
// cannot: it narrows in SQL, then loads every surviving candidate into memory,
// re-evaluates Firestore semantics in Go, sorts, and only then applies the
// limit. That is correct — and it is the reference this package must match —
// but its cost is O(matching documents), not O(limit). At a thousand documents
// nobody notices; at a million it is the difference between a query and an
// outage.
//
// A materialized entry is (index_id, key, doc_name), where key is the
// concatenated sort keys of the indexed fields followed by the document name.
// Because value.AppendSortKey's byte order equals Firestore's total order and
// its encoding is prefix-free, a plain Postgres range scan over (index_id, key)
// traverses results in exact Firestore order — so LIMIT and OFFSET push down to
// the database and the planner is deterministic rather than dependent on what
// the query planner guesses about jsonb selectivity.
package index

import (
	"encoding/binary"
	"hash/fnv"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/value"
)

// Field is one component of a composite index.
type Field struct {
	Path string // field path, or "__name__" for the document name
	Desc bool
}

// Def is a composite index definition.
//
// Every index implicitly terminates with __name__ in the direction of its last
// field, matching Firestore: without it, documents with equal values for the
// indexed fields would have no defined order, and cursors could not resume
// deterministically.
type Def struct {
	CollectionID string
	Fields       []Field
	// Group indexes span every collection with this id regardless of parent,
	// serving collection-group queries.
	Group bool
}

// NameField is the implicit trailing component of every index.
const NameField = "__name__"

// Spec renders the canonical, stable description of a definition. It is the
// identity of the index: two definitions with the same spec are the same index,
// and any change to fields, order or direction yields a different one.
func (d Def) Spec() string {
	var b strings.Builder
	if d.Group {
		b.WriteString("group:")
	}
	b.WriteString(d.CollectionID)
	for _, f := range d.Fields {
		b.WriteByte('|')
		b.WriteString(f.Path)
		if f.Desc {
			b.WriteString(" desc")
		} else {
			b.WriteString(" asc")
		}
	}
	return b.String()
}

// ID is the stable identifier stored on every entry.
//
// Derived from the spec rather than assigned by a sequence, so it needs no
// coordination and survives restarts and rebuilds. A changed definition
// naturally becomes a different index: its old entries are simply unreachable
// and get swept, instead of being silently reinterpreted under the new field
// order — which would return wrong results rather than none.
func (d Def) ID() int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(d.Spec()))
	// Mask to 63 bits: Postgres bigint is signed, and a negative id would be
	// legal but needlessly surprising in logs and queries.
	return int64(h.Sum64() &^ (1 << 63))
}

// trailingDirection is the direction of the implicit __name__ component.
func (d Def) trailingDirection() bool {
	if len(d.Fields) == 0 {
		return false
	}
	return d.Fields[len(d.Fields)-1].Desc
}

// Key builds the index key for one document.
//
// Returns ok=false when the document does not belong in this index: Firestore
// omits a document from an index if any indexed field is absent, which is why
// an orderBy silently excludes documents missing that field. Reproducing that
// here is what keeps index-backed results identical to the general path.
func (d Def) Key(name string, fields map[string]*pb.Value) (key []byte, ok bool) {
	key = make([]byte, 0, 64)
	for _, f := range d.Fields {
		if f.Path == NameField {
			key = appendName(key, name, f.Desc)
			continue
		}
		fp, err := value.ParseFieldPath(f.Path)
		if err != nil {
			return nil, false
		}
		v, found := value.GetField(fields, fp)
		if !found || v == nil {
			return nil, false
		}
		if f.Desc {
			key = value.AppendSortKeyDesc(key, v)
		} else {
			key = value.AppendSortKey(key, v)
		}
	}
	// Implicit __name__ terminator, unless the definition named it explicitly
	// as its final component.
	if len(d.Fields) == 0 || d.Fields[len(d.Fields)-1].Path != NameField {
		key = appendName(key, name, d.trailingDirection())
	}
	return key, true
}

func appendName(dst []byte, name string, desc bool) []byte {
	if desc {
		return value.AppendNameKeyDesc(dst, name)
	}
	return value.AppendNameKey(dst, name)
}

// PrefixKey builds the key prefix for a set of leading equality values — the
// scan bound for a query whose equality filters match this index's prefix.
func (d Def) PrefixKey(eq []*pb.Value) []byte {
	key := make([]byte, 0, 32)
	for i, v := range eq {
		if i >= len(d.Fields) {
			break
		}
		if d.Fields[i].Desc {
			key = value.AppendSortKeyDesc(key, v)
		} else {
			key = value.AppendSortKey(key, v)
		}
	}
	return key
}

// PrefixEnd returns the exclusive upper bound for a prefix range scan: the
// prefix with its final byte incremented, or nil when the prefix is all 0xFF
// (meaning "scan to the end of this index").
func PrefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] != 0xFF {
			end[i]++
			return end[:i+1]
		}
	}
	return nil
}

// EncodeID renders an index id for logging and debug output.
func EncodeID(id int64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(id))
	const hex = "0123456789abcdef"
	out := make([]byte, 16)
	for i, c := range b {
		out[i*2], out[i*2+1] = hex[c>>4], hex[c&0x0F]
	}
	return string(out)
}
