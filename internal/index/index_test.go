package index

import (
	"bytes"
	"math/rand"
	"sort"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/value"
)

func str(s string) *pb.Value  { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}} }
func num(n int64) *pb.Value   { return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: n}} }
func dbl(f float64) *pb.Value { return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}} }
func boolean(b bool) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: b}}
}

func def(fields ...Field) Def {
	return Def{CollectionID: "c", Fields: fields}
}

func TestSpecAndIDAreStableAndDistinct(t *testing.T) {
	a := def(Field{Path: "x"}, Field{Path: "y"})
	b := def(Field{Path: "x"}, Field{Path: "y"})
	if a.Spec() != b.Spec() || a.ID() != b.ID() {
		t.Error("identical definitions must share a spec and id")
	}

	// Every axis of the definition must change identity: reinterpreting old
	// entries under a new field order returns wrong results, which is worse
	// than returning none.
	variants := map[string]Def{
		"field order":     def(Field{Path: "y"}, Field{Path: "x"}),
		"direction":       def(Field{Path: "x"}, Field{Path: "y", Desc: true}),
		"extra field":     def(Field{Path: "x"}, Field{Path: "y"}, Field{Path: "z"}),
		"collection":      {CollectionID: "other", Fields: []Field{{Path: "x"}, {Path: "y"}}},
		"group vs single": {CollectionID: "c", Fields: []Field{{Path: "x"}, {Path: "y"}}, Group: true},
	}
	for name, v := range variants {
		if v.ID() == a.ID() {
			t.Errorf("%s must produce a different index id", name)
		}
	}
	if a.ID() < 0 {
		t.Error("ids must be non-negative — a signed bigint is legal but needlessly surprising")
	}
}

func TestKeyOmitsDocumentsMissingAnIndexedField(t *testing.T) {
	d := def(Field{Path: "a"}, Field{Path: "b"})
	if _, ok := d.Key("docs/1", map[string]*pb.Value{"a": str("x")}); ok {
		t.Error("a document missing an indexed field must be omitted — this is why " +
			"an orderBy silently excludes documents lacking that field")
	}
	if _, ok := d.Key("docs/1", map[string]*pb.Value{"a": str("x"), "b": num(1)}); !ok {
		t.Error("a document with every indexed field must be included")
	}
}

func TestKeyOrderMatchesFirestoreOrder(t *testing.T) {
	// The core property: byte order over concatenated keys must equal the
	// comparator's order over the same field tuples. If this drifts, an
	// index-backed query silently returns a different order than the general
	// path — the exact class of bug indexes must never introduce.
	d := def(Field{Path: "a"}, Field{Path: "b"})

	type row struct {
		name   string
		a, b   *pb.Value
		encKey []byte
	}
	vals := []*pb.Value{
		num(-5), num(0), num(1), num(1 << 53), dbl(-0.0), dbl(1.5), dbl(3),
		str(""), str("a"), str("ab"), str("b"), boolean(false), boolean(true),
	}

	var rows []row
	for i, a := range vals {
		for j, b := range vals {
			name := "projects/p/databases/(default)/documents/c/" + string(rune('a'+i)) + string(rune('a'+j))
			fields := map[string]*pb.Value{"a": a, "b": b}
			k, ok := d.Key(name, fields)
			if !ok {
				t.Fatalf("key build failed for %v/%v", a, b)
			}
			rows = append(rows, row{name, a, b, k})
		}
	}

	shuffled := append([]row(nil), rows...)
	rand.New(rand.NewSource(7)).Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})

	byKey := append([]row(nil), shuffled...)
	sort.SliceStable(byKey, func(i, j int) bool {
		return bytes.Compare(byKey[i].encKey, byKey[j].encKey) < 0
	})

	byCompare := append([]row(nil), shuffled...)
	sort.SliceStable(byCompare, func(i, j int) bool {
		if c := value.Compare(byCompare[i].a, byCompare[j].a); c != 0 {
			return c < 0
		}
		if c := value.Compare(byCompare[i].b, byCompare[j].b); c != 0 {
			return c < 0
		}
		return byCompare[i].name < byCompare[j].name
	})

	for i := range byKey {
		if byKey[i].name != byCompare[i].name {
			t.Fatalf("order diverges at %d: key order %s, comparator order %s",
				i, byKey[i].name, byCompare[i].name)
		}
	}
}

func TestDescendingFieldReversesOrder(t *testing.T) {
	asc := def(Field{Path: "a"})
	desc := def(Field{Path: "a", Desc: true})

	mk := func(d Def, v *pb.Value, name string) []byte {
		k, ok := d.Key(name, map[string]*pb.Value{"a": v})
		if !ok {
			t.Fatal("key build failed")
		}
		return k
	}
	base := "projects/p/databases/(default)/documents/c/"
	lo, hi := num(1), num(2)

	if bytes.Compare(mk(asc, lo, base+"x"), mk(asc, hi, base+"x")) >= 0 {
		t.Error("ascending: 1 must sort before 2")
	}
	if bytes.Compare(mk(desc, lo, base+"x"), mk(desc, hi, base+"x")) <= 0 {
		t.Error("descending: 1 must sort after 2")
	}
	// The implicit __name__ terminator follows the last field's direction, so
	// ties break in the same direction as the ordering the caller asked for.
	if bytes.Compare(mk(desc, lo, base+"a"), mk(desc, lo, base+"b")) <= 0 {
		t.Error("descending: equal values must break ties by name in reverse too")
	}
}

func TestPrefixScanIsolatesEqualityGroup(t *testing.T) {
	// Where(a==X).OrderBy(b): the prefix bounds must capture exactly the
	// documents with a==X, in b-order, and nothing else.
	d := def(Field{Path: "a"}, Field{Path: "b"})
	base := "projects/p/databases/(default)/documents/c/"

	var in, out [][]byte
	for _, b := range []int64{1, 2, 3} {
		k, _ := d.Key(base+"in", map[string]*pb.Value{"a": str("X"), "b": num(b)})
		in = append(in, k)
	}
	for _, a := range []string{"W", "Y"} {
		k, _ := d.Key(base+"out", map[string]*pb.Value{"a": str(a), "b": num(2)})
		out = append(out, k)
	}

	lo := d.PrefixKey([]*pb.Value{str("X")})
	hi := PrefixEnd(lo)
	within := func(k []byte) bool {
		return bytes.Compare(k, lo) >= 0 && (hi == nil || bytes.Compare(k, hi) < 0)
	}
	for i, k := range in {
		if !within(k) {
			t.Errorf("in-group key %d fell outside the prefix range", i)
		}
	}
	for i, k := range out {
		if within(k) {
			t.Errorf("out-of-group key %d fell inside the prefix range", i)
		}
	}
}

func TestPrefixEnd(t *testing.T) {
	if got := PrefixEnd([]byte{1, 2, 3}); !bytes.Equal(got, []byte{1, 2, 4}) {
		t.Errorf("PrefixEnd = %v, want [1 2 4]", got)
	}
	if got := PrefixEnd([]byte{1, 0xFF}); !bytes.Equal(got, []byte{2}) {
		t.Errorf("PrefixEnd should carry over 0xFF, got %v", got)
	}
	if got := PrefixEnd([]byte{0xFF, 0xFF}); got != nil {
		t.Errorf("an all-0xFF prefix has no upper bound, got %v", got)
	}
	if got := PrefixEnd(nil); got != nil {
		t.Errorf("an empty prefix has no upper bound, got %v", got)
	}
}
