package value

import (
	"bytes"
	"encoding/json"
	"math"
	"math/rand"
	"testing"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// --- generators ---------------------------------------------------------

func null() *pb.Value    { return &pb.Value{ValueType: &pb.Value_NullValue{}} }
func vbool(b bool) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: b}}
}
func vint(i int64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: i}}
}
func vdouble(d float64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: d}}
}
func vstr(s string) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}}
}
func vbytes(b []byte) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_BytesValue{BytesValue: b}}
}
func vref(r string) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: r}}
}
func vgeo(lat, lng float64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_GeoPointValue{GeoPointValue: &latlng.LatLng{Latitude: lat, Longitude: lng}}}
}
func vtime(t time.Time) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
}
func varr(vs ...*pb.Value) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vs}}}
}
func vmap(kv map[string]*pb.Value) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: kv}}}
}

var interestingInts = []int64{0, 1, -1, 63, -63, math.MaxInt64, math.MinInt64,
	1 << 53, (1 << 53) + 1, -(1 << 53) - 1, 999999999999999999}

var interestingDoubles = []float64{0, math.Copysign(0, -1), 1, -1, 0.5, 1.5,
	math.NaN(), math.Inf(1), math.Inf(-1), math.MaxFloat64, -math.MaxFloat64,
	math.SmallestNonzeroFloat64, -math.SmallestNonzeroFloat64,
	float64(1 << 53), float64(1<<53) + 2, 1e-300, 1e300, 3.141592653589793}

var interestingStrings = []string{"", "a", "ab", "a\x00b", "\x00", "é",
	"é", "é", "日本語", "￿", "\U0010FFFF", "a/b", "zzz"}

func randString(r *rand.Rand) string {
	if r.Intn(4) == 0 {
		return interestingStrings[r.Intn(len(interestingStrings))]
	}
	n := r.Intn(8)
	runes := make([]rune, n)
	for i := range runes {
		switch r.Intn(4) {
		case 0:
			runes[i] = rune(r.Intn(0x80)) // ASCII incl. NUL
		case 1:
			runes[i] = rune(0x80 + r.Intn(0x800-0x80))
		case 2:
			runes[i] = rune(0x800 + r.Intn(0xD800-0x800))
		default:
			runes[i] = rune(0xE000 + r.Intn(0x10FFFF-0xE000))
		}
	}
	return string(runes)
}

func randValue(r *rand.Rand, depth int) *pb.Value {
	max := 11
	if depth <= 0 {
		max = 9 // no arrays/maps at max depth
	}
	switch r.Intn(max) {
	case 0:
		return null()
	case 1:
		return vbool(r.Intn(2) == 0)
	case 2:
		if r.Intn(2) == 0 {
			return vint(interestingInts[r.Intn(len(interestingInts))])
		}
		return vint(int64(r.Uint64()))
	case 3:
		if r.Intn(2) == 0 {
			return vdouble(interestingDoubles[r.Intn(len(interestingDoubles))])
		}
		// Random bit patterns cover NaNs, infs, subnormals.
		return vdouble(math.Float64frombits(r.Uint64()))
	case 4:
		sec := r.Int63n(253402300800) // within year 9999
		return vtime(time.Unix(sec, int64(r.Intn(1_000_000))*1000).UTC())
	case 5:
		return vstr(randString(r))
	case 6:
		b := make([]byte, r.Intn(10))
		r.Read(b)
		return vbytes(b)
	case 7:
		segs := []string{"projects", "p", "databases", "(default)", "documents"}
		for i := 0; i < 2+r.Intn(4); i++ {
			segs = append(segs, randRefSegment(r))
		}
		ref := segs[0]
		for _, s := range segs[1:] {
			ref += "/" + s
		}
		return vref(ref)
	case 8:
		return vgeo(float64(r.Intn(180)-90)+r.Float64(), float64(r.Intn(360)-180)+r.Float64())
	case 9:
		n := r.Intn(4)
		vs := make([]*pb.Value, n)
		for i := range vs {
			vs[i] = randValue(r, depth-1)
		}
		return varr(vs...)
	default:
		n := r.Intn(4)
		m := make(map[string]*pb.Value, n)
		for i := 0; i < n; i++ {
			k := randString(r)
			for k == "" || bytes.ContainsRune([]byte(k), 0) {
				k = randString(r)
			}
			m[k] = randValue(r, depth-1)
		}
		return vmap(m)
	}
}

func randRefSegment(r *rand.Rand) string {
	const chars = "abcdefgh0123456789_-!"
	n := 1 + r.Intn(5)
	b := make([]byte, n)
	for i := range b {
		b[i] = chars[r.Intn(len(chars))]
	}
	return string(b)
}

// equalExact compares values for storage fidelity: exact oneof case, bit-exact
// doubles except NaN payloads (Firestore canonicalizes NaN), -0 preserved,
// timestamps compared at microsecond precision (Firestore truncates).
func equalExact(a, b *pb.Value) bool {
	switch at := a.GetValueType().(type) {
	case *pb.Value_NullValue, nil:
		_, ok := b.GetValueType().(*pb.Value_NullValue)
		return ok || b.GetValueType() == nil
	case *pb.Value_BooleanValue:
		bt, ok := b.GetValueType().(*pb.Value_BooleanValue)
		return ok && at.BooleanValue == bt.BooleanValue
	case *pb.Value_IntegerValue:
		bt, ok := b.GetValueType().(*pb.Value_IntegerValue)
		return ok && at.IntegerValue == bt.IntegerValue
	case *pb.Value_DoubleValue:
		bt, ok := b.GetValueType().(*pb.Value_DoubleValue)
		if !ok {
			return false
		}
		if math.IsNaN(at.DoubleValue) {
			return math.IsNaN(bt.DoubleValue)
		}
		return math.Float64bits(at.DoubleValue) == math.Float64bits(bt.DoubleValue)
	case *pb.Value_TimestampValue:
		bt, ok := b.GetValueType().(*pb.Value_TimestampValue)
		if !ok {
			return false
		}
		x := at.TimestampValue.AsTime().Truncate(time.Microsecond)
		y := bt.TimestampValue.AsTime().Truncate(time.Microsecond)
		return x.Equal(y)
	case *pb.Value_StringValue:
		bt, ok := b.GetValueType().(*pb.Value_StringValue)
		return ok && at.StringValue == bt.StringValue
	case *pb.Value_BytesValue:
		bt, ok := b.GetValueType().(*pb.Value_BytesValue)
		return ok && bytes.Equal(at.BytesValue, bt.BytesValue)
	case *pb.Value_ReferenceValue:
		bt, ok := b.GetValueType().(*pb.Value_ReferenceValue)
		return ok && at.ReferenceValue == bt.ReferenceValue
	case *pb.Value_GeoPointValue:
		bt, ok := b.GetValueType().(*pb.Value_GeoPointValue)
		return ok &&
			math.Float64bits(at.GeoPointValue.GetLatitude()) == math.Float64bits(bt.GeoPointValue.GetLatitude()) &&
			math.Float64bits(at.GeoPointValue.GetLongitude()) == math.Float64bits(bt.GeoPointValue.GetLongitude())
	case *pb.Value_ArrayValue:
		bt, ok := b.GetValueType().(*pb.Value_ArrayValue)
		if !ok || len(at.ArrayValue.GetValues()) != len(bt.ArrayValue.GetValues()) {
			return false
		}
		for i, av := range at.ArrayValue.GetValues() {
			if !equalExact(av, bt.ArrayValue.GetValues()[i]) {
				return false
			}
		}
		return true
	case *pb.Value_MapValue:
		bt, ok := b.GetValueType().(*pb.Value_MapValue)
		if !ok || len(at.MapValue.GetFields()) != len(bt.MapValue.GetFields()) {
			return false
		}
		for k, av := range at.MapValue.GetFields() {
			bv, ok := bt.MapValue.GetFields()[k]
			if !ok || !equalExact(av, bv) {
				return false
			}
		}
		return true
	}
	return false
}

// --- tests ---------------------------------------------------------------

func TestRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 20000; i++ {
		v := randValue(r, 3)
		enc, err := Marshal(v)
		if err != nil {
			t.Fatalf("marshal %v: %v", v, err)
		}
		// Simulate jsonb round-trip: re-parse and re-serialize through generic
		// JSON (key order scrambles; content must survive).
		var generic any
		if err := json.Unmarshal(enc, &generic); err != nil {
			t.Fatalf("re-parse: %v", err)
		}
		enc2, err := json.Marshal(generic)
		if err != nil {
			t.Fatalf("re-marshal: %v", err)
		}
		dec, err := Unmarshal(enc2)
		if err != nil {
			t.Fatalf("unmarshal %s: %v", enc2, err)
		}
		if !equalExact(v, dec) {
			t.Fatalf("round-trip mismatch:\n in: %v\nout: %v\nenc: %s", v, dec, enc)
		}
	}
}

func TestFieldsRoundTrip(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	for i := 0; i < 2000; i++ {
		mv := randValue(r, 2)
		m, ok := mv.GetValueType().(*pb.Value_MapValue)
		if !ok {
			continue
		}
		fields := m.MapValue.GetFields()
		enc, err := MarshalFields(fields)
		if err != nil {
			t.Fatalf("marshal fields: %v", err)
		}
		dec, err := UnmarshalFields(enc)
		if err != nil {
			t.Fatalf("unmarshal fields: %v", err)
		}
		if !equalExact(vmap(fields), vmap(dec)) {
			t.Fatalf("fields round-trip mismatch: %s", enc)
		}
	}
}

func TestSortKeyAgreesWithCompare(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 50000; i++ {
		a, b := randValue(r, 2), randValue(r, 2)
		want := Compare(a, b)
		got := sign(bytes.Compare(AppendSortKey(nil, a), AppendSortKey(nil, b)))
		if got != want {
			t.Fatalf("sortkey order mismatch: Compare=%d memcmp=%d\na=%v\nb=%v\nka=%x\nkb=%x",
				want, got, a, b, AppendSortKey(nil, a), AppendSortKey(nil, b))
		}
		gotDesc := sign(bytes.Compare(AppendSortKeyDesc(nil, a), AppendSortKeyDesc(nil, b)))
		if gotDesc != -want {
			t.Fatalf("desc sortkey order mismatch: Compare=%d desc-memcmp=%d\na=%v\nb=%v", want, gotDesc, a, b)
		}
	}
}

// Multi-field keys must order lexicographically by (field1, field2) even when
// field encodings have different lengths — the framing/prefix-freedom test.
func TestSortKeyConcatenation(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	for i := 0; i < 50000; i++ {
		a1, a2 := randValue(r, 2), randValue(r, 1)
		b1, b2 := randValue(r, 2), randValue(r, 1)
		want := Compare(a1, b1)
		if want == 0 {
			want = Compare(a2, b2)
		}
		ka := AppendSortKey(AppendSortKey(nil, a1), a2)
		kb := AppendSortKey(AppendSortKey(nil, b1), b2)
		if got := sign(bytes.Compare(ka, kb)); got != want {
			t.Fatalf("concat order mismatch: want %d got %d\na1=%v a2=%v\nb1=%v b2=%v", want, got, a1, a2, b1, b2)
		}
		// Mixed direction: first field DESC, second ASC.
		wantMixed := -Compare(a1, b1)
		if wantMixed == 0 {
			wantMixed = Compare(a2, b2)
		}
		ka = AppendSortKey(AppendSortKeyDesc(nil, a1), a2)
		kb = AppendSortKey(AppendSortKeyDesc(nil, b1), b2)
		if got := sign(bytes.Compare(ka, kb)); got != wantMixed {
			t.Fatalf("mixed-direction order mismatch: want %d got %d\na1=%v a2=%v\nb1=%v b2=%v", wantMixed, got, a1, a2, b1, b2)
		}
	}
}

func TestNumericOrderingLadder(t *testing.T) {
	// Strictly ascending per Firestore semantics; adjacent equal pairs listed
	// separately below.
	ladder := []*pb.Value{
		vdouble(math.Inf(-1)),
		vdouble(-1e308),
		vint(math.MinInt64),
		vint(-(1 << 53) - 1),
		vdouble(-float64(1 << 53)),
		vdouble(-1.5),
		vint(-1),
		vdouble(-math.SmallestNonzeroFloat64),
		vint(0),
		vdouble(math.SmallestNonzeroFloat64),
		vdouble(0.5),
		vint(1),
		vdouble(1.5),
		vint(2),
		vdouble(float64(1<<53) - 1),
		vint(1 << 53),
		vint((1 << 53) + 1),
		vdouble(float64(1<<53) + 2), // 9007199254740994.0
		vint((1 << 53) + 3),
		vint(math.MaxInt64),
		vdouble(1e308),
		vdouble(math.Inf(1)),
	}
	for i := 0; i < len(ladder)-1; i++ {
		if Compare(ladder[i], ladder[i+1]) != -1 {
			t.Errorf("ladder[%d] %v should be < ladder[%d] %v", i, ladder[i], i+1, ladder[i+1])
		}
		ka, kb := AppendSortKey(nil, ladder[i]), AppendSortKey(nil, ladder[i+1])
		if bytes.Compare(ka, kb) != -1 {
			t.Errorf("sortkey ladder[%d] %v should be < ladder[%d] %v (%x vs %x)", i, ladder[i], i+1, ladder[i+1], ka, kb)
		}
	}
	equalPairs := [][2]*pb.Value{
		{vint(0), vdouble(0)},
		{vint(0), vdouble(math.Copysign(0, -1))},
		{vint(5), vdouble(5)},
		{vint(1 << 53), vdouble(float64(1 << 53))},
		{vdouble(math.NaN()), vdouble(math.Float64frombits(0x7FF8000000000001))},
	}
	for _, p := range equalPairs {
		if Compare(p[0], p[1]) != 0 {
			t.Errorf("Compare(%v, %v) should be 0", p[0], p[1])
		}
		if !bytes.Equal(AppendSortKey(nil, p[0]), AppendSortKey(nil, p[1])) {
			t.Errorf("sortkeys of %v and %v should be equal", p[0], p[1])
		}
	}
}

func TestTypeBucketOrdering(t *testing.T) {
	buckets := []*pb.Value{
		null(),
		vbool(true),
		vdouble(math.NaN()),
		vint(42),
		vtime(time.Unix(1000, 0)),
		vstr("hello"),
		vbytes([]byte{1, 2}),
		vref("projects/p/databases/d/documents/c/x"),
		vgeo(1, 2),
		varr(vstr("z")),
		vmap(map[string]*pb.Value{"__type__": vstr("__vector__"), "value": varr(vdouble(1))}),
		vmap(map[string]*pb.Value{"a": vint(1)}),
	}
	for i := 0; i < len(buckets)-1; i++ {
		if Compare(buckets[i], buckets[i+1]) != -1 {
			t.Errorf("bucket %d should sort before bucket %d", i, i+1)
		}
		if bytes.Compare(AppendSortKey(nil, buckets[i]), AppendSortKey(nil, buckets[i+1])) != -1 {
			t.Errorf("sortkey bucket %d should sort before bucket %d", i, i+1)
		}
	}
}

func TestReferenceSegmentOrdering(t *testing.T) {
	// Segment-wise: "a" < "a/b"; and '!' (0x21) < '/' (0x2F) inside a segment
	// must not flip the segment-boundary order.
	a := vref("projects/p/databases/d/documents/a")
	ab := vref("projects/p/databases/d/documents/a/b")
	aBang := vref("projects/p/databases/d/documents/a!x")
	if Compare(a, ab) != -1 {
		t.Error("a should sort before a/b")
	}
	if Compare(ab, aBang) != -1 {
		t.Error("a/b should sort before a!x (segment-wise, 'a' < 'a!x')")
	}
	for _, pair := range [][2]*pb.Value{{a, ab}, {ab, aBang}} {
		if bytes.Compare(AppendSortKey(nil, pair[0]), AppendSortKey(nil, pair[1])) != -1 {
			t.Errorf("sortkey: %v should sort before %v", pair[0], pair[1])
		}
	}
}

func TestMapEmptyKeyFraming(t *testing.T) {
	empty := vmap(map[string]*pb.Value{})
	withEmptyish := vmap(map[string]*pb.Value{"a": vint(1)})
	// Empty map must sort before any non-empty map even when followed by a
	// larger second field in a concatenated key.
	k1 := AppendSortKey(AppendSortKey(nil, empty), vstr("zzzz"))
	k2 := AppendSortKey(AppendSortKey(nil, withEmptyish), vstr("aaaa"))
	if bytes.Compare(k1, k2) != -1 {
		t.Fatalf("empty-map-first invariant violated across field boundary")
	}
}

func sign(x int) int {
	switch {
	case x < 0:
		return -1
	case x > 0:
		return 1
	default:
		return 0
	}
}
