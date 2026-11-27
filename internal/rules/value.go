package rules

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// Rules values are plain Go values:
//
//	null nil · bool · int int64 · float float64 · string · bytes []byte
//	timestamp time.Time · Duration · LatLng · Path · list []any
//	map map[string]any · *Set · *MapDiff · *Resource
//
// plus two evaluation-only values: *Error (an evaluation error, which denies
// unless a logical operator absorbs it) and Unknown (a value the symbolic
// query check cannot know; see query.go).
type (
	Duration time.Duration
	LatLng   struct{ Lat, Lng float64 }
	// Path is a rules path, "/databases/(default)/documents/c/d".
	Path string
	Set  struct{ items []any }
	// MapDiff is the result of map.diff(other).
	MapDiff struct{ left, right map[string]any }
	// Resource is a document as rules see it: data, id, __name__.
	Resource struct {
		Name Path
		ID   string
		Data any // map[string]any, or a symbolic map
	}
	Error   struct{ Msg string }
	unknown struct{}
)

// Unknown is the symbolic unknown value.
var Unknown = unknown{}

func errf(f string, a ...any) *Error { return &Error{fmt.Sprintf(f, a...)} }

func (e *Error) Error() string { return e.Msg }

func isErr(v any) bool     { _, ok := v.(*Error); return ok }
func isUnknown(v any) bool { _, ok := v.(unknown); return ok }

// typeName is the name `is` checks against.
func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "bool"
	case int64:
		return "int"
	case float64:
		return "float"
	case string:
		return "string"
	case []byte:
		return "bytes"
	case time.Time:
		return "timestamp"
	case Duration:
		return "duration"
	case LatLng:
		return "latlng"
	case Path:
		return "path"
	case []any, *symList:
		return "list"
	case map[string]any, *symMap:
		return "map"
	case *Set:
		return "set"
	case *MapDiff:
		return "map_diff"
	case *Resource, *symResource:
		return "map"
	}
	return fmt.Sprintf("%T", v)
}

// FromProto converts a Firestore value.
func FromProto(v *pb.Value) any {
	switch t := v.GetValueType().(type) {
	case *pb.Value_NullValue:
		return nil
	case *pb.Value_BooleanValue:
		return t.BooleanValue
	case *pb.Value_IntegerValue:
		return t.IntegerValue
	case *pb.Value_DoubleValue:
		return t.DoubleValue
	case *pb.Value_TimestampValue:
		return t.TimestampValue.AsTime()
	case *pb.Value_StringValue:
		return t.StringValue
	case *pb.Value_BytesValue:
		return t.BytesValue
	case *pb.Value_ReferenceValue:
		return PathFromName(t.ReferenceValue)
	case *pb.Value_GeoPointValue:
		return LatLng{t.GeoPointValue.GetLatitude(), t.GeoPointValue.GetLongitude()}
	case *pb.Value_ArrayValue:
		out := make([]any, len(t.ArrayValue.GetValues()))
		for i, e := range t.ArrayValue.GetValues() {
			out[i] = FromProto(e)
		}
		return out
	case *pb.Value_MapValue:
		return FieldsFromProto(t.MapValue.GetFields())
	}
	return nil
}

// FieldsFromProto converts a document's fields.
func FieldsFromProto(f map[string]*pb.Value) map[string]any {
	out := make(map[string]any, len(f))
	for k, v := range f {
		out[k] = FromProto(v)
	}
	return out
}

// PathFromName turns "projects/p/databases/d/documents/c/x" into the rules
// path "/databases/d/documents/c/x".
func PathFromName(name string) Path {
	if i := strings.Index(name, "/databases/"); i >= 0 {
		return Path(name[i:])
	}
	return Path("/" + strings.TrimPrefix(name, "/"))
}

// NewResource builds the resource for a document name and fields.
func NewResource(name string, fields map[string]*pb.Value) *Resource {
	p := PathFromName(name)
	return &Resource{Name: p, ID: p.last(), Data: FieldsFromProto(fields)}
}

func (p Path) segments() []string {
	s := strings.Trim(string(p), "/")
	if s == "" {
		return nil
	}
	return strings.Split(s, "/")
}

func (p Path) last() string {
	segs := p.segments()
	if len(segs) == 0 {
		return ""
	}
	return segs[len(segs)-1]
}

func newSet(items []any) *Set {
	s := &Set{}
	for _, it := range items {
		if !s.has(it) {
			s.items = append(s.items, it)
		}
	}
	return s
}

func (s *Set) has(v any) bool {
	for _, it := range s.items {
		if eq, _ := equal(it, v); eq {
			return true
		}
	}
	return false
}

func (d *MapDiff) keys(pick func(k string, inL, inR bool, same bool) bool) *Set {
	var ks []string
	seen := map[string]bool{}
	for _, m := range []map[string]any{d.left, d.right} {
		for k := range m {
			if seen[k] {
				continue
			}
			seen[k] = true
			lv, inL := d.left[k]
			rv, inR := d.right[k]
			same := false
			if inL && inR {
				same, _ = equal(lv, rv)
			}
			if pick(k, inL, inR, same) {
				ks = append(ks, k)
			}
		}
	}
	sort.Strings(ks)
	items := make([]any, len(ks))
	for i, k := range ks {
		items[i] = k
	}
	return &Set{items: items}
}

// equal is rules equality: deep, numbers compare across int and float, and
// values of different types are unequal rather than an error.
func equal(a, b any) (bool, *Error) {
	switch x := a.(type) {
	case nil:
		return b == nil, nil
	case int64:
		switch y := b.(type) {
		case int64:
			return x == y, nil
		case float64:
			return float64(x) == y, nil
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return x == float64(y), nil
		case float64:
			return x == y, nil
		}
	case bool:
		y, ok := b.(bool)
		return ok && x == y, nil
	case string:
		y, ok := b.(string)
		return ok && x == y, nil
	case []byte:
		y, ok := b.([]byte)
		return ok && string(x) == string(y), nil
	case time.Time:
		y, ok := b.(time.Time)
		return ok && x.Equal(y), nil
	case Duration:
		y, ok := b.(Duration)
		return ok && x == y, nil
	case LatLng:
		y, ok := b.(LatLng)
		return ok && x == y, nil
	case Path:
		y, ok := b.(Path)
		return ok && strings.Trim(string(x), "/") == strings.Trim(string(y), "/"), nil
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false, nil
		}
		for i := range x {
			if e, _ := equal(x[i], y[i]); !e {
				return false, nil
			}
		}
		return true, nil
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false, nil
		}
		for k, xv := range x {
			yv, ok := y[k]
			if !ok {
				return false, nil
			}
			if e, _ := equal(xv, yv); !e {
				return false, nil
			}
		}
		return true, nil
	case *Set:
		y, ok := b.(*Set)
		if !ok || len(x.items) != len(y.items) {
			return false, nil
		}
		for _, it := range x.items {
			if !y.has(it) {
				return false, nil
			}
		}
		return true, nil
	case *Resource:
		y, ok := b.(*Resource)
		if !ok {
			return false, nil
		}
		if x.Name != y.Name {
			return false, nil
		}
		return equal(x.Data, y.Data)
	}
	return false, nil
}

// compare orders two values of an ordered type; error for anything else.
func compare(a, b any) (int, *Error) {
	switch x := a.(type) {
	case int64:
		switch y := b.(type) {
		case int64:
			return cmp3(x < y, x > y), nil
		case float64:
			return cmpFloat(float64(x), y)
		}
	case float64:
		switch y := b.(type) {
		case int64:
			return cmpFloat(x, float64(y))
		case float64:
			return cmpFloat(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y), nil
		}
	case time.Time:
		if y, ok := b.(time.Time); ok {
			return x.Compare(y), nil
		}
	case Duration:
		if y, ok := b.(Duration); ok {
			return cmp3(x < y, x > y), nil
		}
	case []byte:
		if y, ok := b.([]byte); ok {
			return strings.Compare(string(x), string(y)), nil
		}
	case bool:
		if y, ok := b.(bool); ok {
			return cmp3(!x && y, x && !y), nil
		}
	}
	return 0, errf("cannot compare %s with %s", typeName(a), typeName(b))
}

func cmp3(lt, gt bool) int {
	switch {
	case lt:
		return -1
	case gt:
		return 1
	}
	return 0
}

func cmpFloat(x, y float64) (int, *Error) {
	if math.IsNaN(x) || math.IsNaN(y) {
		return 0, errf("NaN is not ordered")
	}
	return cmp3(x < y, x > y), nil
}
