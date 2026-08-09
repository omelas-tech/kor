package value

import (
	"bytes"
	"math"
	"math/big"
	"sort"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// Type-order buckets per Firestore's documented mixed-type ordering:
// null < boolean < NaN < number < timestamp < string < bytes < reference
// < geopoint < array < vector < map.
const (
	orderNull = iota
	orderBool
	orderNaN
	orderNumber
	orderTimestamp
	orderString
	orderBytes
	orderReference
	orderGeoPoint
	orderArray
	orderVector
	orderMap
)

// IsNumeric reports whether a value is in the number or NaN buckets.
func IsNumeric(v *pb.Value) bool {
	o := TypeOrder(v)
	return o == orderNumber || o == orderNaN
}

// TypeOrder returns the ordering bucket for a value.
func TypeOrder(v *pb.Value) int {
	switch t := v.GetValueType().(type) {
	case *pb.Value_NullValue, nil:
		return orderNull
	case *pb.Value_BooleanValue:
		return orderBool
	case *pb.Value_IntegerValue:
		return orderNumber
	case *pb.Value_DoubleValue:
		if math.IsNaN(t.DoubleValue) {
			return orderNaN
		}
		return orderNumber
	case *pb.Value_TimestampValue:
		return orderTimestamp
	case *pb.Value_StringValue:
		return orderString
	case *pb.Value_BytesValue:
		return orderBytes
	case *pb.Value_ReferenceValue:
		return orderReference
	case *pb.Value_GeoPointValue:
		return orderGeoPoint
	case *pb.Value_ArrayValue:
		return orderArray
	case *pb.Value_MapValue:
		if isVector(t.MapValue) {
			return orderVector
		}
		return orderMap
	default:
		return orderMap + 1
	}
}

// isVector reports whether a map value is Firestore's Vector representation:
// {"__type__": "__vector__", "value": [doubles...]}.
func isVector(m *pb.MapValue) bool {
	f := m.GetFields()
	tv, ok := f["__type__"]
	if !ok || tv.GetStringValue() != "__vector__" {
		return false
	}
	_, ok = f["value"]
	return ok
}

// Compare implements Firestore's total order over values. It returns
// -1, 0, or +1. NaN compares equal to NaN; -0.0 equals 0.0; int64 and double
// compare as exact mathematical values.
func Compare(a, b *pb.Value) int {
	ta, tb := TypeOrder(a), TypeOrder(b)
	if ta != tb {
		return cmpInt(ta, tb)
	}
	switch ta {
	case orderNull, orderNaN:
		return 0
	case orderBool:
		return cmpBool(a.GetBooleanValue(), b.GetBooleanValue())
	case orderNumber:
		return compareNumbers(a, b)
	case orderTimestamp:
		at, bt := a.GetTimestampValue(), b.GetTimestampValue()
		if c := cmpInt64(at.GetSeconds(), bt.GetSeconds()); c != 0 {
			return c
		}
		// Firestore stores microsecond precision; compare at full nanos anyway
		// (writes are truncated before storage, so this is equivalent).
		return cmpInt(int(at.GetNanos()), int(bt.GetNanos()))
	case orderString:
		// UTF-8 encoded byte order.
		return bytes.Compare([]byte(a.GetStringValue()), []byte(b.GetStringValue()))
	case orderBytes:
		return bytes.Compare(a.GetBytesValue(), b.GetBytesValue())
	case orderReference:
		return CompareResourceNames(a.GetReferenceValue(), b.GetReferenceValue())
	case orderGeoPoint:
		ag, bg := a.GetGeoPointValue(), b.GetGeoPointValue()
		if c := cmpFloat(ag.GetLatitude(), bg.GetLatitude()); c != 0 {
			return c
		}
		return cmpFloat(ag.GetLongitude(), bg.GetLongitude())
	case orderArray:
		return compareArrays(a.GetArrayValue().GetValues(), b.GetArrayValue().GetValues())
	case orderVector:
		av := a.GetMapValue().GetFields()["value"].GetArrayValue().GetValues()
		bv := b.GetMapValue().GetFields()["value"].GetArrayValue().GetValues()
		// Vectors order by dimension first, then element-wise.
		if c := cmpInt(len(av), len(bv)); c != 0 {
			return c
		}
		return compareArrays(av, bv)
	case orderMap:
		return compareMaps(a.GetMapValue().GetFields(), b.GetMapValue().GetFields())
	}
	return 0
}

// Equal reports Firestore equality semantics (used by array-contains,
// arrayUnion/arrayRemove, and == filters). Note: unlike Compare, Firestore's
// == filter does NOT match NaN to NaN, which callers must handle at the
// filter layer; this function follows Compare (NaN == NaN) for use in
// ordering and array membership.
func Equal(a, b *pb.Value) bool {
	return Compare(a, b) == 0
}

func compareNumbers(a, b *pb.Value) int {
	ai, aIsInt := a.GetValueType().(*pb.Value_IntegerValue)
	bi, bIsInt := b.GetValueType().(*pb.Value_IntegerValue)
	if aIsInt && bIsInt {
		return cmpInt64(ai.IntegerValue, bi.IntegerValue)
	}
	if !aIsInt && !bIsInt {
		return cmpFloat(a.GetDoubleValue(), b.GetDoubleValue())
	}
	// Mixed int64/double: compare exactly (float64 can't represent all int64s).
	var af, bf big.Float
	if aIsInt {
		af.SetInt64(ai.IntegerValue)
	} else {
		af.SetFloat64(normalizeZero(a.GetDoubleValue()))
	}
	if bIsInt {
		bf.SetInt64(bi.IntegerValue)
	} else {
		bf.SetFloat64(normalizeZero(b.GetDoubleValue()))
	}
	return af.Cmp(&bf)
}

func compareArrays(a, b []*pb.Value) int {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		if c := Compare(a[i], b[i]); c != 0 {
			return c
		}
	}
	return cmpInt(len(a), len(b))
}

func compareMaps(a, b map[string]*pb.Value) int {
	ak, bk := sortedKeys(a), sortedKeys(b)
	n := min(len(ak), len(bk))
	for i := 0; i < n; i++ {
		if c := bytes.Compare([]byte(ak[i]), []byte(bk[i])); c != 0 {
			return c
		}
		if c := Compare(a[ak[i]], b[bk[i]]); c != 0 {
			return c
		}
	}
	return cmpInt(len(ak), len(bk))
}

// CompareResourceNames orders resource names segment-wise, which differs from
// plain string order when segments contain characters below '/':
// "a!/b" as a single segment vs "a" + "b" as two.
func CompareResourceNames(a, b string) int {
	as, bs := strings.Split(a, "/"), strings.Split(b, "/")
	n := min(len(as), len(bs))
	for i := 0; i < n; i++ {
		if c := bytes.Compare([]byte(as[i]), []byte(bs[i])); c != 0 {
			return c
		}
	}
	return cmpInt(len(as), len(bs))
}

func sortedKeys(m map[string]*pb.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare([]byte(keys[i]), []byte(keys[j])) < 0
	})
	return keys
}

func normalizeZero(f float64) float64 {
	if f == 0 {
		return 0 // collapse -0.0 to +0.0
	}
	return f
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

func cmpInt(a, b int) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}

// cmpFloat compares two non-NaN doubles with -0 == +0.
func cmpFloat(a, b float64) int {
	a, b = normalizeZero(a), normalizeZero(b)
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	default:
		return 0
	}
}
