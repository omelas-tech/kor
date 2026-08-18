package value

import (
	"encoding/binary"
	"math"
	"math/bits"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// Sort keys: a byte encoding of values whose unsigned byte-wise (memcmp)
// order equals Compare's order. Index entries store concatenated field sort
// keys, so range scans over (index_id, key) in Postgres traverse results in
// exact Firestore order.
//
// Layout:
//   - one type-tag byte per Compare's ordering buckets (gaps left for future
//     types), followed by a type-specific payload;
//   - variable-length payloads (strings, bytes) escape 0x00 as {0x00 0xFF}
//     and end with the terminator {0x00 0x01}, so no encoding is a prefix of
//     another and content always sorts after termination;
//   - composite values (references, arrays, maps, vectors) frame each item as
//     {0x01 <item>} and end with {0x00}: the end marker always compares below
//     an item marker, so shorter composites sort first regardless of what
//     follows in a concatenated multi-field key;
//   - descending fields append the complemented (XOR 0xFF) encoding; the
//     scheme is prefix-free, so complementing reverses order even across
//     field boundaries.
const (
	tagNull      = 0x10
	tagBool      = 0x18
	tagNaN       = 0x20
	tagNumber    = 0x28
	tagTimestamp = 0x30
	tagString    = 0x38
	tagBytes     = 0x40
	tagReference = 0x48
	tagGeoPoint  = 0x50
	tagArray     = 0x58
	tagVector    = 0x5C
	tagMap       = 0x60
)

// Number class bytes (after tagNumber).
const (
	numNegInf = 0x00
	numNeg    = 0x01
	numZero   = 0x02
	numPos    = 0x03
	numPosInf = 0x04
)

const (
	itemMarker = 0x01
	endMarker  = 0x00
)

// AppendSortKey appends the ascending sort key of v to dst.
func AppendSortKey(dst []byte, v *pb.Value) []byte {
	switch t := v.GetValueType().(type) {
	case *pb.Value_NullValue, nil:
		return append(dst, tagNull)
	case *pb.Value_BooleanValue:
		if t.BooleanValue {
			return append(dst, tagBool, 1)
		}
		return append(dst, tagBool, 0)
	case *pb.Value_IntegerValue:
		return appendIntKey(append(dst, tagNumber), t.IntegerValue)
	case *pb.Value_DoubleValue:
		if math.IsNaN(t.DoubleValue) {
			return append(dst, tagNaN)
		}
		return appendDoubleKey(append(dst, tagNumber), t.DoubleValue)
	case *pb.Value_TimestampValue:
		ts := t.TimestampValue
		dst = append(dst, tagTimestamp)
		dst = binary.BigEndian.AppendUint64(dst, uint64(ts.GetSeconds())^(1<<63))
		return binary.BigEndian.AppendUint32(dst, uint32(ts.GetNanos()))
	case *pb.Value_StringValue:
		return appendEscaped(append(dst, tagString), []byte(t.StringValue))
	case *pb.Value_BytesValue:
		return appendEscaped(append(dst, tagBytes), t.BytesValue)
	case *pb.Value_ReferenceValue:
		return appendNamePayload(append(dst, tagReference), t.ReferenceValue)
	case *pb.Value_GeoPointValue:
		g := t.GeoPointValue
		dst = append(dst, tagGeoPoint)
		dst = appendOrderedDouble(dst, g.GetLatitude())
		return appendOrderedDouble(dst, g.GetLongitude())
	case *pb.Value_ArrayValue:
		dst = append(dst, tagArray)
		for _, el := range t.ArrayValue.GetValues() {
			dst = append(dst, itemMarker)
			dst = AppendSortKey(dst, el)
		}
		return append(dst, endMarker)
	case *pb.Value_MapValue:
		if isVector(t.MapValue) {
			elems := t.MapValue.GetFields()["value"].GetArrayValue().GetValues()
			dst = append(dst, tagVector)
			// Dimension first, then element-wise.
			dst = binary.BigEndian.AppendUint32(dst, uint32(len(elems)))
			for _, el := range elems {
				dst = append(dst, itemMarker)
				dst = AppendSortKey(dst, el)
			}
			return append(dst, endMarker)
		}
		dst = append(dst, tagMap)
		for _, k := range sortedKeys(t.MapValue.GetFields()) {
			dst = append(dst, itemMarker)
			dst = appendEscaped(dst, []byte(k))
			dst = AppendSortKey(dst, t.MapValue.GetFields()[k])
		}
		return append(dst, endMarker)
	default:
		// Unknown future type: sort last.
		return append(dst, 0xFE)
	}
}

// AppendSortKeyDesc appends the descending sort key of v to dst.
func AppendSortKeyDesc(dst []byte, v *pb.Value) []byte {
	start := len(dst)
	dst = AppendSortKey(dst, v)
	complement(dst[start:])
	return dst
}

// AppendNameKey appends a segment-wise ordered key for a document resource
// name (the __name__ pseudo-field that terminates every index key).
func AppendNameKey(dst []byte, name string) []byte {
	return appendNamePayload(dst, name)
}

// AppendNameKeyDesc appends the descending form of AppendNameKey.
func AppendNameKeyDesc(dst []byte, name string) []byte {
	start := len(dst)
	dst = appendNamePayload(dst, name)
	complement(dst[start:])
	return dst
}

func appendNamePayload(dst []byte, name string) []byte {
	seg := name
	for len(seg) > 0 {
		i := 0
		for i < len(seg) && seg[i] != '/' {
			i++
		}
		dst = append(dst, itemMarker)
		dst = appendEscaped(dst, []byte(seg[:i]))
		if i == len(seg) {
			break
		}
		seg = seg[i+1:]
		if len(seg) == 0 {
			// Trailing slash: one final empty segment.
			dst = append(dst, itemMarker)
			dst = appendEscaped(dst, nil)
		}
	}
	return append(dst, endMarker)
}

// appendEscaped writes content with 0x00 -> {0x00 0xFF} escaping and the
// {0x00 0x01} terminator.
func appendEscaped(dst, content []byte) []byte {
	for _, b := range content {
		if b == 0x00 {
			dst = append(dst, 0x00, 0xFF)
		} else {
			dst = append(dst, b)
		}
	}
	return append(dst, 0x00, 0x01)
}

// appendIntKey encodes a nonzero-aware int64 into the unified numeric order.
func appendIntKey(dst []byte, x int64) []byte {
	if x == 0 {
		return append(dst, numZero)
	}
	mag := uint64(x)
	if x < 0 {
		mag = -mag // two's complement negation yields |x|, incl. MinInt64
	}
	nbits := bits.Len64(mag)
	exp := nbits - 1
	mant := mag << (64 - uint(nbits))
	return appendNumParts(dst, x < 0, exp, mant)
}

// appendDoubleKey encodes a non-NaN double into the unified numeric order.
func appendDoubleKey(dst []byte, d float64) []byte {
	switch {
	case d == 0: // covers -0.0
		return append(dst, numZero)
	case math.IsInf(d, 1):
		return append(dst, numPosInf)
	case math.IsInf(d, -1):
		return append(dst, numNegInf)
	}
	neg := d < 0
	frac, exp := math.Frexp(math.Abs(d)) // frac in [0.5, 1)
	// frac has at most 53 significant bits, so frac*2^53 is an exact integer.
	mant := uint64(math.Ldexp(frac, 53)) << 11
	return appendNumParts(dst, neg, exp-1, mant)
}

// appendNumParts writes class + biased exponent + left-aligned mantissa.
// The value represented is ±(mant/2^64) * 2^(exp+1), mant's top bit set.
func appendNumParts(dst []byte, neg bool, exp int, mant uint64) []byte {
	const expBias = 1074 // double min exponent is -1074 (subnormals)
	var payload [10]byte
	binary.BigEndian.PutUint16(payload[0:2], uint16(exp+expBias))
	binary.BigEndian.PutUint64(payload[2:10], mant)
	if neg {
		complement(payload[:])
		dst = append(dst, numNeg)
	} else {
		dst = append(dst, numPos)
	}
	return append(dst, payload[:]...)
}

// appendOrderedDouble writes a fixed 8-byte encoding of a non-NaN double in
// numeric order (used inside geopoints, which have bounded lat/lng).
func appendOrderedDouble(dst []byte, d float64) []byte {
	b := math.Float64bits(normalizeZero(d))
	if b&(1<<63) != 0 {
		b = ^b
	} else {
		b |= 1 << 63
	}
	return binary.BigEndian.AppendUint64(dst, b)
}

func complement(b []byte) {
	for i := range b {
		b[i] ^= 0xFF
	}
}

// TypeBucket appends the single type-tag byte that every sort key of v's type
// begins with, complemented when the field is encoded descending.
//
// It exists because Firestore range comparisons apply only WITHIN a type: `x >
// 4` matches numbers, never the strings that sort after them. The general path
// enforces that with an explicit TypeOrder check; an index range has to enforce
// it in bytes, by clamping the scan to [tag, PrefixEnd(tag)).
//
// The tag is read back off a real encoding rather than duplicated from the
// switch that writes it, so the two cannot drift apart.
func TypeBucket(dst []byte, v *pb.Value, desc bool) []byte {
	var k []byte
	if desc {
		k = AppendSortKeyDesc(nil, v)
	} else {
		k = AppendSortKey(nil, v)
	}
	if len(k) == 0 {
		return dst
	}
	return append(dst, k[0])
}
