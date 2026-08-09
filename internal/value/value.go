// Package value implements Kor's canonical encoding of Firestore values.
//
// Firestore's value model (google.firestore.v1.Value) does not fit raw JSON:
// int64 exceeds JSON number precision, doubles distinguish -0/NaN/±Inf,
// bytes and references are first-class types, and ordering is defined across
// types. Kor stores every value as a single-key "tagged" JSON object whose
// payloads are strings for all numeric/temporal types, so PostgreSQL's jsonb
// normalization can never alter a value:
//
//	{"z":null}          null
//	{"b":true}          boolean
//	{"i":"-42"}         int64, decimal string
//	{"d":"1.5"}         double, canonical shortest form ("NaN","+Inf","-Inf","-0")
//	{"t":"2026-01-02T03:04:05.000006Z"}  timestamp, UTC, fixed microseconds
//	{"s":"text"}        string (valid UTF-8, no U+0000)
//	{"s0":"YQBi"}       string containing U+0000, base64 of raw bytes
//	{"y":"AQID"}        bytes, std base64
//	{"r":"projects/p/databases/d/documents/c/id"}  reference
//	{"g":["52.4","4.9"]} geopoint [lat,lng], canonical double strings
//	{"a":[...]}         array of tagged values
//	{"m":{...}}         map of field name -> tagged value
//
// A document's data column is the "m" payload of its root: {"field": <tagged>, ...}.
// The encoding round-trips byte-perfectly through Value protos; jsonb only
// reorders object keys, which the decoder does not depend on.
package value

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Firestore's documented timestamp bounds.
var (
	minTimestamp = time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)
	maxTimestamp = time.Date(9999, time.December, 31, 23, 59, 59, 999999999, time.UTC)
)

const timestampLayout = "2006-01-02T15:04:05.000000Z07:00"

// MarshalFields encodes a document's fields map to canonical tagged JSON.
func MarshalFields(fields map[string]*pb.Value) ([]byte, error) {
	obj, err := encodeMapPayload(fields)
	if err != nil {
		return nil, err
	}
	return json.Marshal(obj)
}

// UnmarshalFields decodes a document data column back into a fields map.
func UnmarshalFields(data []byte) (map[string]*pb.Value, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("value: bad document data: %w", err)
	}
	return decodeMapPayload(raw)
}

// Marshal encodes a single value to canonical tagged JSON.
func Marshal(v *pb.Value) ([]byte, error) {
	obj, err := encode(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(obj)
}

// Unmarshal decodes a single tagged JSON value.
func Unmarshal(data []byte) (*pb.Value, error) {
	var raw json.RawMessage = data
	return decode(raw)
}

func encode(v *pb.Value) (any, error) {
	switch t := v.GetValueType().(type) {
	case *pb.Value_NullValue, nil:
		return map[string]any{"z": nil}, nil
	case *pb.Value_BooleanValue:
		return map[string]any{"b": t.BooleanValue}, nil
	case *pb.Value_IntegerValue:
		return map[string]any{"i": strconv.FormatInt(t.IntegerValue, 10)}, nil
	case *pb.Value_DoubleValue:
		return map[string]any{"d": formatDouble(t.DoubleValue)}, nil
	case *pb.Value_TimestampValue:
		s, err := formatTimestamp(t.TimestampValue)
		if err != nil {
			return nil, err
		}
		return map[string]any{"t": s}, nil
	case *pb.Value_StringValue:
		if strings.ContainsRune(t.StringValue, 0) {
			return map[string]any{"s0": base64.StdEncoding.EncodeToString([]byte(t.StringValue))}, nil
		}
		return map[string]any{"s": t.StringValue}, nil
	case *pb.Value_BytesValue:
		return map[string]any{"y": base64.StdEncoding.EncodeToString(t.BytesValue)}, nil
	case *pb.Value_ReferenceValue:
		if strings.ContainsRune(t.ReferenceValue, 0) {
			return nil, fmt.Errorf("value: reference contains NUL byte")
		}
		return map[string]any{"r": t.ReferenceValue}, nil
	case *pb.Value_GeoPointValue:
		lat, lng := t.GeoPointValue.GetLatitude(), t.GeoPointValue.GetLongitude()
		return map[string]any{"g": []any{formatDouble(lat), formatDouble(lng)}}, nil
	case *pb.Value_ArrayValue:
		arr := make([]any, 0, len(t.ArrayValue.GetValues()))
		for _, el := range t.ArrayValue.GetValues() {
			enc, err := encode(el)
			if err != nil {
				return nil, err
			}
			arr = append(arr, enc)
		}
		return map[string]any{"a": arr}, nil
	case *pb.Value_MapValue:
		obj, err := encodeMapPayload(t.MapValue.GetFields())
		if err != nil {
			return nil, err
		}
		return map[string]any{"m": obj}, nil
	default:
		return nil, fmt.Errorf("value: unsupported value type %T", t)
	}
}

func encodeMapPayload(fields map[string]*pb.Value) (map[string]any, error) {
	obj := make(map[string]any, len(fields))
	for k, fv := range fields {
		if strings.ContainsRune(k, 0) {
			return nil, fmt.Errorf("value: field name contains NUL byte")
		}
		enc, err := encode(fv)
		if err != nil {
			return nil, err
		}
		obj[k] = enc
	}
	return obj, nil
}

func decode(raw json.RawMessage) (*pb.Value, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("value: bad tagged value: %w", err)
	}
	if len(obj) != 1 {
		return nil, fmt.Errorf("value: tagged value must have exactly one key, got %d", len(obj))
	}
	for tag, payload := range obj {
		switch tag {
		case "z":
			return &pb.Value{ValueType: &pb.Value_NullValue{}}, nil
		case "b":
			var b bool
			if err := json.Unmarshal(payload, &b); err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_BooleanValue{BooleanValue: b}}, nil
		case "i":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			n, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("value: bad int64 %q: %w", s, err)
			}
			return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: n}}, nil
		case "d":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			f, err := parseDouble(s)
			if err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: f}}, nil
		case "t":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			ts, err := time.Parse(time.RFC3339Nano, s)
			if err != nil {
				return nil, fmt.Errorf("value: bad timestamp %q: %w", s, err)
			}
			return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(ts)}}, nil
		case "s":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}}, nil
		case "s0":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("value: bad s0 payload: %w", err)
			}
			return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: string(b)}}, nil
		case "y":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			b, err := base64.StdEncoding.DecodeString(s)
			if err != nil {
				return nil, fmt.Errorf("value: bad bytes payload: %w", err)
			}
			return &pb.Value{ValueType: &pb.Value_BytesValue{BytesValue: b}}, nil
		case "r":
			var s string
			if err := json.Unmarshal(payload, &s); err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_ReferenceValue{ReferenceValue: s}}, nil
		case "g":
			var pair []string
			if err := json.Unmarshal(payload, &pair); err != nil {
				return nil, err
			}
			if len(pair) != 2 {
				return nil, fmt.Errorf("value: geopoint needs [lat,lng], got %d elements", len(pair))
			}
			lat, err := parseDouble(pair[0])
			if err != nil {
				return nil, err
			}
			lng, err := parseDouble(pair[1])
			if err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_GeoPointValue{
				GeoPointValue: &latlng.LatLng{Latitude: lat, Longitude: lng},
			}}, nil
		case "a":
			var elems []json.RawMessage
			if err := json.Unmarshal(payload, &elems); err != nil {
				return nil, err
			}
			values := make([]*pb.Value, 0, len(elems))
			for _, el := range elems {
				v, err := decode(el)
				if err != nil {
					return nil, err
				}
				values = append(values, v)
			}
			return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: values}}}, nil
		case "m":
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(payload, &raw); err != nil {
				return nil, err
			}
			fields, err := decodeMapPayload(raw)
			if err != nil {
				return nil, err
			}
			return &pb.Value{ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{Fields: fields}}}, nil
		default:
			return nil, fmt.Errorf("value: unknown tag %q", tag)
		}
	}
	panic("unreachable")
}

func decodeMapPayload(raw map[string]json.RawMessage) (map[string]*pb.Value, error) {
	fields := make(map[string]*pb.Value, len(raw))
	for k, rv := range raw {
		v, err := decode(rv)
		if err != nil {
			return nil, fmt.Errorf("field %q: %w", k, err)
		}
		fields[k] = v
	}
	return fields, nil
}

func formatDouble(f float64) string {
	// strconv handles the specials as "NaN", "+Inf", "-Inf" and preserves "-0".
	// 'g' with precision -1 is the shortest representation that round-trips.
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func parseDouble(s string) (float64, error) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("value: bad double %q: %w", s, err)
	}
	return f, nil
}

func formatTimestamp(ts *timestamppb.Timestamp) (string, error) {
	t := ts.AsTime()
	if t.Before(minTimestamp) || t.After(maxTimestamp) {
		return "", fmt.Errorf("value: timestamp %v outside Firestore range [0001-01-01, 9999-12-31]", t)
	}
	// Firestore is precise to microseconds; extra precision is rounded down.
	return t.Truncate(time.Microsecond).UTC().Format(timestampLayout), nil
}

// CanonicalizeTimestamp returns the value Firestore would actually store for
// a written timestamp (truncated to microseconds). Used so that round-trip
// tests and write results match server behavior.
func CanonicalizeTimestamp(ts *timestamppb.Timestamp) *timestamppb.Timestamp {
	return timestamppb.New(ts.AsTime().Truncate(time.Microsecond))
}
