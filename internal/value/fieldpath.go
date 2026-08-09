package value

import (
	"fmt"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// FieldPath is a parsed document field path (each element one map key).
type FieldPath []string

// ParseFieldPath parses a Firestore field-path string as used in
// DocumentMask/DocumentTransform: dot-separated segments, where a segment is
// either a simple identifier ([A-Za-z_][A-Za-z0-9_]*) or a backtick-quoted
// string with backslash escapes.
func ParseFieldPath(s string) (FieldPath, error) {
	if s == "" {
		return nil, fmt.Errorf("empty field path")
	}
	var path FieldPath
	i := 0
	for {
		if i >= len(s) {
			return nil, fmt.Errorf("field path %q ends with separator", s)
		}
		var seg strings.Builder
		if s[i] == '`' {
			i++
			closed := false
			for i < len(s) {
				c := s[i]
				if c == '\\' {
					if i+1 >= len(s) {
						return nil, fmt.Errorf("field path %q: dangling escape", s)
					}
					seg.WriteByte(s[i+1])
					i += 2
					continue
				}
				if c == '`' {
					i++
					closed = true
					break
				}
				seg.WriteByte(c)
				i++
			}
			if !closed {
				return nil, fmt.Errorf("field path %q: unterminated quote", s)
			}
		} else {
			start := i
			for i < len(s) && s[i] != '.' {
				if s[i] == '`' {
					return nil, fmt.Errorf("field path %q: unexpected backtick", s)
				}
				i++
			}
			if i == start {
				return nil, fmt.Errorf("field path %q: empty segment", s)
			}
			seg.WriteString(s[start:i])
		}
		path = append(path, seg.String())
		if i == len(s) {
			return path, nil
		}
		if s[i] != '.' {
			return nil, fmt.Errorf("field path %q: expected '.' at offset %d", s, i)
		}
		i++
	}
}

// String renders the path back in Firestore's canonical quoted form.
func (p FieldPath) String() string {
	var b strings.Builder
	for i, seg := range p {
		if i > 0 {
			b.WriteByte('.')
		}
		if isSimpleSegment(seg) {
			b.WriteString(seg)
		} else {
			b.WriteByte('`')
			for j := 0; j < len(seg); j++ {
				if seg[j] == '`' || seg[j] == '\\' {
					b.WriteByte('\\')
				}
				b.WriteByte(seg[j])
			}
			b.WriteByte('`')
		}
	}
	return b.String()
}

func isSimpleSegment(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// GetField resolves a path inside a fields map. Returns (nil, false) if any
// intermediate step is missing or not a map.
func GetField(fields map[string]*pb.Value, path FieldPath) (*pb.Value, bool) {
	cur := fields
	for i, seg := range path {
		v, ok := cur[seg]
		if !ok {
			return nil, false
		}
		if i == len(path)-1 {
			return v, true
		}
		m, ok := v.GetValueType().(*pb.Value_MapValue)
		if !ok {
			return nil, false
		}
		cur = m.MapValue.GetFields()
	}
	return nil, false
}

// SetField writes a value at path, creating intermediate maps and replacing
// non-map intermediates (Firestore update semantics for dotted paths).
func SetField(fields map[string]*pb.Value, path FieldPath, v *pb.Value) {
	cur := fields
	for i, seg := range path {
		if i == len(path)-1 {
			cur[seg] = v
			return
		}
		next, ok := cur[seg]
		var m *pb.Value_MapValue
		if ok {
			m, ok = next.GetValueType().(*pb.Value_MapValue)
		}
		if !ok || m.MapValue.Fields == nil {
			fresh := &pb.MapValue{Fields: map[string]*pb.Value{}}
			if ok && m.MapValue != nil && m.MapValue.Fields == nil {
				m.MapValue.Fields = fresh.Fields
			} else {
				cur[seg] = &pb.Value{ValueType: &pb.Value_MapValue{MapValue: fresh}}
				m = cur[seg].GetValueType().(*pb.Value_MapValue)
			}
		}
		cur = m.MapValue.Fields
	}
}

// DeleteField removes the value at path if present. Missing intermediates
// are a no-op. Empty intermediate maps are left in place (matches Firestore:
// deleting a.b does not delete a).
func DeleteField(fields map[string]*pb.Value, path FieldPath) {
	cur := fields
	for i, seg := range path {
		if i == len(path)-1 {
			delete(cur, seg)
			return
		}
		next, ok := cur[seg]
		if !ok {
			return
		}
		m, ok := next.GetValueType().(*pb.Value_MapValue)
		if !ok || m.MapValue.GetFields() == nil {
			return
		}
		cur = m.MapValue.GetFields()
	}
}
