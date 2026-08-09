package value

import (
	"encoding/json"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// ContainmentJSON builds a jsonb containment probe (for `data @> $1`) that
// matches documents whose field at path equals v. Nested paths wrap in the
// map tag at each level: path a.b -> {"a":{"m":{"b":<tagged v>}}}.
func ContainmentJSON(path FieldPath, v *pb.Value) ([]byte, error) {
	tagged, err := encode(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(nestPath(path, tagged))
}

// ArrayContainmentJSON builds a probe matching documents whose ARRAY field at
// path contains element el: path tags -> {"tags":{"a":[<tagged el>]}}.
// jsonb containment on arrays is "contains these elements".
func ArrayContainmentJSON(path FieldPath, el *pb.Value) ([]byte, error) {
	tagged, err := encode(el)
	if err != nil {
		return nil, err
	}
	return json.Marshal(nestPath(path, map[string]any{"a": []any{tagged}}))
}

func nestPath(path FieldPath, leaf any) any {
	node := leaf
	for i := len(path) - 1; i >= 0; i-- {
		wrapped := map[string]any{path[i]: node}
		if i > 0 {
			node = map[string]any{"m": wrapped}
		} else {
			node = wrapped
		}
	}
	return node
}
