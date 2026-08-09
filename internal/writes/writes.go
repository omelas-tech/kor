// Package writes implements the semantics of google.firestore.v1.Write —
// document set/merge/update/delete, field transforms, and preconditions —
// as pure operations over in-memory document state. The store layer handles
// persistence and locking; this package is the single place where Firestore
// write behavior is defined.
package writes

import (
	"math"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/omelas-tech/kor/internal/value"
)

// DocState is the mutable in-memory state of one document during a commit.
type DocState struct {
	Name       string
	Exists     bool
	Fields     map[string]*pb.Value
	CreateTime time.Time
	UpdateTime time.Time
	// Dirty is set when a write changed this document.
	Dirty bool
}

// TargetName returns the document a write addresses.
func TargetName(w *pb.Write) (string, error) {
	switch op := w.GetOperation().(type) {
	case *pb.Write_Update:
		if op.Update.GetName() == "" {
			return "", status.Error(codes.InvalidArgument, "update write missing document name")
		}
		return op.Update.GetName(), nil
	case *pb.Write_Delete:
		if op.Delete == "" {
			return "", status.Error(codes.InvalidArgument, "delete write missing document name")
		}
		return op.Delete, nil
	case *pb.Write_Transform:
		if op.Transform.GetDocument() == "" {
			return "", status.Error(codes.InvalidArgument, "transform write missing document name")
		}
		return op.Transform.GetDocument(), nil
	default:
		return "", status.Errorf(codes.InvalidArgument, "write has no operation")
	}
}

// Apply executes one write against state at the given commit time and
// returns its WriteResult. State is mutated in place.
func Apply(state *DocState, w *pb.Write, commitTime time.Time) (*pb.WriteResult, error) {
	if err := checkPrecondition(state, w.GetCurrentDocument()); err != nil {
		return nil, err
	}

	var transformResults []*pb.Value

	switch op := w.GetOperation().(type) {
	case *pb.Write_Update:
		if w.GetUpdateMask() == nil {
			// Full set: replace all fields.
			state.Fields = cloneFields(op.Update.GetFields())
		} else {
			if !state.Exists || state.Fields == nil {
				state.Fields = map[string]*pb.Value{}
			}
			for _, p := range w.GetUpdateMask().GetFieldPaths() {
				path, err := value.ParseFieldPath(p)
				if err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "bad field path %q: %v", p, err)
				}
				if v, ok := value.GetField(op.Update.GetFields(), path); ok {
					value.SetField(state.Fields, path, v)
				} else {
					value.DeleteField(state.Fields, path)
				}
			}
		}
		if !state.Exists {
			state.Exists = true
			state.CreateTime = commitTime
		}
		state.UpdateTime = commitTime
		state.Dirty = true

	case *pb.Write_Delete:
		state.Exists = false
		state.Fields = nil
		state.Dirty = true

	case *pb.Write_Transform:
		// Legacy standalone transform write (older SDKs pair it with an
		// update in the same commit). Creates the document if missing.
		if !state.Exists {
			state.Exists = true
			state.Fields = map[string]*pb.Value{}
			state.CreateTime = commitTime
		}
		results, err := applyTransforms(state, op.Transform.GetFieldTransforms(), commitTime)
		if err != nil {
			return nil, err
		}
		transformResults = results
		state.UpdateTime = commitTime
		state.Dirty = true

	default:
		return nil, status.Error(codes.InvalidArgument, "write has no operation")
	}

	// update_transforms run after the update operation, in the same write.
	if n := len(w.GetUpdateTransforms()); n > 0 {
		if _, isDelete := w.GetOperation().(*pb.Write_Delete); isDelete {
			return nil, status.Error(codes.InvalidArgument, "delete write cannot carry update_transforms")
		}
		results, err := applyTransforms(state, w.GetUpdateTransforms(), commitTime)
		if err != nil {
			return nil, err
		}
		transformResults = append(transformResults, results...)
		state.UpdateTime = commitTime
	}

	return &pb.WriteResult{
		UpdateTime:       timestamppb.New(commitTime),
		TransformResults: transformResults,
	}, nil
}

func checkPrecondition(state *DocState, pre *pb.Precondition) error {
	if pre == nil {
		return nil
	}
	switch c := pre.GetConditionType().(type) {
	case *pb.Precondition_Exists:
		if c.Exists && !state.Exists {
			return status.Errorf(codes.NotFound, "no document to update: %s", state.Name)
		}
		if !c.Exists && state.Exists {
			return status.Errorf(codes.AlreadyExists, "document already exists: %s", state.Name)
		}
	case *pb.Precondition_UpdateTime:
		if !state.Exists {
			return status.Errorf(codes.FailedPrecondition, "document %s does not exist", state.Name)
		}
		want := c.UpdateTime.AsTime().Truncate(time.Microsecond)
		if !state.UpdateTime.Truncate(time.Microsecond).Equal(want) {
			return status.Errorf(codes.FailedPrecondition,
				"stored update time %v does not match required %v", state.UpdateTime, want)
		}
	}
	return nil
}

func applyTransforms(state *DocState, transforms []*pb.DocumentTransform_FieldTransform, commitTime time.Time) ([]*pb.Value, error) {
	results := make([]*pb.Value, 0, len(transforms))
	for _, ft := range transforms {
		path, err := value.ParseFieldPath(ft.GetFieldPath())
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "bad transform field path %q: %v", ft.GetFieldPath(), err)
		}
		cur, _ := value.GetField(state.Fields, path)

		switch tt := ft.GetTransformType().(type) {
		case *pb.DocumentTransform_FieldTransform_SetToServerValue:
			if tt.SetToServerValue != pb.DocumentTransform_FieldTransform_REQUEST_TIME {
				return nil, status.Errorf(codes.InvalidArgument, "unknown server value %v", tt.SetToServerValue)
			}
			v := tsValue(commitTime)
			value.SetField(state.Fields, path, v)
			results = append(results, v)

		case *pb.DocumentTransform_FieldTransform_Increment:
			v := numericTransform(cur, tt.Increment, func(a, b int64) int64 {
				s, ok := addInt64(a, b)
				if !ok {
					if (a > 0) == (b > 0) && a > 0 {
						return math.MaxInt64
					}
					return math.MinInt64
				}
				return s
			}, func(a, b float64) float64 { return a + b })
			value.SetField(state.Fields, path, v)
			results = append(results, v)

		case *pb.DocumentTransform_FieldTransform_Maximum:
			v := extremumTransform(cur, tt.Maximum, +1)
			value.SetField(state.Fields, path, v)
			results = append(results, v)

		case *pb.DocumentTransform_FieldTransform_Minimum:
			v := extremumTransform(cur, tt.Minimum, -1)
			value.SetField(state.Fields, path, v)
			results = append(results, v)

		case *pb.DocumentTransform_FieldTransform_AppendMissingElements:
			existing := arrayElements(cur)
			for _, el := range tt.AppendMissingElements.GetValues() {
				if !containsValue(existing, el) {
					existing = append(existing, el)
				}
			}
			value.SetField(state.Fields, path, arrValue(existing))
			results = append(results, nullValue())

		case *pb.DocumentTransform_FieldTransform_RemoveAllFromArray:
			existing := arrayElements(cur)
			kept := existing[:0]
			for _, el := range existing {
				if !containsValue(tt.RemoveAllFromArray.GetValues(), el) {
					kept = append(kept, el)
				}
			}
			value.SetField(state.Fields, path, arrValue(kept))
			results = append(results, nullValue())

		default:
			return nil, status.Errorf(codes.InvalidArgument, "unsupported field transform %T", tt)
		}
	}
	return results, nil
}

// numericTransform implements increment: if the current field is not a
// number (or missing), the operand becomes the value; if either side is a
// double, arithmetic is double; integer overflow saturates.
func numericTransform(cur, operand *pb.Value, intOp func(a, b int64) int64, floatOp func(a, b float64) float64) *pb.Value {
	ci, curIsInt := cur.GetValueType().(*pb.Value_IntegerValue)
	cd, curIsDouble := cur.GetValueType().(*pb.Value_DoubleValue)
	oi, opIsInt := operand.GetValueType().(*pb.Value_IntegerValue)
	od, opIsDouble := operand.GetValueType().(*pb.Value_DoubleValue)

	if !opIsInt && !opIsDouble {
		// Invalid operand; Firestore rejects this at write validation.
		return operand
	}
	if !curIsInt && !curIsDouble {
		return operand
	}
	if curIsInt && opIsInt {
		return intValue(intOp(ci.IntegerValue, oi.IntegerValue))
	}
	var a, b float64
	if curIsInt {
		a = float64(ci.IntegerValue)
	} else {
		a = cd.DoubleValue
	}
	if opIsInt {
		b = float64(oi.IntegerValue)
	} else {
		b = od.DoubleValue
	}
	return doubleValue(floatOp(a, b))
}

// extremumTransform implements maximum (dir=+1) / minimum (dir=-1) per the
// proto contract: non-numbers/missing take the operand; NaN wins; on ties
// the stored value is kept.
func extremumTransform(cur, operand *pb.Value, dir int) *pb.Value {
	if !value.IsNumeric(cur) {
		return operand
	}
	if isNaNValue(cur) || isNaNValue(operand) {
		if isNaNValue(cur) {
			return cur
		}
		return doubleValue(math.NaN())
	}
	c := value.Compare(operand, cur)
	if c*dir > 0 {
		return operand
	}
	return cur
}

func isNaNValue(v *pb.Value) bool {
	d, ok := v.GetValueType().(*pb.Value_DoubleValue)
	return ok && math.IsNaN(d.DoubleValue)
}

func addInt64(a, b int64) (int64, bool) {
	s := a + b
	if (a > 0 && b > 0 && s < 0) || (a < 0 && b < 0 && s >= 0) {
		return 0, false
	}
	return s, true
}

func arrayElements(v *pb.Value) []*pb.Value {
	if av, ok := v.GetValueType().(*pb.Value_ArrayValue); ok {
		return append([]*pb.Value(nil), av.ArrayValue.GetValues()...)
	}
	return nil
}

func containsValue(list []*pb.Value, v *pb.Value) bool {
	for _, el := range list {
		if value.Equal(el, v) {
			return true
		}
	}
	return false
}

func cloneFields(fields map[string]*pb.Value) map[string]*pb.Value {
	out := make(map[string]*pb.Value, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}

func tsValue(t time.Time) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(t)}}
}
func intValue(i int64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: i}}
}
func doubleValue(d float64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_DoubleValue{DoubleValue: d}}
}
func nullValue() *pb.Value {
	return &pb.Value{ValueType: &pb.Value_NullValue{}}
}
func arrValue(vs []*pb.Value) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_ArrayValue{ArrayValue: &pb.ArrayValue{Values: vs}}}
}
