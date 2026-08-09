package store

import (
	"context"
	"testing"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/omelas-tech/kor/internal/pgtest"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

const docName = "projects/p/databases/(default)/documents/users/alice"

func openStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), pgtest.DSN(t))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func setWrite(name string, fields map[string]*pb.Value) *pb.Write {
	return &pb.Write{Operation: &pb.Write_Update{Update: &pb.Document{Name: name, Fields: fields}}}
}

func str(s string) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}}
}
func integer(i int64) *pb.Value {
	return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: i}}
}

func TestSetGetDelete(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	_, commitTime, err := s.Commit(ctx, []*pb.Write{
		setWrite(docName, map[string]*pb.Value{"name": str("Alice"), "age": integer(30)}),
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	docs, err := s.GetDocuments(ctx, []string{docName})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	doc := docs[docName]
	if doc == nil {
		t.Fatal("document missing after set")
	}
	if got := doc.Fields["name"].GetStringValue(); got != "Alice" {
		t.Errorf("name = %q", got)
	}
	if got := doc.Fields["age"].GetIntegerValue(); got != 30 {
		t.Errorf("age = %d", got)
	}
	if !doc.CreateTime.Equal(commitTime) || !doc.UpdateTime.Equal(commitTime) {
		t.Errorf("times: create=%v update=%v commit=%v", doc.CreateTime, doc.UpdateTime, commitTime)
	}

	// Overwrite must replace, not merge, and keep create_time.
	time.Sleep(2 * time.Millisecond)
	_, commit2, err := s.Commit(ctx, []*pb.Write{
		setWrite(docName, map[string]*pb.Value{"name": str("Alice v2")}),
	})
	if err != nil {
		t.Fatalf("commit2: %v", err)
	}
	docs, _ = s.GetDocuments(ctx, []string{docName})
	doc = docs[docName]
	if _, stillThere := doc.Fields["age"]; stillThere {
		t.Error("full set should have removed 'age'")
	}
	if !doc.CreateTime.Equal(commitTime) {
		t.Errorf("create_time changed on overwrite: %v vs %v", doc.CreateTime, commitTime)
	}
	if !doc.UpdateTime.Equal(commit2) {
		t.Errorf("update_time not bumped: %v vs %v", doc.UpdateTime, commit2)
	}

	// Delete.
	if _, _, err := s.Commit(ctx, []*pb.Write{
		{Operation: &pb.Write_Delete{Delete: docName}},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	docs, _ = s.GetDocuments(ctx, []string{docName})
	if docs[docName] != nil {
		t.Error("document still present after delete")
	}
}

func TestMergeAndUpdateMask(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	if _, _, err := s.Commit(ctx, []*pb.Write{
		setWrite(docName, map[string]*pb.Value{"a": integer(1), "b": integer(2)}),
	}); err != nil {
		t.Fatal(err)
	}

	// MergeAll-style: mask lists only "b" and a nested path; "a" survives.
	w := setWrite(docName, map[string]*pb.Value{
		"b": integer(20),
		"nested": {ValueType: &pb.Value_MapValue{MapValue: &pb.MapValue{
			Fields: map[string]*pb.Value{"x": str("y")},
		}}},
	})
	w.UpdateMask = &pb.DocumentMask{FieldPaths: []string{"b", "nested.x"}}
	if _, _, err := s.Commit(ctx, []*pb.Write{w}); err != nil {
		t.Fatal(err)
	}

	docs, _ := s.GetDocuments(ctx, []string{docName})
	doc := docs[docName]
	if doc.Fields["a"].GetIntegerValue() != 1 {
		t.Error("merge lost unmasked field 'a'")
	}
	if doc.Fields["b"].GetIntegerValue() != 20 {
		t.Error("merge did not apply 'b'")
	}
	if doc.Fields["nested"].GetMapValue().GetFields()["x"].GetStringValue() != "y" {
		t.Error("merge did not create nested.x")
	}

	// Mask path absent from update doc = field delete.
	w2 := setWrite(docName, map[string]*pb.Value{})
	w2.UpdateMask = &pb.DocumentMask{FieldPaths: []string{"b"}}
	if _, _, err := s.Commit(ctx, []*pb.Write{w2}); err != nil {
		t.Fatal(err)
	}
	docs, _ = s.GetDocuments(ctx, []string{docName})
	if _, ok := docs[docName].Fields["b"]; ok {
		t.Error("masked delete did not remove 'b'")
	}
}

func TestPreconditionsAndTransforms(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	// Update with exists:true on a missing doc must be NotFound.
	w := setWrite(docName, map[string]*pb.Value{"x": integer(1)})
	w.CurrentDocument = &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: true}}
	if _, _, err := s.Commit(ctx, []*pb.Write{w}); status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}

	// Create, then increment + serverTimestamp + arrayUnion in one write.
	if _, _, err := s.Commit(ctx, []*pb.Write{
		setWrite(docName, map[string]*pb.Value{"count": integer(10)}),
	}); err != nil {
		t.Fatal(err)
	}
	w2 := setWrite(docName, map[string]*pb.Value{"count": integer(10)})
	w2.UpdateMask = &pb.DocumentMask{FieldPaths: []string{}} // pure transform carrier
	w2.UpdateTransforms = []*pb.DocumentTransform_FieldTransform{
		{FieldPath: "count", TransformType: &pb.DocumentTransform_FieldTransform_Increment{Increment: integer(5)}},
		{FieldPath: "updatedAt", TransformType: &pb.DocumentTransform_FieldTransform_SetToServerValue{
			SetToServerValue: pb.DocumentTransform_FieldTransform_REQUEST_TIME}},
		{FieldPath: "tags", TransformType: &pb.DocumentTransform_FieldTransform_AppendMissingElements{
			AppendMissingElements: &pb.ArrayValue{Values: []*pb.Value{str("a"), str("b"), str("a")}}}},
	}
	results, commitTime, err := s.Commit(ctx, []*pb.Write{w2})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(results[0].TransformResults); n != 3 {
		t.Fatalf("want 3 transform results, got %d", n)
	}
	if got := results[0].TransformResults[0].GetIntegerValue(); got != 15 {
		t.Errorf("increment result = %d, want 15", got)
	}
	if got := results[0].TransformResults[1].GetTimestampValue().AsTime(); !got.Equal(commitTime) {
		t.Errorf("serverTimestamp result = %v, want %v", got, commitTime)
	}

	docs, _ := s.GetDocuments(ctx, []string{docName})
	doc := docs[docName]
	if doc.Fields["count"].GetIntegerValue() != 15 {
		t.Errorf("count = %v", doc.Fields["count"])
	}
	tags := doc.Fields["tags"].GetArrayValue().GetValues()
	if len(tags) != 2 || tags[0].GetStringValue() != "a" || tags[1].GetStringValue() != "b" {
		t.Errorf("tags = %v", tags)
	}

	// ArrayRemove.
	w3 := &pb.Write{
		Operation: &pb.Write_Transform{Transform: &pb.DocumentTransform{
			Document: docName,
			FieldTransforms: []*pb.DocumentTransform_FieldTransform{
				{FieldPath: "tags", TransformType: &pb.DocumentTransform_FieldTransform_RemoveAllFromArray{
					RemoveAllFromArray: &pb.ArrayValue{Values: []*pb.Value{str("a")}}}},
			},
		}},
	}
	if _, _, err := s.Commit(ctx, []*pb.Write{w3}); err != nil {
		t.Fatal(err)
	}
	docs, _ = s.GetDocuments(ctx, []string{docName})
	tags = docs[docName].Fields["tags"].GetArrayValue().GetValues()
	if len(tags) != 1 || tags[0].GetStringValue() != "b" {
		t.Errorf("tags after remove = %v", tags)
	}
}

func TestSameDocTwiceInOneCommit(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	// Old-SDK shape: update write followed by a standalone transform write
	// for the same document, in one commit.
	_, _, err := s.Commit(ctx, []*pb.Write{
		setWrite(docName, map[string]*pb.Value{"n": integer(1)}),
		{Operation: &pb.Write_Transform{Transform: &pb.DocumentTransform{
			Document: docName,
			FieldTransforms: []*pb.DocumentTransform_FieldTransform{
				{FieldPath: "n", TransformType: &pb.DocumentTransform_FieldTransform_Increment{Increment: integer(41)}},
			},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	docs, _ := s.GetDocuments(ctx, []string{docName})
	if got := docs[docName].Fields["n"].GetIntegerValue(); got != 42 {
		t.Errorf("n = %d, want 42 (transform must see the update in the same commit)", got)
	}
}

func TestParseDocumentName(t *testing.T) {
	p, err := ParseDocumentName("projects/p/databases/(default)/documents/newsfeed/u1/items/i1")
	if err != nil {
		t.Fatal(err)
	}
	if p.ParentPath != "projects/p/databases/(default)/documents/newsfeed/u1" ||
		p.CollectionID != "items" || p.DocID != "i1" {
		t.Errorf("parsed = %+v", p)
	}
	if _, err := ParseDocumentName("projects/p/databases/(default)/documents/users"); err == nil {
		t.Error("collection path should be rejected as a document name")
	}
	if _, err := ParseDocumentName("bogus"); err == nil {
		t.Error("garbage should be rejected")
	}
}
