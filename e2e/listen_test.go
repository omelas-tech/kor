package e2e

import (
	"context"
	"os"
	"sort"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

const docsRoot = "projects/kor-e2e/databases/(default)/documents"

type change struct {
	kind firestore.DocumentChangeKind
	id   string
}

func nextQuery(t *testing.T, it *firestore.QuerySnapshotIterator) (ids []string, changes []change) {
	t.Helper()
	snap, err := it.Next()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	docs, err := snap.Documents.GetAll()
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range docs {
		ids = append(ids, d.Ref.ID)
	}
	for _, c := range snap.Changes {
		changes = append(changes, change{c.Kind, c.Doc.Ref.ID})
	}
	return ids, changes
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestListenQueryLifecycle(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msgs := client.Collection("chats").Doc("c1").Collection("messages")
	if _, err := msgs.Doc("m1").Set(ctx, map[string]any{"room": "a", "n": 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := msgs.Doc("other").Set(ctx, map[string]any{"room": "b", "n": 2}); err != nil {
		t.Fatal(err)
	}
	// A same-named collection under another parent must not leak in.
	if _, err := client.Collection("chats").Doc("c2").Collection("messages").Doc("x").Set(ctx, map[string]any{"room": "a", "n": 9}); err != nil {
		t.Fatal(err)
	}

	it := msgs.Where("room", "==", "a").OrderBy("n", firestore.Asc).Snapshots(ctx)
	defer it.Stop()

	if ids, _ := nextQuery(t, it); !eq(ids, []string{"m1"}) {
		t.Fatalf("initial = %v, want [m1]", ids)
	}

	// Insert a match.
	if _, err := msgs.Doc("m2").Set(ctx, map[string]any{"room": "a", "n": 3}); err != nil {
		t.Fatal(err)
	}
	ids, ch := nextQuery(t, it)
	if !eq(ids, []string{"m1", "m2"}) || len(ch) != 1 || ch[0] != (change{firestore.DocumentAdded, "m2"}) {
		t.Fatalf("after add: ids=%v changes=%v", ids, ch)
	}

	// A write outside the query produces no snapshot; the next one carries
	// only the modification that follows it.
	if _, err := msgs.Doc("other").Set(ctx, map[string]any{"room": "b", "n": 5}); err != nil {
		t.Fatal(err)
	}
	if _, err := msgs.Doc("m1").Update(ctx, []firestore.Update{{Path: "n", Value: 10}}); err != nil {
		t.Fatal(err)
	}
	ids, ch = nextQuery(t, it)
	if !eq(ids, []string{"m2", "m1"}) || len(ch) != 1 || ch[0] != (change{firestore.DocumentModified, "m1"}) {
		t.Fatalf("after modify: ids=%v changes=%v", ids, ch)
	}

	// Leaving the filter is a removal, not a delete.
	if _, err := msgs.Doc("m2").Update(ctx, []firestore.Update{{Path: "room", Value: "b"}}); err != nil {
		t.Fatal(err)
	}
	ids, ch = nextQuery(t, it)
	if !eq(ids, []string{"m1"}) || len(ch) != 1 || ch[0] != (change{firestore.DocumentRemoved, "m2"}) {
		t.Fatalf("after leave: ids=%v changes=%v", ids, ch)
	}

	// Coming back in, then a delete.
	if _, err := msgs.Doc("other").Update(ctx, []firestore.Update{{Path: "room", Value: "a"}}); err != nil {
		t.Fatal(err)
	}
	if ids, _ = nextQuery(t, it); !eq(ids, []string{"other", "m1"}) {
		t.Fatalf("after re-enter: %v", ids)
	}
	if _, err := msgs.Doc("m1").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	ids, ch = nextQuery(t, it)
	if !eq(ids, []string{"other"}) || len(ch) != 1 || ch[0] != (change{firestore.DocumentRemoved, "m1"}) {
		t.Fatalf("after delete: ids=%v changes=%v", ids, ch)
	}
}

func TestListenDocument(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	ref := client.Collection("user_presence").Doc("u1")
	it := ref.Snapshots(ctx)
	defer it.Stop()

	snap, err := it.Next()
	if err != nil || snap.Exists() {
		t.Fatalf("initial: exists=%v err=%v", snap != nil && snap.Exists(), err)
	}
	if _, err := ref.Set(ctx, map[string]any{"online": true}); err != nil {
		t.Fatal(err)
	}
	if snap, err = it.Next(); err != nil || !snap.Exists() || snap.Data()["online"] != true {
		t.Fatalf("after set: %v %v", snap, err)
	}
	if _, err := ref.Set(ctx, map[string]any{"online": false}); err != nil {
		t.Fatal(err)
	}
	if snap, err = it.Next(); err != nil || snap.Data()["online"] != false {
		t.Fatalf("after update: %v %v", snap.Data(), err)
	}
	if _, err := ref.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if snap, err = it.Next(); err != nil || snap.Exists() {
		t.Fatalf("after delete: exists=%v err=%v", snap.Exists(), err)
	}
}

// A limit window is decided by other documents: deleting the top one must
// pull the next one in, and a new top one must push the last one out.
func TestListenLimitWindow(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	col := client.Collection("comments")
	for id, n := range map[string]int{"a": 1, "b": 2, "c": 3} {
		if _, err := col.Doc(id).Set(ctx, map[string]any{"n": n}); err != nil {
			t.Fatal(err)
		}
	}
	it := col.OrderBy("n", firestore.Asc).Limit(2).Snapshots(ctx)
	defer it.Stop()
	if ids, _ := nextQuery(t, it); !eq(ids, []string{"a", "b"}) {
		t.Fatalf("initial = %v", ids)
	}
	if _, err := col.Doc("a").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if ids, _ := nextQuery(t, it); !eq(ids, []string{"b", "c"}) {
		t.Fatalf("after delete = %v, want [b c]", ids)
	}
	if _, err := col.Doc("z").Set(ctx, map[string]any{"n": 0}); err != nil {
		t.Fatal(err)
	}
	ids, ch := nextQuery(t, it)
	if !eq(ids, []string{"z", "b"}) {
		t.Fatalf("after insert = %v, want [z b]", ids)
	}
	sort.Slice(ch, func(i, j int) bool { return ch[i].id < ch[j].id })
	if len(ch) != 2 || ch[0] != (change{firestore.DocumentRemoved, "c"}) || ch[1] != (change{firestore.DocumentAdded, "z"}) {
		t.Fatalf("changes = %v", ch)
	}
}

func rawClient(t *testing.T) pb.FirestoreClient {
	t.Helper()
	conn, err := grpc.NewClient(os.Getenv("FIRESTORE_EMULATOR_HOST"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewFirestoreClient(conn)
}

func collQuery(coll string) *pb.Target_Query {
	return &pb.Target_Query{Query: &pb.Target_QueryTarget{
		Parent: docsRoot,
		QueryType: &pb.Target_QueryTarget_StructuredQuery{StructuredQuery: &pb.StructuredQuery{
			From: []*pb.StructuredQuery_CollectionSelector{{CollectionId: coll}},
		}},
	}}
}

// recvUntilConsistent collects responses up to the next global NO_CHANGE.
func recvUntilConsistent(t *testing.T, s pb.Firestore_ListenClient) []*pb.ListenResponse {
	t.Helper()
	var out []*pb.ListenResponse
	for {
		r, err := s.Recv()
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		out = append(out, r)
		if tc := r.GetTargetChange(); tc != nil && tc.TargetChangeType == pb.TargetChange_NO_CHANGE && len(tc.TargetIds) == 0 {
			if tc.ReadTime == nil {
				t.Fatal("global NO_CHANGE without read_time")
			}
			return out
		}
	}
}

// Mobile SDKs multiplex every listener of a client on one stream, by
// target id, and add/remove targets as screens come and go.
func TestListenMultiTargetRaw(t *testing.T) {
	startKord(t)
	client := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := client.Collection("newsfeed").Doc("n1").Set(ctx, map[string]any{"k": 1}); err != nil {
		t.Fatal(err)
	}
	s, err := rawClient(t).Listen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	add := func(id int32, tg any, token []byte) {
		target := &pb.Target{TargetId: id, ResumeType: nil}
		switch v := tg.(type) {
		case *pb.Target_Query:
			target.TargetType = v
		case *pb.Target_Documents:
			target.TargetType = v
		}
		if token != nil {
			target.ResumeType = &pb.Target_ResumeToken{ResumeToken: token}
		}
		if err := s.Send(&pb.ListenRequest{Database: "projects/kor-e2e/databases/(default)",
			TargetChange: &pb.ListenRequest_AddTarget{AddTarget: target}}); err != nil {
			t.Fatal(err)
		}
	}

	add(2, collQuery("newsfeed"), nil)
	got := recvUntilConsistent(t, s)
	kinds := []string{}
	for _, r := range got {
		switch {
		case r.GetTargetChange() != nil:
			kinds = append(kinds, r.GetTargetChange().TargetChangeType.String())
		case r.GetDocumentChange() != nil:
			kinds = append(kinds, "DOC")
		}
	}
	if !eq(kinds, []string{"ADD", "DOC", "CURRENT", "NO_CHANGE"}) {
		t.Fatalf("initial sequence = %v", kinds)
	}
	var token []byte
	for _, r := range got {
		if tc := r.GetTargetChange(); tc != nil && tc.TargetChangeType == pb.TargetChange_CURRENT {
			token = tc.ResumeToken
		}
	}
	if len(token) == 0 {
		t.Fatal("CURRENT carried no resume token")
	}

	// A second target on the same stream: a missing document.
	add(4, &pb.Target_Documents{Documents: &pb.Target_DocumentsTarget{Documents: []string{docsRoot + "/user_presence/ghost"}}}, nil)
	recvUntilConsistent(t, s)

	// One write fans out only to the target it affects.
	if _, err := client.Collection("user_presence").Doc("ghost").Set(ctx, map[string]any{"online": true}); err != nil {
		t.Fatal(err)
	}
	for _, r := range recvUntilConsistent(t, s) {
		if dc := r.GetDocumentChange(); dc != nil && !eq32(dc.TargetIds, 4) {
			t.Fatalf("presence write delivered to targets %v", dc.TargetIds)
		}
	}

	// A bad query is reported on its own target; the stream survives.
	if err := s.Send(&pb.ListenRequest{TargetChange: &pb.ListenRequest_AddTarget{AddTarget: &pb.Target{TargetId: 6,
		TargetType: &pb.Target_Query{Query: &pb.Target_QueryTarget{Parent: docsRoot,
			QueryType: &pb.Target_QueryTarget_StructuredQuery{StructuredQuery: &pb.StructuredQuery{}}}}}}}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if tc := r.GetTargetChange(); tc == nil || tc.TargetChangeType != pb.TargetChange_REMOVE || tc.Cause.GetCode() != int32(codes.InvalidArgument) {
		t.Fatalf("bad target answered with %v", r)
	}

	// Removing a target is acknowledged and stops its deliveries.
	if err := s.Send(&pb.ListenRequest{TargetChange: &pb.ListenRequest_RemoveTarget{RemoveTarget: 4}}); err != nil {
		t.Fatal(err)
	}
	if r, err = s.Recv(); err != nil || r.GetTargetChange().GetTargetChangeType() != pb.TargetChange_REMOVE {
		t.Fatalf("remove ack = %v %v", r, err)
	}

	// Resuming with a token resets the target, then resends the full result.
	add(8, collQuery("newsfeed"), token)
	kinds = nil
	for _, r := range recvUntilConsistent(t, s) {
		if tc := r.GetTargetChange(); tc != nil {
			kinds = append(kinds, tc.TargetChangeType.String())
		} else if r.GetDocumentChange() != nil {
			kinds = append(kinds, "DOC")
		}
	}
	if !eq(kinds, []string{"ADD", "RESET", "DOC", "CURRENT", "NO_CHANGE"}) {
		t.Fatalf("resume sequence = %v", kinds)
	}
}

func eq32(ids []int32, want int32) bool { return len(ids) == 1 && ids[0] == want }

func TestWriteStream(t *testing.T) {
	startKord(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := rawClient(t).Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Send(&pb.WriteRequest{Database: "projects/kor-e2e/databases/(default)"}); err != nil {
		t.Fatal(err)
	}
	hs, err := s.Recv()
	if err != nil || hs.StreamId == "" || len(hs.StreamToken) == 0 || len(hs.WriteResults) != 0 {
		t.Fatalf("handshake = %v %v", hs, err)
	}
	name := docsRoot + "/chats/c9"
	if err := s.Send(&pb.WriteRequest{StreamToken: hs.StreamToken, Writes: []*pb.Write{{
		Operation: &pb.Write_Update{Update: &pb.Document{Name: name, Fields: map[string]*pb.Value{
			"n": {ValueType: &pb.Value_IntegerValue{IntegerValue: 7}}}}},
	}}}); err != nil {
		t.Fatal(err)
	}
	wr, err := s.Recv()
	if err != nil || len(wr.WriteResults) != 1 || wr.CommitTime == nil || string(wr.StreamToken) == string(hs.StreamToken) {
		t.Fatalf("write response = %v %v", wr, err)
	}
	got, err := rawClient(t).GetDocument(ctx, &pb.GetDocumentRequest{Name: name})
	if err != nil || got.Fields["n"].GetIntegerValue() != 7 {
		t.Fatalf("written doc = %v %v", got, err)
	}

	// An empty request just acknowledges a token.
	if err := s.Send(&pb.WriteRequest{StreamToken: wr.StreamToken}); err != nil {
		t.Fatal(err)
	}
	if ack, err := s.Recv(); err != nil || len(ack.WriteResults) != 0 || len(ack.StreamToken) == 0 {
		t.Fatalf("ack = %v %v", ack, err)
	}

	// A failing batch ends the stream with its error.
	if err := s.Send(&pb.WriteRequest{Writes: []*pb.Write{{
		Operation:       &pb.Write_Update{Update: &pb.Document{Name: docsRoot + "/chats/none"}},
		CurrentDocument: &pb.Precondition{ConditionType: &pb.Precondition_Exists{Exists: true}},
	}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Recv(); err == nil {
		t.Fatal("precondition failure did not end the stream")
	}
}
