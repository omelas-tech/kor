package store

import (
	"context"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

// A registered collection's events carry what a Firestore trigger needs: the
// document before AND after this particular write. Create, update (with a
// field transform, which mutates the state in place) and delete each have a
// distinct shape.
func TestFeedImagesCaptureBeforeAndAfter(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.RegisterFeedImages(ctx, []string{"posts"}); err != nil {
		t.Fatal(err)
	}
	doc := idxParent + "/posts/a"

	setDoc(t, s, doc, map[string]*pb.Value{"v": ival(1)}) // create
	// update via merge + increment transform: Apply mutates the write state,
	// which shares its field map with the loaded document.
	if _, _, err := s.Commit(ctx, []*pb.Write{{
		Operation:  &pb.Write_Update{Update: &pb.Document{Name: doc, Fields: map[string]*pb.Value{"w": sval("x")}}},
		UpdateMask: &pb.DocumentMask{FieldPaths: []string{"w"}},
		UpdateTransforms: []*pb.DocumentTransform_FieldTransform{{
			FieldPath:     "v",
			TransformType: &pb.DocumentTransform_FieldTransform_Increment{Increment: ival(10)},
		}},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Commit(ctx, []*pb.Write{{Operation: &pb.Write_Delete{Delete: doc}}}); err != nil {
		t.Fatal(err)
	}

	got, err := s.ReadFeed(ctx, Cursor{}, []string{"posts"}, true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d changes, want 3", len(got))
	}
	for i, c := range got {
		if !c.HasImages {
			t.Fatalf("change %d has no images", i)
		}
	}
	intOf := func(d *pb.Document, f string) int64 { return d.GetFields()[f].GetIntegerValue() }

	create, update, del := got[0], got[1], got[2]
	if create.Before != nil || create.After == nil || intOf(create.After, "v") != 1 {
		t.Errorf("create images = before %v after %v; want nil, v=1", create.Before, create.After)
	}
	if update.Before == nil || intOf(update.Before, "v") != 1 || update.Before.Fields["w"] != nil {
		t.Errorf("update before-image = %v; want the pre-write document (v=1, no w) — an in-place mutation leaked into it", update.Before)
	}
	if update.After == nil || intOf(update.After, "v") != 11 || update.After.Fields["w"].GetStringValue() != "x" {
		t.Errorf("update after-image = %v; want v=11, w=x", update.After)
	}
	if !update.After.CreateTime.AsTime().Equal(create.After.CreateTime.AsTime()) {
		t.Errorf("update kept create_time %v, want %v", update.After.CreateTime, create.After.CreateTime)
	}
	if del.Kind != "delete" || del.Before == nil || intOf(del.Before, "v") != 11 || del.After != nil {
		t.Errorf("delete images = before %v after %v; want v=11, nil", del.Before, del.After)
	}
	if update.After.Name != doc {
		t.Errorf("image name = %q, want %q", update.After.Name, doc)
	}
}

// Unregistered collections keep the data-free feed: no write amplification
// for a bulk import nobody triggers on.
func TestFeedImagesOnlyForRegisteredCollections(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.RegisterFeedImages(ctx, []string{"posts"}); err != nil {
		t.Fatal(err)
	}
	setDoc(t, s, idxParent+"/posts/a", map[string]*pb.Value{"v": ival(1)})
	setDoc(t, s, idxParent+"/other/a", map[string]*pb.Value{"v": ival(1)})

	got, err := s.ReadFeed(ctx, Cursor{}, nil, true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d changes, want 2", len(got))
	}
	for _, c := range got {
		want := c.CollectionID == "posts"
		if c.HasImages != want || (c.After != nil) != want {
			t.Errorf("%s: HasImages=%v After=%v, want images only for posts", c.DocName, c.HasImages, c.After != nil)
		}
	}

	// Unregistering stops capture for later writes.
	if _, err := s.UnregisterFeedImages(ctx, []string{"posts"}); err != nil {
		t.Fatal(err)
	}
	setDoc(t, s, idxParent+"/posts/b", map[string]*pb.Value{"v": ival(1)})
	got, err = s.ReadFeed(ctx, Cursor{XID: got[1].XID, Seq: got[1].Seq}, []string{"posts"}, true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].HasImages {
		t.Errorf("after unregister: %+v, want one change without images", got)
	}
}

// The registry is sorted and the head cursor is where a fresh consumer starts:
// reading from it yields nothing until the next write.
func TestFeedImagesRegistryAndHead(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	if _, err := s.RegisterFeedImages(ctx, []string{"b", "a"}); err != nil {
		t.Fatal(err)
	}
	if got := s.FeedImageCollections(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("registered = %v, want [a b]", got)
	}
	head, err := s.FeedHead(ctx)
	if err != nil || head != (Cursor{}) {
		t.Fatalf("empty feed head = %+v, %v; want zero cursor", head, err)
	}
	setDoc(t, s, idxParent+"/a/x", map[string]*pb.Value{"v": ival(1)})
	head, err = s.FeedHead(ctx)
	if err != nil || head.Seq == 0 {
		t.Fatalf("feed head after a write = %+v, %v", head, err)
	}
	after, err := s.ReadFeed(ctx, head, nil, false, 10)
	if err != nil || len(after) != 0 {
		t.Fatalf("reading from head returned %d events, want 0 (%v)", len(after), err)
	}
}
