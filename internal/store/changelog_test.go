package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
)

func TestChangelogRecordsWritesAndDeletes(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	setDoc(t, s, idxParent+"/posts/a", map[string]*pb.Value{"v": ival(1)})
	setDoc(t, s, idxParent+"/posts/b", map[string]*pb.Value{"v": ival(2)})
	if _, _, err := s.applyCommit(ctx, []*pb.Write{
		{Operation: &pb.Write_Delete{Delete: idxParent + "/posts/a"}},
	}, nil, nil); err != nil {
		t.Fatal(err)
	}

	changes, err := s.Changes(ctx, Cursor{}, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 3 {
		t.Fatalf("got %d changes, want 3: %+v", len(changes), changes)
	}
	if changes[2].Kind != "delete" || changes[2].DocName != idxParent+"/posts/a" {
		t.Errorf("last change = %+v, want the delete of posts/a", changes[2])
	}
	for _, c := range changes {
		if c.CollectionID != "posts" {
			t.Errorf("change %+v has the wrong collection", c)
		}
	}
}

// The bug this design exists to prevent.
//
// Sequence numbers are handed out at INSERT, but rows become visible at COMMIT,
// and those orders differ. A reader tailing on seq alone sees the later
// transaction first, advances its cursor past the earlier one, and never sees
// it — silently, forever. This holds one transaction open while another commits
// and requires that the committed event stays BELOW the low-water mark until
// the older transaction finishes.
func TestChangelogHidesEventsUntilOlderTransactionsFinish(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	// An older transaction, still in flight. It takes its xid now.
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := appendChange(ctx, tx, idxParent+"/posts/old", "posts", "write"); err != nil {
		t.Fatal(err)
	}

	// A younger transaction that commits FIRST — higher seq, higher xid.
	setDoc(t, s, idxParent+"/posts/young", map[string]*pb.Value{"v": ival(1)})

	changes, err := s.Changes(ctx, Cursor{}, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range changes {
		if c.DocName == idxParent+"/posts/young" {
			t.Fatal("a committed event became visible while an OLDER transaction was still open — " +
				"a reader would advance past the older event and lose it permanently")
		}
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}

	// Both are now below the low-water mark, and in transaction order.
	changes, err = s.Changes(ctx, Cursor{}, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 {
		t.Fatalf("after both commit, got %d changes, want 2: %+v", len(changes), changes)
	}
	if changes[0].DocName != idxParent+"/posts/old" {
		t.Errorf("feed order = %s first, want the older transaction first", changes[0].DocName)
	}
}

// A consumer resumes from its cursor and must see each event exactly once.
func TestChangelogCursorResumesWithoutGapOrRepeat(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	for i := 0; i < 25; i++ {
		setDoc(t, s, fmt.Sprintf("%s/posts/p%02d", idxParent, i), map[string]*pb.Value{"v": ival(int64(i))})
	}

	seen := map[string]int{}
	var cur Cursor
	for range 10 {
		batch, err := s.Changes(ctx, cur, "", 7)
		if err != nil {
			t.Fatal(err)
		}
		if len(batch) == 0 {
			break
		}
		for _, c := range batch {
			seen[c.DocName]++
			cur = Cursor{XID: c.XID, Seq: c.Seq}
		}
	}
	if len(seen) != 25 {
		t.Fatalf("consumed %d distinct documents, want 25", len(seen))
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("%s delivered %d times, want exactly once", name, n)
		}
	}
}

func TestChangelogFiltersByCollection(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	setDoc(t, s, idxParent+"/posts/a", map[string]*pb.Value{"v": ival(1)})
	setDoc(t, s, idxParent+"/other/b", map[string]*pb.Value{"v": ival(2)})

	changes, err := s.Changes(ctx, Cursor{}, "posts", 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].CollectionID != "posts" {
		t.Fatalf("collection filter returned %+v", changes)
	}
}

func TestPruneChangesRemovesOldEvents(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	setDoc(t, s, idxParent+"/posts/a", map[string]*pb.Value{"v": ival(1)})

	// Nothing is old enough yet.
	if n, err := s.PruneChanges(ctx, time.Hour); err != nil || n != 0 {
		t.Fatalf("prune removed %d (err %v), want 0", n, err)
	}
	if n, err := s.PruneChanges(ctx, 0); err != nil || n != 1 {
		t.Fatalf("prune removed %d (err %v), want 1", n, err)
	}
}
