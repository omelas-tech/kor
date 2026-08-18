package store

import (
	"context"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/index"
)

func idxDef(coll string, fields ...index.Field) index.Def {
	return index.Def{CollectionID: coll, Fields: fields}
}

func sval(s string) *pb.Value { return &pb.Value{ValueType: &pb.Value_StringValue{StringValue: s}} }
func ival(n int64) *pb.Value  { return &pb.Value{ValueType: &pb.Value_IntegerValue{IntegerValue: n}} }

// setDoc writes one document through the normal commit path, so index
// maintenance runs exactly as it does in production.
func setDoc(t *testing.T, s *Store, name string, fields map[string]*pb.Value) {
	t.Helper()
	_, _, err := s.Commit(context.Background(), []*pb.Write{{
		Operation: &pb.Write_Update{Update: &pb.Document{Name: name, Fields: fields}},
	}})
	if err != nil {
		t.Fatalf("commit %s: %v", name, err)
	}
}

func deleteDoc(t *testing.T, s *Store, name string) {
	t.Helper()
	_, _, err := s.Commit(context.Background(), []*pb.Write{{
		Operation: &pb.Write_Delete{Delete: name},
	}})
	if err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}
}

func TestIndexEntriesFollowDocumentWrites(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	d := idxDef("posts", index.Field{Path: "author"}, index.Field{Path: "score", Desc: true})
	if err := s.SetIndexes(ctx, []index.Def{d}); err != nil {
		t.Fatal(err)
	}

	base := "projects/p/databases/(default)/documents/posts/"
	setDoc(t, s, base+"a", map[string]*pb.Value{"author": sval("ann"), "score": ival(3)})
	setDoc(t, s, base+"b", map[string]*pb.Value{"author": sval("ann"), "score": ival(1)})

	n, err := s.CountIndexEntries(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Fatalf("entries = %d, want 2", n)
	}

	t.Run("an update replaces rather than accumulates", func(t *testing.T) {
		setDoc(t, s, base+"a", map[string]*pb.Value{"author": sval("ann"), "score": ival(9)})
		n, _ := s.CountIndexEntries(ctx, d)
		if n != 2 {
			t.Errorf("entries = %d, want 2 — a rewritten document must not leave its old key behind", n)
		}
	})

	t.Run("a delete removes the entry", func(t *testing.T) {
		deleteDoc(t, s, base+"b")
		n, _ := s.CountIndexEntries(ctx, d)
		if n != 1 {
			t.Errorf("entries = %d, want 1", n)
		}
	})

	t.Run("a document missing an indexed field is omitted", func(t *testing.T) {
		setDoc(t, s, base+"c", map[string]*pb.Value{"author": sval("bob")}) // no score
		n, _ := s.CountIndexEntries(ctx, d)
		if n != 1 {
			t.Errorf("entries = %d, want 1 — a document lacking an indexed field must not be indexed", n)
		}
	})

	t.Run("gaining the missing field indexes it", func(t *testing.T) {
		setDoc(t, s, base+"c", map[string]*pb.Value{"author": sval("bob"), "score": ival(5)})
		n, _ := s.CountIndexEntries(ctx, d)
		if n != 2 {
			t.Errorf("entries = %d, want 2 — a document that gains the field must become indexed", n)
		}
	})
}

func TestIndexMaintenanceIsScopedToItsCollection(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()
	d := idxDef("posts", index.Field{Path: "author"})
	if err := s.SetIndexes(ctx, []index.Def{d}); err != nil {
		t.Fatal(err)
	}
	setDoc(t, s, "projects/p/databases/(default)/documents/posts/x",
		map[string]*pb.Value{"author": sval("ann")})
	setDoc(t, s, "projects/p/databases/(default)/documents/comments/y",
		map[string]*pb.Value{"author": sval("ann")})

	n, _ := s.CountIndexEntries(ctx, d)
	if n != 1 {
		t.Errorf("entries = %d, want 1 — a document in another collection must not be indexed", n)
	}
}

func TestNoIndexesMeansNoEntriesAndNoCost(t *testing.T) {
	// The default. A deployment defining no indexes must behave exactly as it
	// did before this existed.
	s := openStore(t)
	ctx := context.Background()
	setDoc(t, s, "projects/p/databases/(default)/documents/posts/x",
		map[string]*pb.Value{"author": sval("ann")})

	var n int64
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM index_entries`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("index_entries = %d, want 0 when no indexes are registered", n)
	}
}

func TestRegistrationDoesNotImplyReadiness(t *testing.T) {
	// The gap between registering and backfilling is the window in which the
	// index exists but is incomplete. Serving reads there returns fewer
	// documents than the collection holds, so readiness is tracked separately.
	s := openStore(t)
	ctx := context.Background()
	d := idxDef("posts", index.Field{Path: "author"})
	if err := s.SetIndexes(ctx, []index.Def{d}); err != nil {
		t.Fatal(err)
	}

	ready, err := s.ReadyIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if ready[d.ID()] {
		t.Error("a freshly registered index must not be marked ready")
	}

	if err := s.MarkIndexReady(ctx, d); err != nil {
		t.Fatal(err)
	}
	ready, _ = s.ReadyIndexes(ctx)
	if !ready[d.ID()] {
		t.Error("MarkIndexReady must make the index ready")
	}
}

// kord holds no config file: it rebuilds the registry from index_defs, which is
// what lets the CLI register and backfill against a running server. The
// readiness half matters most — a definition registered but not yet backfilled
// must come back unusable, or a restart would quietly start serving reads from
// a partial index.
func TestLoadIndexesRebuildsRegistryFromPostgres(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	ready := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "score"}}}
	pending := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}, {Path: "views", Desc: true}}}
	if err := s.SetIndexes(ctx, []index.Def{ready, pending}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.BackfillIndex(ctx, ready); err != nil {
		t.Fatal(err)
	}

	// Emptying the in-memory registry stands in for a restarted kord: whatever
	// comes back must come from index_defs, not from what this process did.
	s.indexes.mu.Lock()
	s.indexes.byColl, s.indexes.ready = nil, nil
	s.indexes.mu.Unlock()

	n, err := s.LoadIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("ready count = %d, want 1", n)
	}
	defs := s.indexes.forCollection("posts")
	if len(defs) != 2 {
		t.Fatalf("registered defs = %d, want both", len(defs))
	}
	set := s.indexes.readySet()
	if !set[ready.ID()] {
		t.Error("a backfilled index must come back ready")
	}
	if set[pending.ID()] {
		t.Error("an index that was never backfilled must come back unusable: serving reads " +
			"from it returns fewer documents than the collection holds")
	}
}

func TestDropIndexRemovesEntriesAndDefinition(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	d := index.Def{CollectionID: "posts", Fields: []index.Field{{Path: "author"}}}
	if err := s.SetIndexes(ctx, []index.Def{d}); err != nil {
		t.Fatal(err)
	}
	setDoc(t, s, idxParent+"/posts/p1", map[string]*pb.Value{"author": sval("ann")})
	if _, err := s.BackfillIndex(ctx, d); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.CountIndexEntries(ctx, d); n == 0 {
		t.Fatal("expected entries before drop")
	}

	if _, err := s.DropIndex(ctx, d); err != nil {
		t.Fatal(err)
	}
	// Entries outliving their definition are unreachable rows that still cost
	// work on every write, with nothing able to read them.
	if n, _ := s.CountIndexEntries(ctx, d); n != 0 {
		t.Errorf("entries survived the drop: %d", n)
	}
	rows, err := s.ListIndexes(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("definition survived the drop: %+v", rows)
	}
}
