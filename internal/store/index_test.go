package store

import (
	"context"
	"fmt"
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

// DropCollection is the undo for a bad import, so the property that matters is
// containment: it removes the named collection completely — documents, index
// entries and registry row — and touches nothing else.
func TestDropCollectionRemovesOnlyItsOwnData(t *testing.T) {
	s := openStore(t)
	ctx := context.Background()

	doomed := index.Def{CollectionID: "doomed", Fields: []index.Field{{Path: "v"}}}
	keeper := index.Def{CollectionID: "keeper", Fields: []index.Field{{Path: "v"}}}
	if err := s.SetIndexes(ctx, []index.Def{doomed, keeper}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		setDoc(t, s, fmt.Sprintf("%s/doomed/d%02d", idxParent, i), map[string]*pb.Value{"v": ival(int64(i))})
	}
	for i := 0; i < 7; i++ {
		setDoc(t, s, fmt.Sprintf("%s/keeper/k%02d", idxParent, i), map[string]*pb.Value{"v": ival(int64(i))})
	}
	for _, d := range []index.Def{doomed, keeper} {
		if _, err := s.BackfillIndex(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	// A batch size below the document count so the loop actually iterates —
	// a single-batch run would not exercise the batching this relies on.
	n, err := s.DropCollection(ctx, "doomed", 10, nil)
	if err != nil {
		t.Fatal(err)
	}
	if n != 25 {
		t.Errorf("deleted %d, want 25", n)
	}

	if got, _ := s.CountCollection(ctx, "doomed"); got != 0 {
		t.Errorf("%d documents survived the drop", got)
	}
	if got, _ := s.CountCollection(ctx, "keeper"); got != 7 {
		t.Errorf("keeper has %d documents, want 7 — the drop was not contained", got)
	}
	// Index entries whose documents are gone are unreachable rows that still
	// cost work on every write, and a query finding one would fail.
	if got, _ := s.CountIndexEntries(ctx, doomed); got != 0 {
		t.Errorf("%d index entries outlived their documents", got)
	}
	if got, _ := s.CountIndexEntries(ctx, keeper); got != 7 {
		t.Errorf("keeper index has %d entries, want 7", got)
	}

	var registry int
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM collections WHERE collection_id = 'doomed'`).Scan(&registry); err != nil {
		t.Fatal(err)
	}
	if registry != 0 {
		t.Errorf("registry still lists the dropped collection (%d rows)", registry)
	}
	if err := s.Pool.QueryRow(ctx,
		`SELECT count(*) FROM collections WHERE collection_id = 'keeper'`).Scan(&registry); err != nil {
		t.Fatal(err)
	}
	if registry != 1 {
		t.Errorf("keeper registry row = %d, want 1", registry)
	}
}
