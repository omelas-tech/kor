package mirror

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/omelas-tech/kor/internal/feed"
	"github.com/omelas-tech/kor/internal/pgtest"
	"github.com/omelas-tech/kor/internal/rpc"
	"github.com/omelas-tech/kor/internal/store"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

const project = "mirror-test"

func startStore(t *testing.T) (*firestore.Client, *store.Store) {
	t.Helper()
	st, err := store.Open(context.Background(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	rpc.New(st).Register(g)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	c, err := firestore.NewClient(context.Background(), project, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c, st
}

type env struct {
	fs, kor *firestore.Client
	m       *Mirror
	cancel  context.CancelFunc
	done    chan struct{}
}

// start runs a mirror with a store standing in for Firestore: kord speaks the
// same API, listeners included.
func start(t *testing.T, fs, kor *firestore.Client, korStore *store.Store, state string) *env {
	t.Helper()
	srv := httptest.NewServer(feed.Handler(korStore, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	m, err := New(Config{
		Firestore: fs, Kor: kor, FeedURL: srv.URL,
		Roots:     []string{"chats", "presence"},
		Groups:    []string{"chats", "messages", "presence"},
		StateFile: state, Poll: 20 * time.Millisecond,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e := &env{fs: fs, kor: kor, m: m, cancel: cancel, done: make(chan struct{})}
	go func() { _ = m.Run(ctx); close(e.done) }()
	t.Cleanup(e.stop)
	select {
	case <-m.Ready():
	case <-time.After(15 * time.Second):
		t.Fatal("initial copy never finished")
	}
	return e
}

func (e *env) stop() {
	e.cancel()
	<-e.done
}

func get(t *testing.T, c *firestore.Client, path string) map[string]any {
	t.Helper()
	s, err := c.Doc(path).Get(context.Background())
	if err != nil {
		if s != nil && !s.Exists() {
			return nil
		}
		return nil
	}
	return s.Data()
}

// eventually waits until both stores hold the same value for path (nil =
// absent) and it equals want.
func eventually(t *testing.T, e *env, path string, want map[string]any) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		a, b := get(t, e.fs, path), get(t, e.kor, path)
		if fingerprint(a) == fingerprint(want) && fingerprint(b) == fingerprint(want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: firestore=%v kor=%v, want %v", path, a, b, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func set(t *testing.T, c *firestore.Client, path string, v map[string]any) {
	t.Helper()
	if _, err := c.Doc(path).Set(context.Background(), v); err != nil {
		t.Fatal(err)
	}
}

func del(t *testing.T, c *firestore.Client, path string) {
	t.Helper()
	if _, err := c.Doc(path).Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMirrorBothWays(t *testing.T) {
	fs, _ := startStore(t)
	kor, korStore := startStore(t)
	ts := time.Date(2026, 11, 28, 12, 0, 0, 123456000, time.UTC)

	// Existing data is copied on start; unmirrored roots are left alone.
	set(t, fs, "chats/c1", map[string]any{"participants": []any{"a", "b"}, "at": ts})
	set(t, fs, "chats/c1/messages/m1", map[string]any{"text": "hi", "n": int64(1), "f": 1.5, "nested": map[string]any{"x": true}})
	set(t, fs, "other/x", map[string]any{"v": int64(1)})
	e := start(t, fs, kor, korStore, filepath.Join(t.TempDir(), "cursor"))
	eventually(t, e, "chats/c1", map[string]any{"participants": []any{"a", "b"}, "at": ts})
	eventually(t, e, "chats/c1/messages/m1", map[string]any{"text": "hi", "n": int64(1), "f": 1.5, "nested": map[string]any{"x": true}})
	if get(t, kor, "other/x") != nil {
		t.Fatal("unmirrored root was copied")
	}

	// Writes and deletes flow both ways.
	set(t, fs, "chats/c1/messages/m2", map[string]any{"text": "from old app"})
	eventually(t, e, "chats/c1/messages/m2", map[string]any{"text": "from old app"})
	set(t, kor, "chats/c1/messages/m3", map[string]any{"text": "from new app"})
	eventually(t, e, "chats/c1/messages/m3", map[string]any{"text": "from new app"})
	set(t, kor, "chats/c1/messages/m2", map[string]any{"text": "edited on kor"})
	eventually(t, e, "chats/c1/messages/m2", map[string]any{"text": "edited on kor"})
	del(t, kor, "chats/c1/messages/m2")
	eventually(t, e, "chats/c1/messages/m2", nil)
	// A listener only reports what it has seen settle; give the Firestore side
	// time to observe the mirrored create before deleting it there.
	time.Sleep(300 * time.Millisecond)
	del(t, fs, "chats/c1/messages/m3")
	eventually(t, e, "chats/c1/messages/m3", nil)

	// A burst of edits on one side converges on the last one, from either side.
	for i := 0; i < 30; i++ {
		set(t, fs, "presence/u1", map[string]any{"online": i%2 == 0, "seq": int64(i)})
	}
	eventually(t, e, "presence/u1", map[string]any{"online": false, "seq": int64(29)})
	for i := 0; i < 30; i++ {
		set(t, kor, "presence/u2", map[string]any{"seq": int64(i)})
	}
	eventually(t, e, "presence/u2", map[string]any{"seq": int64(29)})

	// And then it stays there: nothing keeps bouncing.
	before := e.m.Stats.ToKor.Load() + e.m.Stats.ToFirestore.Load()
	time.Sleep(500 * time.Millisecond)
	if after := e.m.Stats.ToKor.Load() + e.m.Stats.ToFirestore.Load(); after != before {
		t.Fatalf("mirror still writing after settling: %d → %d", before, after)
	}
	eventually(t, e, "presence/u1", map[string]any{"online": false, "seq": int64(29)})
	eventually(t, e, "presence/u2", map[string]any{"seq": int64(29)})
}

func TestMirrorResumesKorFeedAfterRestart(t *testing.T) {
	fs, _ := startStore(t)
	kor, korStore := startStore(t)
	state := filepath.Join(t.TempDir(), "cursor")
	e := start(t, fs, kor, korStore, state)
	set(t, kor, "chats/c1", map[string]any{"v": int64(1)})
	eventually(t, e, "chats/c1", map[string]any{"v": int64(1)})
	e.stop()

	// Written to kor while the mirror is down: replayed from the cursor.
	set(t, kor, "chats/c2", map[string]any{"v": int64(2)})
	del(t, kor, "chats/c1")
	e = start(t, fs, kor, korStore, state)
	eventually(t, e, "chats/c2", map[string]any{"v": int64(2)})
	eventually(t, e, "chats/c1", nil)
}

func TestMirrorManyDocs(t *testing.T) {
	fs, _ := startStore(t)
	kor, korStore := startStore(t)
	b := fs.Batch()
	for i := 0; i < 450; i++ {
		b.Set(fs.Doc(fmt.Sprintf("chats/c%d", i%15)), map[string]any{"n": int64(i % 15)})
		b.Set(fs.Doc(fmt.Sprintf("chats/c%d/messages/m%d", i%15, i)), map[string]any{"i": int64(i)})
		if i%100 == 99 {
			if _, err := b.Commit(context.Background()); err != nil {
				t.Fatal(err)
			}
			b = fs.Batch()
		}
	}
	if _, err := b.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	e := start(t, fs, kor, korStore, "")
	for _, i := range []int{0, 1, 223, 449} {
		eventually(t, e, fmt.Sprintf("chats/c%d/messages/m%d", i%15, i), map[string]any{"i": int64(i)})
	}
	n, err := kor.CollectionGroup("messages").Documents(context.Background()).GetAll()
	if err != nil || len(n) != 450 {
		t.Fatalf("kor has %d messages (%v), want 450", len(n), err)
	}
}
