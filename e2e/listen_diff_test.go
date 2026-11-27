package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
)

// Differential test for realtime: the same listeners run against Google's
// Firestore emulator and Kor while a random sequence of writes lands on both.
// After every write each listener's latest snapshot must equal the result of
// the same query read from the emulator, on BOTH stores — so this checks Kor
// against Firestore's semantics for membership, order and limit windows, and
// checks the oracle against itself.
//
//	KOR_FUZZ_EMULATOR=127.0.0.1:8432 go test ./e2e/ -run TestListenDifferential -count=1
func TestListenDifferential(t *testing.T) {
	emu := dial(t, emulatorAddr(t))
	kor, _ := startKordConn(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	seed := time.Now().UnixNano()
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("seed %d", seed)
	// The emulator keeps state between runs: give each run its own names.
	run := fmt.Sprintf("ld%d", seed%1_000_000_000)
	sub := "items_" + run

	queries := map[string]func(c *firestore.Client) firestore.Query{
		"filter-order": func(c *firestore.Client) firestore.Query {
			return c.Collection(run).Where("room", "==", "a").OrderBy("n", firestore.Asc)
		},
		"desc-limit": func(c *firestore.Client) firestore.Query {
			return c.Collection(run).OrderBy("n", firestore.Desc).Limit(3)
		},
		// Explicit OrderBy: the Go SDK orders a listener's snapshot client-side
		// and ignores the implicit inequality ordering a Get applies, so
		// without it both Firestore and Kor "fail" (the query fuzz covers the
		// server's implicit ordering).
		"inequality": func(c *firestore.Client) firestore.Query {
			return c.Collection(run).Where("n", ">=", 3).OrderBy("n", firestore.Asc)
		},
		"in-limit": func(c *firestore.Client) firestore.Query {
			return c.Collection(run).Where("room", "in", []any{"a", "b"}).OrderBy("n", firestore.Asc).Limit(2)
		},
		"group": func(c *firestore.Client) firestore.Query {
			return c.CollectionGroup(sub).Where("room", "==", "b")
		},
		"whole": func(c *firestore.Client) firestore.Query { return c.Collection(run).Query },
	}
	watchedDoc := run + "/d1"

	type pair struct{ emu, kor *watcher }
	watchers := map[string]pair{}
	for name, q := range queries {
		watchers[name] = pair{watchQuery(ctx, q(emu)), watchQuery(ctx, q(kor))}
	}
	watchers["doc"] = pair{watchDoc(ctx, emu.Doc(watchedDoc)), watchDoc(ctx, kor.Doc(watchedDoc))}

	expected := func(name string) string {
		if name == "doc" {
			s, err := emu.Doc(watchedDoc).Get(ctx)
			if err != nil && !s.Exists() {
				return "<missing>"
			}
			return renderDocs([]*firestore.DocumentSnapshot{s})
		}
		docs, err := queries[name](emu).Documents(ctx).GetAll()
		if err != nil {
			t.Fatalf("oracle %s: %v", name, err)
		}
		return renderDocs(docs)
	}

	docPath := func() string {
		if rnd.Intn(3) == 0 {
			return fmt.Sprintf("%s/p%d/%s/i%d", run, rnd.Intn(3), sub, rnd.Intn(4))
		}
		return fmt.Sprintf("%s/d%d", run, rnd.Intn(8))
	}
	randomDoc := func() map[string]any {
		d := map[string]any{"room": []string{"a", "b", "c"}[rnd.Intn(3)]}
		if rnd.Intn(5) > 0 { // sometimes no n: it must drop out of n-ordered queries
			d["n"] = int64(rnd.Intn(7))
		}
		return d
	}

	for step := 0; step < 80; step++ {
		path := docPath()
		var op string
		var apply func(c *firestore.Client) error
		switch r := rnd.Intn(10); {
		case r < 5:
			d := randomDoc()
			op = fmt.Sprintf("set %s %v", path, d)
			apply = func(c *firestore.Client) error { _, err := c.Doc(path).Set(ctx, d); return err }
		case r < 8:
			n := int64(rnd.Intn(7))
			op = fmt.Sprintf("merge %s n=%d", path, n)
			apply = func(c *firestore.Client) error {
				_, err := c.Doc(path).Set(ctx, map[string]any{"n": n}, firestore.MergeAll)
				return err
			}
		default:
			op = "delete " + path
			apply = func(c *firestore.Client) error { _, err := c.Doc(path).Delete(ctx); return err }
		}
		if err := apply(emu); err != nil {
			t.Fatalf("step %d %s on emulator: %v", step, op, err)
		}
		if err := apply(kor); err != nil {
			t.Fatalf("step %d %s on kor: %v", step, op, err)
		}
		for name, w := range watchers {
			want := expected(name)
			for side, wt := range map[string]*watcher{"emulator": w.emu, "kor": w.kor} {
				if got, ok := wt.await(want, 5*time.Second); !ok {
					t.Fatalf("seed %d step %d (%s): %s listener on %s\n got: %s\nwant: %s", seed, step, op, name, side, got, want)
				}
			}
		}
	}
}

type watcher struct {
	mu     sync.Mutex
	latest string
	err    error
	seen   bool
}

func (w *watcher) set(s string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil {
		w.err = err
		return
	}
	w.latest, w.seen = s, true
}

// await waits for the listener to reach want: snapshots arrive
// asynchronously after the write that caused them.
func (w *watcher) await(want string, d time.Duration) (string, bool) {
	deadline := time.Now().Add(d)
	for {
		w.mu.Lock()
		got, err, seen := w.latest, w.err, w.seen
		w.mu.Unlock()
		if err != nil {
			return "error: " + err.Error(), false
		}
		if seen && got == want {
			return got, true
		}
		if time.Now().After(deadline) {
			return got, false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func watchQuery(ctx context.Context, q firestore.Query) *watcher {
	w := &watcher{}
	it := q.Snapshots(ctx)
	go func() {
		defer it.Stop()
		for {
			snap, err := it.Next()
			if err != nil {
				if ctx.Err() == nil {
					w.set("", err)
				}
				return
			}
			docs, err := snap.Documents.GetAll()
			w.set(renderDocs(docs), err)
		}
	}()
	return w
}

func watchDoc(ctx context.Context, ref *firestore.DocumentRef) *watcher {
	w := &watcher{}
	it := ref.Snapshots(ctx)
	go func() {
		defer it.Stop()
		for {
			snap, err := it.Next()
			if err != nil {
				if ctx.Err() == nil {
					w.set("", err)
				}
				return
			}
			if !snap.Exists() {
				w.set("<missing>", nil)
				continue
			}
			w.set(renderDocs([]*firestore.DocumentSnapshot{snap}), nil)
		}
	}()
	return w
}

func renderDocs(docs []*firestore.DocumentSnapshot) string {
	parts := make([]string, 0, len(docs))
	for _, d := range docs {
		if !d.Exists() {
			parts = append(parts, "<missing>")
			continue
		}
		p := d.Ref.Path
		if i := strings.Index(p, "/documents/"); i >= 0 {
			p = p[i+len("/documents/"):]
		}
		parts = append(parts, fmt.Sprintf("%s%v", p, d.Data()))
	}
	return "[" + strings.Join(parts, " ") + "]"
}
