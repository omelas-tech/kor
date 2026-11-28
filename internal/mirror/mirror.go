// Package mirror keeps a set of collection trees identical in Firestore and
// kord, in both directions, while clients are split between the two.
//
// The case it exists for: an app moves a collection to kord, but installed
// copies of the old version keep reading and writing Firestore. Until those are
// gone, a write on either side must show up on the other, or users on two app
// versions stop seeing each other.
//
// Firestore → kord follows collection-group listeners; kord → Firestore follows
// kord's change feed. Every write the mirror makes is fingerprinted, so when it
// comes back from the other side it is recognised as an echo and dropped. That,
// not "are the two sides equal right now", is what stops a fast pair of edits
// from ping-ponging: the side that lags would otherwise copy its stale value
// back over the newer one.
//
// Conflicts are last-writer-wins per document. Two Firestore-side deletes are
// not copied: one made while the mirror is down (a listener's first snapshot
// shows what exists, not what vanished), and one made before Firestore's
// listener has reported the document at all (a listener reports net changes, so
// a create and delete it sees together cancel out). kord-side deletes always
// are, replayed from the saved cursor after a restart.
package mirror

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/genproto/googleapis/type/latlng"
)

// Config describes one mirror.
type Config struct {
	Firestore *firestore.Client // the Google side
	Kor       *firestore.Client // kord's trusted endpoint
	FeedURL   string            // kord's feed API, e.g. http://127.0.0.1:6566
	// Roots are the top-level collections mirrored, with everything under them.
	Roots []string
	// Groups are the collection ids to follow: each root and every
	// subcollection name under it (e.g. chats, messages, typing).
	Groups    []string
	StateFile string // kord feed cursor, so a restart resumes instead of skipping
	DryRun    bool
	Log       *slog.Logger
	// EchoTTL bounds how long a fingerprint waits for its echo. Default 2m.
	EchoTTL time.Duration
	// Poll is the feed polling interval when it is idle. Default 250ms.
	Poll time.Duration
}

// Stats counts what the mirror did. Safe to read while it runs.
type Stats struct {
	ToKor, ToFirestore, Deletes, Echoes, Equal, Errors atomic.Int64
}

// Mirror is a running mirror.
type Mirror struct {
	cfg    Config
	roots  map[string]bool
	echoes *echoSet
	Stats  Stats
	// ready closes once every Firestore listener has applied its first
	// snapshot (the initial copy).
	ready   chan struct{}
	pending atomic.Int32
}

// New validates the config.
func New(cfg Config) (*Mirror, error) {
	if cfg.Firestore == nil || cfg.Kor == nil || cfg.FeedURL == "" {
		return nil, errors.New("mirror: Firestore, Kor and FeedURL are required")
	}
	if len(cfg.Roots) == 0 || len(cfg.Groups) == 0 {
		return nil, errors.New("mirror: roots and groups are required")
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.EchoTTL == 0 {
		cfg.EchoTTL = 2 * time.Minute
	}
	if cfg.Poll == 0 {
		cfg.Poll = 250 * time.Millisecond
	}
	m := &Mirror{cfg: cfg, roots: map[string]bool{}, echoes: newEchoSet(cfg.EchoTTL), ready: make(chan struct{})}
	for _, r := range cfg.Roots {
		m.roots[r] = true
	}
	m.pending.Store(int32(len(cfg.Groups)))
	return m, nil
}

// Ready is closed once the initial Firestore → kord copy has been applied.
func (m *Mirror) Ready() <-chan struct{} { return m.ready }

// Run mirrors until ctx ends.
func (m *Mirror) Run(ctx context.Context) error {
	cursor, err := m.startCursor(ctx)
	if err != nil {
		return err
	}
	// Drain what kord changed while the mirror was down before listening to
	// Firestore: otherwise a listener's first snapshot would copy back a
	// document whose kord-side delete is still waiting in the feed.
	if cursor, err = m.drainKor(ctx, cursor); err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, g := range m.cfg.Groups {
		wg.Add(1)
		go func() { defer wg.Done(); m.followFirestore(ctx, g) }()
	}
	wg.Add(1)
	go func() { defer wg.Done(); m.followKor(ctx, cursor) }()
	wg.Add(1)
	go func() { defer wg.Done(); m.report(ctx) }()
	wg.Wait()
	return ctx.Err()
}

// rel turns a full document name into a path relative to the database root.
func rel(name string) string {
	if i := strings.Index(name, "/documents/"); i >= 0 {
		return name[i+len("/documents/"):]
	}
	return name
}

func (m *Mirror) mirrored(relPath string) bool {
	root, _, _ := strings.Cut(relPath, "/")
	return m.roots[root]
}

// --- Firestore → kord --------------------------------------------------------

func (m *Mirror) followFirestore(ctx context.Context, group string) {
	first := true
	backoff := time.Second
	for ctx.Err() == nil {
		it := m.cfg.Firestore.CollectionGroup(group).Snapshots(ctx)
		for {
			snap, err := it.Next()
			if err != nil {
				if ctx.Err() == nil {
					m.cfg.Log.Warn("mirror: firestore listener ended; restarting", "group", group, "err", err)
					m.Stats.Errors.Add(1)
				}
				break
			}
			backoff = time.Second
			if err := m.applyFromFirestore(ctx, snap.Changes); err != nil {
				m.cfg.Log.Error("mirror: firestore → kor", "group", group, "err", err)
				m.Stats.Errors.Add(1)
			}
			if first {
				first = false
				m.cfg.Log.Info("mirror: initial copy applied", "group", group, "docs", len(snap.Changes))
				if m.pending.Add(-1) == 0 {
					close(m.ready)
				}
			}
		}
		it.Stop()
		select {
		case <-ctx.Done():
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, time.Minute)
	}
}

func (m *Mirror) applyFromFirestore(ctx context.Context, changes []firestore.DocumentChange) error {
	type item struct {
		path string
		data map[string]any // nil = deleted
	}
	var items []item
	for _, c := range changes {
		p := rel(c.Doc.Ref.Path)
		if !m.mirrored(p) {
			continue
		}
		if c.Kind == firestore.DocumentRemoved {
			items = append(items, item{path: p})
		} else {
			items = append(items, item{path: p, data: c.Doc.Data()})
		}
	}
	if len(items) == 0 {
		return nil
	}
	paths := make([]string, len(items))
	for i, it := range items {
		paths[i] = it.path
	}
	current, err := getAll(ctx, m.cfg.Kor, paths)
	if err != nil {
		return fmt.Errorf("read kor: %w", err)
	}
	var writes []write
	for _, it := range items {
		h := fingerprint(it.data)
		if m.echoes.take(it.path, h) {
			m.Stats.Echoes.Add(1)
			continue
		}
		cur, exists := current[it.path]
		if it.data == nil && !exists || it.data != nil && exists && fingerprint(cur) == h {
			m.Stats.Equal.Add(1)
			continue
		}
		writes = append(writes, write{path: it.path, data: it.data, hash: h})
	}
	return m.commit(ctx, m.cfg.Kor, writes, &m.Stats.ToKor)
}

// --- kord → Firestore --------------------------------------------------------

func (m *Mirror) startCursor(ctx context.Context) (string, error) {
	if m.cfg.StateFile != "" {
		if b, err := os.ReadFile(m.cfg.StateFile); err == nil {
			if c := strings.TrimSpace(string(b)); c != "" {
				m.cfg.Log.Info("mirror: resuming kor feed", "cursor", c)
				return c, nil
			}
		}
	}
	var head struct {
		Cursor string `json:"cursor"`
	}
	if err := m.getJSON(ctx, m.cfg.FeedURL+"/feed/v1/head", &head); err != nil {
		return "", fmt.Errorf("mirror: feed head: %w", err)
	}
	m.cfg.Log.Info("mirror: starting kor feed at head", "cursor", head.Cursor)
	return head.Cursor, nil
}

type feedPage struct {
	Changes []struct {
		Name string `json:"name"`
	} `json:"changes"`
	Cursor string `json:"cursor"`
}

// drainKor applies the feed up to its current end.
func (m *Mirror) drainKor(ctx context.Context, cursor string) (string, error) {
	for {
		next, n, err := m.korPage(ctx, cursor)
		if err != nil {
			return cursor, fmt.Errorf("mirror: replay kor feed: %w", err)
		}
		cursor = next
		if n == 0 {
			return cursor, nil
		}
	}
}

func (m *Mirror) followKor(ctx context.Context, cursor string) {
	for ctx.Err() == nil {
		next, n, err := m.korPage(ctx, cursor)
		if err != nil {
			if ctx.Err() == nil {
				m.cfg.Log.Warn("mirror: kor → firestore", "err", err)
				m.Stats.Errors.Add(1)
				sleep(ctx, 2*time.Second)
			}
			continue
		}
		cursor = next
		if n == 0 {
			sleep(ctx, m.cfg.Poll)
		}
	}
}

// korPage applies one feed page and returns the cursor after it and how many
// changes it held. On error the cursor is not advanced: the page is retried,
// and every step is idempotent.
func (m *Mirror) korPage(ctx context.Context, cursor string) (string, int, error) {
	{
		q := url.Values{"after": {cursor}, "limit": {"500"}, "collection": {strings.Join(m.cfg.Groups, ",")}}
		var page feedPage
		if err := m.getJSON(ctx, m.cfg.FeedURL+"/feed/v1/changes?"+q.Encode(), &page); err != nil {
			return cursor, 0, err
		}
		if len(page.Changes) == 0 {
			return cursor, 0, nil
		}
		seen := map[string]bool{}
		var paths []string
		for _, c := range page.Changes {
			p := rel(c.Name)
			if m.mirrored(p) && !seen[p] {
				seen[p] = true
				paths = append(paths, p)
			}
		}
		if err := m.applyFromKor(ctx, paths); err != nil {
			return cursor, 0, err
		}
		m.saveCursor(page.Cursor)
		return page.Cursor, len(page.Changes), nil
	}
}

func (m *Mirror) applyFromKor(ctx context.Context, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	current, err := getAll(ctx, m.cfg.Kor, paths)
	if err != nil {
		return fmt.Errorf("read kor: %w", err)
	}
	// Echoes are recognised without touching Firestore; only what is left
	// costs a Firestore read.
	var check []string
	hashes := map[string]string{}
	for _, p := range paths {
		data, ok := current[p]
		if !ok {
			data = nil
		}
		h := fingerprint(data)
		if m.echoes.take(p, h) {
			m.Stats.Echoes.Add(1)
			continue
		}
		hashes[p] = h
		check = append(check, p)
	}
	if len(check) == 0 {
		return nil
	}
	remote, err := getAll(ctx, m.cfg.Firestore, check)
	if err != nil {
		return fmt.Errorf("read firestore: %w", err)
	}
	var writes []write
	for _, p := range check {
		data, exists := current[p]
		r, rexists := remote[p]
		if !exists && !rexists || exists && rexists && fingerprint(r) == hashes[p] {
			m.Stats.Equal.Add(1)
			continue
		}
		if !exists {
			data = nil
		}
		writes = append(writes, write{path: p, data: data, hash: hashes[p]})
	}
	return m.commit(ctx, m.cfg.Firestore, writes, &m.Stats.ToFirestore)
}

func (m *Mirror) saveCursor(c string) {
	if m.cfg.StateFile == "" || m.cfg.DryRun {
		return
	}
	tmp := m.cfg.StateFile + ".tmp"
	if err := os.WriteFile(tmp, []byte(c+"\n"), 0o644); err == nil {
		_ = os.Rename(tmp, m.cfg.StateFile)
	}
}

// --- shared ------------------------------------------------------------------

type write struct {
	path string
	data map[string]any // nil = delete
	hash string
}

// commit applies writes in batches, fingerprinting each before it is sent so
// the echo can never arrive first.
func (m *Mirror) commit(ctx context.Context, c *firestore.Client, writes []write, counter *atomic.Int64) error {
	const batchSize = 200
	for start := 0; start < len(writes); start += batchSize {
		chunk := writes[start:min(start+batchSize, len(writes))]
		if m.cfg.DryRun {
			for _, w := range chunk {
				m.cfg.Log.Info("mirror: dry-run write", "path", w.path, "delete", w.data == nil)
			}
			counter.Add(int64(len(chunk)))
			continue
		}
		b := c.Batch()
		for _, w := range chunk {
			m.echoes.put(w.path, w.hash)
			ref := c.Doc(w.path)
			if w.data == nil {
				b.Delete(ref)
				m.Stats.Deletes.Add(1)
			} else {
				b.Set(ref, retarget(c, w.data))
			}
		}
		if _, err := b.Commit(ctx); err != nil {
			for _, w := range chunk {
				m.echoes.drop(w.path, w.hash)
			}
			return err
		}
		counter.Add(int64(len(chunk)))
	}
	return nil
}

// getAll reads documents by relative path; missing ones are absent from the map.
func getAll(ctx context.Context, c *firestore.Client, paths []string) (map[string]map[string]any, error) {
	out := make(map[string]map[string]any, len(paths))
	const chunk = 300
	for start := 0; start < len(paths); start += chunk {
		var refs []*firestore.DocumentRef
		for _, p := range paths[start:min(start+chunk, len(paths))] {
			refs = append(refs, c.Doc(p))
		}
		snaps, err := c.GetAll(ctx, refs)
		if err != nil {
			return nil, err
		}
		for _, s := range snaps {
			if s.Exists() {
				out[rel(s.Ref.Path)] = s.Data()
			}
		}
	}
	return out, nil
}

// retarget rewrites document references so they belong to the client being
// written to (a reference carries its client's project and database).
func retarget(c *firestore.Client, v map[string]any) map[string]any {
	var conv func(any) any
	conv = func(x any) any {
		switch t := x.(type) {
		case *firestore.DocumentRef:
			if t == nil {
				return nil
			}
			return c.Doc(rel(t.Path))
		case map[string]any:
			m := make(map[string]any, len(t))
			for k, e := range t {
				m[k] = conv(e)
			}
			return m
		case []any:
			l := make([]any, len(t))
			for i, e := range t {
				l[i] = conv(e)
			}
			return l
		}
		return x
	}
	return conv(v).(map[string]any)
}

// fingerprint is a stable hash of a document's fields; "" means "no document".
func fingerprint(data map[string]any) string {
	if data == nil {
		return ""
	}
	var b bytes.Buffer
	canon(&b, data)
	sum := sha256.Sum256(b.Bytes())
	return hex.EncodeToString(sum[:16])
}

func canon(b *bytes.Buffer, v any) {
	switch t := v.(type) {
	case nil:
		b.WriteString("n")
	case bool:
		if t {
			b.WriteString("T")
		} else {
			b.WriteString("F")
		}
	case int64:
		b.WriteString("i" + strconv.FormatInt(t, 10))
	case float64:
		switch {
		case math.IsNaN(t):
			b.WriteString("dNaN")
		default:
			b.WriteString("d" + strconv.FormatFloat(t, 'g', -1, 64))
		}
	case string:
		b.WriteString("s" + strconv.Quote(t))
	case []byte:
		b.WriteString("y" + base64.StdEncoding.EncodeToString(t))
	case time.Time:
		// Firestore keeps microseconds.
		b.WriteString("t" + strconv.FormatInt(t.UnixMicro(), 10))
	case *latlng.LatLng:
		fmt.Fprintf(b, "g%v,%v", t.GetLatitude(), t.GetLongitude())
	case *firestore.DocumentRef:
		if t == nil {
			b.WriteString("n")
		} else {
			b.WriteString("r" + strconv.Quote(rel(t.Path)))
		}
	case []any:
		b.WriteString("[")
		for _, e := range t {
			canon(b, e)
			b.WriteString(",")
		}
		b.WriteString("]")
	case map[string]any:
		keys := make([]string, 0, len(t))
		for k := range t {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("{")
		for _, k := range keys {
			b.WriteString(strconv.Quote(k) + ":")
			canon(b, t[k])
			b.WriteString(",")
		}
		b.WriteString("}")
	default:
		fmt.Fprintf(b, "?%T:%v", t, t)
	}
}

// echoSet remembers fingerprints of the mirror's own writes until their echo
// arrives (each is consumed once) or they expire.
type echoSet struct {
	mu  sync.Mutex
	ttl time.Duration
	m   map[string][]echo
}

type echo struct {
	hash string
	at   time.Time
}

func newEchoSet(ttl time.Duration) *echoSet { return &echoSet{ttl: ttl, m: map[string][]echo{}} }

func (e *echoSet) put(path, hash string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now()
	e.m[path] = append(e.fresh(path, now), echo{hash, now})
}

func (e *echoSet) take(path, hash string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	list := e.fresh(path, time.Now())
	for i, x := range list {
		if x.hash == hash {
			list = append(list[:i], list[i+1:]...)
			e.set(path, list)
			return true
		}
	}
	e.set(path, list)
	return false
}

func (e *echoSet) drop(path, hash string) { e.take(path, hash) }

func (e *echoSet) fresh(path string, now time.Time) []echo {
	var out []echo
	for _, x := range e.m[path] {
		if now.Sub(x.at) < e.ttl {
			out = append(out, x)
		}
	}
	return out
}

func (e *echoSet) set(path string, list []echo) {
	if len(list) == 0 {
		delete(e.m, path)
	} else {
		e.m[path] = list
	}
}

func (m *Mirror) getJSON(ctx context.Context, u string, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", u, res.Status)
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (m *Mirror) report(ctx context.Context) {
	t := time.NewTicker(time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s := &m.Stats
			m.cfg.Log.Info("mirror: stats", "toKor", s.ToKor.Load(), "toFirestore", s.ToFirestore.Load(),
				"deletes", s.Deletes.Load(), "echoes", s.Echoes.Load(), "equal", s.Equal.Load(), "errors", s.Errors.Load())
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
