package feed

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"

	"github.com/omelas-tech/kor/internal/pgtest"
	"github.com/omelas-tech/kor/internal/store"
)

func TestMain(m *testing.M) { pgtest.Main(m) }

const root = "projects/p/databases/(default)/documents"

func setup(t *testing.T) (*store.Store, *httptest.Server) {
	t.Helper()
	st, err := store.Open(context.Background(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	srv := httptest.NewServer(Handler(st, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(srv.Close)
	return st, srv
}

func set(t *testing.T, st *store.Store, name string, v int64) {
	t.Helper()
	if _, _, err := st.Commit(context.Background(), []*pb.Write{{Operation: &pb.Write_Update{Update: &pb.Document{
		Name: name, Fields: map[string]*pb.Value{"v": {ValueType: &pb.Value_IntegerValue{IntegerValue: v}}},
	}}}}); err != nil {
		t.Fatal(err)
	}
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if err := json.NewDecoder(res.Body).Decode(out); err != nil {
		t.Fatal(err)
	}
	return res.StatusCode
}

func TestFeedAPIRoundTrip(t *testing.T) {
	st, srv := setup(t)

	// Register, and take the head the registration hands back.
	res, err := http.Post(srv.URL+"/feed/v1/images", "application/json",
		bytes.NewBufferString(`{"register":["posts"]}`))
	if err != nil {
		t.Fatal(err)
	}
	var reg struct {
		Collections []string `json:"collections"`
		Head        string   `json:"head"`
	}
	if err := json.NewDecoder(res.Body).Decode(&reg); err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if len(reg.Collections) != 1 || reg.Collections[0] != "posts" || reg.Head == "" {
		t.Fatalf("register response = %+v", reg)
	}

	set(t, st, root+"/posts/a", 1)
	set(t, st, root+"/posts/a", 2)
	set(t, st, root+"/other/x", 1)

	var page Page
	if code := getJSON(t, srv.URL+"/feed/v1/changes?images=1&collection=posts&after="+reg.Head, &page); code != 200 {
		t.Fatalf("status %d", code)
	}
	if len(page.Changes) != 2 {
		t.Fatalf("got %d changes, want 2 (collection filter): %+v", len(page.Changes), page.Changes)
	}
	upd := page.Changes[1]
	// REST JSON encoding: typed values, int64 as a string — what Firestore's
	// own trigger events carry.
	var before, after struct {
		Name   string                       `json:"name"`
		Fields map[string]map[string]string `json:"fields"`
		Create string                       `json:"createTime"`
	}
	if err := json.Unmarshal(upd.Before, &before); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(upd.After, &after); err != nil {
		t.Fatal(err)
	}
	if before.Fields["v"]["integerValue"] != "1" || after.Fields["v"]["integerValue"] != "2" {
		t.Errorf("update images: before %v after %v", before.Fields, after.Fields)
	}
	if after.Name != root+"/posts/a" || after.Create == "" {
		t.Errorf("after-image name/createTime = %q/%q", after.Name, after.Create)
	}
	if page.Changes[0].Before != nil {
		t.Errorf("create carried a before-image: %s", page.Changes[0].Before)
	}

	// Resuming from the returned cursor yields nothing new.
	var rest Page
	getJSON(t, srv.URL+"/feed/v1/changes?collection=posts&after="+page.Cursor, &rest)
	if len(rest.Changes) != 0 || rest.Cursor != page.Cursor {
		t.Errorf("resume: %d changes, cursor %q (want 0, %q)", len(rest.Changes), rest.Cursor, page.Cursor)
	}

	// Without images=1 the rows stay small.
	var bare Page
	getJSON(t, srv.URL+"/feed/v1/changes?collection=posts&after="+reg.Head, &bare)
	if len(bare.Changes) != 2 || bare.Changes[1].After != nil || !bare.Changes[1].HasImages {
		t.Errorf("images omitted unless asked: %+v", bare.Changes)
	}
}

func TestFeedAPIRejectsBadCursor(t *testing.T) {
	_, srv := setup(t)
	var e map[string]string
	if code := getJSON(t, srv.URL+"/feed/v1/changes?after=nope", &e); code != http.StatusBadRequest || e["error"] == "" {
		t.Errorf("bad cursor: status %d body %v", code, e)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	c := store.Cursor{XID: 18446744073709551000, Seq: 42}
	got, err := ParseCursor(FormatCursor(c))
	if err != nil || got != c {
		t.Errorf("round trip %+v -> %+v (%v)", c, got, err)
	}
	if z, err := ParseCursor(""); err != nil || z != (store.Cursor{}) {
		t.Errorf("empty cursor = %+v, %v", z, err)
	}
}
