package web

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestASCIIJSONMatchesJSStringLength(t *testing.T) {
	in := []byte(`{"t":"çağ 🎨"}`)
	out := asciiJSON(in)
	for _, c := range out {
		if c >= 0x80 {
			t.Fatalf("non-ASCII byte left in %q", out)
		}
	}
	var a, b map[string]string
	if err := json.Unmarshal(in, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(out, &b); err != nil {
		t.Fatal(err)
	}
	if a["t"] != b["t"] {
		t.Fatalf("round trip changed the text: %q vs %q", a["t"], b["t"])
	}
	if want := `\u` + "d83c" + `\u` + "dfa8"; !strings.Contains(string(out), want) {
		t.Fatalf("astral rune not a surrogate pair: %s", out)
	}
}

func TestErrorBodyCarriesStatusName(t *testing.T) {
	b := errorBody(status.Error(codes.PermissionDenied, "no"))
	e := b["error"].(map[string]any)
	if e["status"] != "PERMISSION_DENIED" || e["code"] != 403 {
		t.Fatalf("got %v", e)
	}
}

func TestSessionAckAndReplay(t *testing.T) {
	s := &session{wake: make(chan struct{}), in: make(chan []byte, 8)}
	s.push([]byte(`["a"]`))
	s.push([]byte(`["b"]`))
	s.push([]byte(`["c"]`))
	fs, _, _ := s.after(0)
	if len(fs) != 2 || fs[0].id != 1 {
		t.Fatalf("after(0) = %+v", fs)
	}
	s.ack(1)
	fs, _, _ = s.after(-1)
	if len(fs) != 1 || fs[0].id != 2 {
		t.Fatalf("acked frames still pending: %+v", fs)
	}
	if got := string(renderFrames(fs)); got != `[[2,["c"]]]` {
		t.Fatalf("render = %s", got)
	}
}

func TestForwardRetryIsNotAppliedTwice(t *testing.T) {
	s := &session{wake: make(chan struct{}), in: make(chan []byte, 8)}
	s.ctx = t.Context()
	s.accept(0, [][]byte{[]byte("m0"), []byte("m1")})
	s.accept(1, [][]byte{[]byte("m1"), []byte("m2")}) // retry overlapping m1
	var got []string
	for len(s.in) > 0 {
		got = append(got, string(<-s.in))
	}
	if strings.Join(got, ",") != "m0,m1,m2" {
		t.Fatalf("delivered %v", got)
	}
}

func TestCORS(t *testing.T) {
	h := Handler(nil, Options{Origins: []string{"https://app.example"}, SessionIdle: time.Minute})
	pre := httptest.NewRequest(http.MethodOptions, "/v1/projects/p/databases/(default)/documents:commit", nil)
	pre.Header.Set("Origin", "https://app.example")
	pre.Header.Set("Access-Control-Request-Headers", "authorization,content-type")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, pre)
	if w.Code != http.StatusNoContent || w.Header().Get("Access-Control-Allow-Origin") != "https://app.example" ||
		w.Header().Get("Access-Control-Allow-Headers") != "authorization,content-type" {
		t.Fatalf("preflight: %d %v", w.Code, w.Header())
	}
	bad := httptest.NewRequest(http.MethodPost, "/v1/x:commit", strings.NewReader("{}"))
	bad.Header.Set("Origin", "https://evil.example")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, bad)
	if w.Code != http.StatusForbidden {
		t.Fatalf("foreign origin: %d", w.Code)
	}
}

func TestUnknownSession(t *testing.T) {
	h := Handler(nil, Options{Origins: []string{"*"}})
	r := httptest.NewRequest(http.MethodGet, "/google.firestore.v1.Firestore/Listen/channel?SID=nope&RID=rpc&CI=0&AID=0", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "Unknown SID") {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}
