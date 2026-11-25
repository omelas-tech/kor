// Package feed serves Kor's change feed over HTTP, for consumers that are not
// Go programs sharing kord's database — the functions runtime first of all.
//
// It is mounted on kord's admin listener (the one serving /metrics), which
// binds loopback by default. Like the gRPC port it has no authentication:
// reachability is authorization, so keep it private.
//
//	GET  /feed/v1/changes?after=XID.SEQ&collection=a&collection=b&images=1&limit=500
//	GET  /feed/v1/head
//	GET  /feed/v1/images
//	POST /feed/v1/images   {"register": ["a"], "unregister": ["b"]}
//
// Documents in before/after are google.firestore.v1.Document in the REST JSON
// encoding (protojson): the shape Firestore's own trigger events use, so a
// consumer can hand them to the Firebase SDKs unchanged.
package feed

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/omelas-tech/kor/internal/store"
)

// MaxLimit caps one page. Images make rows large; a consumer that wants more
// asks again.
const MaxLimit = 2000

// Change is the wire form of store.Change.
type Change struct {
	Seq        int64           `json:"seq"`
	XID        string          `json:"xid"`
	Name       string          `json:"name"`
	Collection string          `json:"collection"`
	Kind       string          `json:"kind"`
	At         time.Time       `json:"at"`
	HasImages  bool            `json:"hasImages"`
	Before     json.RawMessage `json:"before,omitempty"`
	After      json.RawMessage `json:"after,omitempty"`
}

// Page is one response from /feed/v1/changes. Cursor is where the next request
// resumes: the last returned event, or the request's own cursor when empty.
type Page struct {
	Changes []Change `json:"changes"`
	Cursor  string   `json:"cursor"`
}

// FormatCursor renders a cursor as XID.SEQ. XID crosses the wire as a string
// because it does not fit a JSON number's safe-integer range.
func FormatCursor(c store.Cursor) string { return fmt.Sprintf("%d.%d", c.XID, c.Seq) }

// ParseCursor accepts XID.SEQ, or "" for the start of the feed.
func ParseCursor(s string) (store.Cursor, error) {
	if s == "" {
		return store.Cursor{}, nil
	}
	x, q, ok := strings.Cut(s, ".")
	if !ok {
		return store.Cursor{}, fmt.Errorf("cursor %q: want XID.SEQ", s)
	}
	xid, err := strconv.ParseUint(x, 10, 64)
	if err != nil {
		return store.Cursor{}, fmt.Errorf("cursor %q: %w", s, err)
	}
	seq, err := strconv.ParseInt(q, 10, 64)
	if err != nil {
		return store.Cursor{}, fmt.Errorf("cursor %q: %w", s, err)
	}
	return store.Cursor{XID: xid, Seq: seq}, nil
}

// Handler returns the feed API. Mount it at the root of a mux; it only claims
// /feed/v1/.
func Handler(st *store.Store, log *slog.Logger) http.Handler {
	h := &handler{st: st, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /feed/v1/changes", h.changes)
	mux.HandleFunc("GET /feed/v1/head", h.head)
	mux.HandleFunc("GET /feed/v1/images", h.images)
	mux.HandleFunc("POST /feed/v1/images", h.setImages)
	return mux
}

type handler struct {
	st  *store.Store
	log *slog.Logger
}

func (h *handler) changes(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	after, err := ParseCursor(q.Get("after"))
	if err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	limit := 500
	if v := q.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit <= 0 {
			httpError(w, http.StatusBadRequest, fmt.Errorf("limit %q: want a positive integer", v))
			return
		}
	}
	limit = min(limit, MaxLimit)
	images := q.Get("images") == "1" || q.Get("images") == "true"
	var colls []string
	for _, c := range q["collection"] {
		for _, part := range strings.Split(c, ",") {
			if part = strings.TrimSpace(part); part != "" {
				colls = append(colls, part)
			}
		}
	}
	rows, err := h.st.ReadFeed(r.Context(), after, colls, images, limit)
	if err != nil {
		h.log.Error("feed: read", "err", err)
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	page := Page{Changes: make([]Change, 0, len(rows)), Cursor: FormatCursor(after)}
	for _, c := range rows {
		out := Change{
			Seq: c.Seq, XID: strconv.FormatUint(c.XID, 10), Name: c.DocName,
			Collection: c.CollectionID, Kind: c.Kind, At: c.At.UTC(), HasImages: c.HasImages,
		}
		if out.Before, err = docJSON(c.Before); err == nil {
			out.After, err = docJSON(c.After)
		}
		if err != nil {
			httpError(w, http.StatusInternalServerError, fmt.Errorf("encode change %d: %w", c.Seq, err))
			return
		}
		page.Changes = append(page.Changes, out)
		page.Cursor = FormatCursor(store.Cursor{XID: c.XID, Seq: c.Seq})
	}
	writeJSON(w, page)
}

func (h *handler) head(w http.ResponseWriter, r *http.Request) {
	c, err := h.st.FeedHead(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, map[string]string{"cursor": FormatCursor(c)})
}

func (h *handler) images(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]any{"collections": nonNil(h.st.FeedImageCollections())})
}

// setImages registers/unregisters collections and returns the registered set
// plus the head cursor taken AFTER the change — the safe starting point for a
// consumer that needs images on every event it reads.
func (h *handler) setImages(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Register   []string `json:"register"`
		Unregister []string `json:"unregister"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, err)
		return
	}
	var err error
	if len(req.Register) > 0 {
		if _, err = h.st.RegisterFeedImages(r.Context(), req.Register); err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
	}
	if len(req.Unregister) > 0 {
		if _, err = h.st.UnregisterFeedImages(r.Context(), req.Unregister); err != nil {
			httpError(w, http.StatusBadRequest, err)
			return
		}
	}
	head, err := h.st.FeedHead(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err)
		return
	}
	h.log.Info("feed images updated", "register", req.Register, "unregister", req.Unregister,
		"collections", h.st.FeedImageCollections())
	writeJSON(w, map[string]any{"collections": nonNil(h.st.FeedImageCollections()), "head": FormatCursor(head)})
}

var marshal = protojson.MarshalOptions{UseProtoNames: false}

func docJSON(d *pb.Document) (json.RawMessage, error) {
	if d == nil {
		return nil, nil
	}
	b, err := marshal.Marshal(d)
	return json.RawMessage(b), err
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}
