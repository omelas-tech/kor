// Package web lets the Firebase JS SDK in a browser talk to kord.
//
// Browsers cannot speak gRPC, so the web SDK uses two other transports:
//
//   - one-shot RPCs (Commit, BatchGetDocuments, RunQuery, RunAggregationQuery,
//     BeginTransaction, Rollback) as JSON over HTTP — Firestore's REST API —
//     at /v1/{resource}:{method};
//   - the Listen and Write streams over WebChannel, Google's long-polling /
//     streaming HTTP protocol, at /google.firestore.v1.Firestore/{Listen,Write}/channel.
//
// This package translates both into gRPC calls against kord's client endpoint,
// forwarding the caller's ID token, so a browser request goes through exactly
// the authentication and security rules a mobile SDK's does. It holds no
// authority of its own.
package web

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/genproto/googleapis/rpc/code"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Options configures the gateway.
type Options struct {
	// Origins allowed to call it from a browser, e.g. https://example.com.
	// "*" allows any origin (development only: ID tokens are credentials).
	Origins []string
	Log     *slog.Logger
	// SessionIdle closes a WebChannel session nobody has polled for this
	// long. Default 90s.
	SessionIdle time.Duration
	// Hang bounds one back-channel response; the client re-polls. Default 50s.
	Hang time.Duration
	// Noop is the keepalive interval on an open back channel. Default 20s.
	Noop time.Duration
}

// Handler serves the REST and WebChannel transports, forwarding to client.
func Handler(client pb.FirestoreClient, opts Options) http.Handler {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.SessionIdle == 0 {
		opts.SessionIdle = 90 * time.Second
	}
	if opts.Hang == 0 {
		opts.Hang = 50 * time.Second
	}
	if opts.Noop == 0 {
		opts.Noop = 20 * time.Second
	}
	g := &gateway{client: client, opts: opts, origins: map[string]bool{}, sessions: newSessions(opts)}
	for _, o := range opts.Origins {
		g.origins[strings.TrimSuffix(o, "/")] = true
	}
	return g
}

type gateway struct {
	client   pb.FirestoreClient
	opts     Options
	origins  map[string]bool
	sessions *sessions
}

func (g *gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !g.cors(w, r) {
		return
	}
	switch p := r.URL.Path; {
	case strings.HasPrefix(p, "/v1/"):
		g.rest(w, r)
	case strings.HasPrefix(p, "/google.firestore.v1.Firestore/Listen/channel"):
		g.channel(w, r, "Listen")
	case strings.HasPrefix(p, "/google.firestore.v1.Firestore/Write/channel"):
		g.channel(w, r, "Write")
	default:
		http.NotFound(w, r)
	}
}

// cors answers preflights and stamps responses. False means the request was
// fully handled (a preflight, or a refused origin).
func (g *gateway) cors(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" {
		if !g.origins["*"] && !g.origins[origin] {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return false
		}
		h := w.Header()
		h.Set("Access-Control-Allow-Origin", origin)
		h.Set("Access-Control-Allow-Credentials", "true")
		h.Set("Access-Control-Expose-Headers", "X-HTTP-Session-Id")
		h.Add("Vary", "Origin")
	}
	if r.Method == http.MethodOptions {
		h := w.Header()
		h.Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if req := r.Header.Get("Access-Control-Request-Headers"); req != "" {
			h.Set("Access-Control-Allow-Headers", req)
		}
		h.Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return false
	}
	return true
}

// outgoing builds the gRPC context for a caller: their ID token, nothing else.
func outgoing(ctx context.Context, authorization string) context.Context {
	if authorization == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
}

// --- REST --------------------------------------------------------------------

var unmarshal = protojson.UnmarshalOptions{DiscardUnknown: true}
var marshal = protojson.MarshalOptions{}

func (g *gateway) rest(w http.ResponseWriter, r *http.Request) {
	ctx := outgoing(r.Context(), r.Header.Get("Authorization"))
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	if r.Method == http.MethodGet {
		doc, err := g.client.GetDocument(ctx, &pb.GetDocumentRequest{Name: path})
		writeProto(w, doc, err)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	resource, method, ok := cutLast(path, ":")
	if !ok {
		http.NotFound(w, r)
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		writeError(w, status.Error(codes.InvalidArgument, err.Error()))
		return
	}
	if len(body) == 0 {
		body = []byte("{}")
	}
	parse := func(m proto.Message) bool {
		if err := unmarshal.Unmarshal(body, m); err != nil {
			writeError(w, status.Errorf(codes.InvalidArgument, "bad request body: %v", err))
			return false
		}
		return true
	}
	switch method {
	case "commit":
		req := &pb.CommitRequest{}
		if parse(req) {
			req.Database = resource
			res, err := g.client.Commit(ctx, req)
			writeProto(w, res, err)
		}
	case "beginTransaction":
		req := &pb.BeginTransactionRequest{}
		if parse(req) {
			req.Database = resource
			res, err := g.client.BeginTransaction(ctx, req)
			writeProto(w, res, err)
		}
	case "rollback":
		req := &pb.RollbackRequest{}
		if parse(req) {
			req.Database = resource
			res, err := g.client.Rollback(ctx, req)
			writeProto(w, res, err)
		}
	case "batchGet":
		req := &pb.BatchGetDocumentsRequest{}
		if parse(req) {
			req.Database = resource
			s, err := g.client.BatchGetDocuments(ctx, req)
			writeStream(w, err, func() (proto.Message, error) { return s.Recv() })
		}
	case "runQuery":
		req := &pb.RunQueryRequest{}
		if parse(req) {
			req.Parent = resource
			s, err := g.client.RunQuery(ctx, req)
			writeStream(w, err, func() (proto.Message, error) { return s.Recv() })
		}
	case "runAggregationQuery":
		req := &pb.RunAggregationQueryRequest{}
		if parse(req) {
			req.Parent = resource
			s, err := g.client.RunAggregationQuery(ctx, req)
			writeStream(w, err, func() (proto.Message, error) { return s.Recv() })
		}
	default:
		writeError(w, status.Errorf(codes.Unimplemented, "kor: %s is not available over REST", method))
	}
}

func cutLast(s, sep string) (string, string, bool) {
	i := strings.LastIndex(s, sep)
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+len(sep):], true
}

func writeProto(w http.ResponseWriter, m proto.Message, err error) {
	if err != nil {
		writeError(w, err)
		return
	}
	b, err := marshal.Marshal(m)
	if err != nil {
		writeError(w, status.Error(codes.Internal, err.Error()))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = w.Write(b)
}

// writeStream renders a server stream as the REST API does: one JSON array.
// The whole stream is read before anything is written, so a mid-stream error
// is a proper error response rather than a truncated array.
func writeStream(w http.ResponseWriter, err error, recv func() (proto.Message, error)) {
	if err != nil {
		writeError(w, err)
		return
	}
	var parts [][]byte
	for {
		m, err := recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeError(w, err)
			return
		}
		b, err := marshal.Marshal(m)
		if err != nil {
			writeError(w, status.Error(codes.Internal, err.Error()))
			return
		}
		parts = append(parts, b)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_, _ = io.WriteString(w, "[")
	for i, p := range parts {
		if i > 0 {
			_, _ = io.WriteString(w, ",")
		}
		_, _ = w.Write(p)
	}
	_, _ = io.WriteString(w, "]")
}

// errorBody is Google's JSON error shape, which the SDKs map back to a code.
func errorBody(err error) map[string]any {
	st := status.Convert(err)
	return map[string]any{"error": map[string]any{
		"code":    httpStatus(st.Code()),
		"message": st.Message(),
		"status":  code.Code_name[int32(st.Code())],
	}}
}

func writeError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(httpStatus(status.Code(err)))
	_ = json.NewEncoder(w).Encode(errorBody(err))
}

func httpStatus(c codes.Code) int {
	switch c {
	case codes.OK:
		return 200
	case codes.InvalidArgument, codes.FailedPrecondition, codes.OutOfRange:
		return 400
	case codes.Unauthenticated:
		return 401
	case codes.PermissionDenied:
		return 403
	case codes.NotFound:
		return 404
	case codes.AlreadyExists, codes.Aborted:
		return 409
	case codes.ResourceExhausted:
		return 429
	case codes.Canceled:
		return 499
	case codes.Unimplemented:
		return 501
	case codes.Unavailable:
		return 503
	case codes.DeadlineExceeded:
		return 504
	}
	return 500
}

// asciiJSON escapes every non-ASCII rune as \uXXXX. WebChannel frames carry a
// length the client compares against JavaScript string length (UTF-16 code
// units); with pure ASCII that equals the byte length.
func asciiJSON(b []byte) []byte {
	ascii := true
	for _, c := range b {
		if c >= utf8.RuneSelf {
			ascii = false
			break
		}
	}
	if ascii {
		return b
	}
	out := make([]byte, 0, len(b)+16)
	for len(b) > 0 {
		r, n := utf8.DecodeRune(b)
		b = b[n:]
		if r < utf8.RuneSelf {
			out = append(out, byte(r))
			continue
		}
		if r > 0xFFFF {
			a, z := utf16.EncodeRune(r)
			out = fmt.Appendf(out, `\u%04x\u%04x`, a, z)
		} else {
			out = fmt.Appendf(out, `\u%04x`, r)
		}
	}
	return out
}
