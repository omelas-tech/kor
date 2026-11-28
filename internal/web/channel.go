package web

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// WebChannel, as the Firebase JS SDK uses it (protocol version 8):
//
//   - POST without SID opens a session; the reply is frame 0, the "c" command
//     carrying the new SID. Messages ride the body as req<N>___data__=<json>.
//   - POST with SID is the forward channel: more client messages, numbered by
//     ofs so a retried request is not applied twice.
//   - GET with RID=rpc is the back channel: server frames, each [id, payload],
//     length-prefixed. CI=0 lets the response stream; CI=1 (a buffering proxy
//     was detected) answers once and the client polls again. AID acknowledges
//     frames already received, which are then dropped.
//   - GET with TYPE=terminate ends the session.
//
// A session is one Listen or Write gRPC stream to the client endpoint. Payloads
// are [message] for a stream message and [{"error": …}] when the stream fails,
// which is how the SDK learns the gRPC status.

type frame struct {
	id      int
	payload []byte
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

var webSessions = promauto.NewGauge(prometheus.GaugeOpts{
	Name: "kord_web_channel_sessions",
	Help: "Open WebChannel (browser Listen/Write) sessions.",
})

type session struct {
	id     string
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	out      []frame // unacknowledged frames, ascending id
	nextID   int
	wake     chan struct{} // closed and replaced whenever out grows or done flips
	done     bool
	nextOfs  int
	polling  int
	lastPoll time.Time
	in       chan []byte
}

func (s *session) push(payload []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return
	}
	s.out = append(s.out, frame{s.nextID, asciiJSON(payload)})
	s.nextID++
	close(s.wake)
	s.wake = make(chan struct{})
}

func (s *session) finish(payload []byte) {
	s.push(payload)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.done {
		s.done = true
		close(s.wake)
		s.wake = make(chan struct{})
	}
}

func (s *session) ack(aid int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 0
	for i < len(s.out) && s.out[i].id <= aid {
		i++
	}
	s.out = s.out[i:]
}

// after returns frames with id > last, the wake channel, and whether the
// session has ended.
func (s *session) after(last int) ([]frame, <-chan struct{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var fs []frame
	for _, f := range s.out {
		if f.id > last {
			fs = append(fs, f)
		}
	}
	return fs, s.wake, s.done
}

type sessions struct {
	opts Options
	mu   sync.Mutex
	m    map[string]*session
	once sync.Once
}

func newSessions(opts Options) *sessions { return &sessions{opts: opts, m: map[string]*session{}} }

func (ss *sessions) get(id string) *session {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	return ss.m[id]
}

func (ss *sessions) add(s *session) {
	ss.once.Do(func() { go ss.reap() })
	ss.mu.Lock()
	ss.m[s.id] = s
	ss.mu.Unlock()
	webSessions.Inc()
}

func (ss *sessions) remove(id string) {
	ss.mu.Lock()
	s := ss.m[id]
	delete(ss.m, id)
	ss.mu.Unlock()
	if s != nil {
		s.cancel()
		webSessions.Dec()
	}
}

// reap closes sessions nobody polls any more: the browser tab went away
// without terminating.
func (ss *sessions) reap() {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for range t.C {
		var idle []string
		ss.mu.Lock()
		for id, s := range ss.m {
			s.mu.Lock()
			if s.polling == 0 && time.Since(s.lastPoll) > ss.opts.SessionIdle {
				idle = append(idle, id)
			}
			s.mu.Unlock()
		}
		ss.mu.Unlock()
		for _, id := range idle {
			ss.remove(id)
		}
	}
}

func newSID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func (g *gateway) channel(w http.ResponseWriter, r *http.Request, method string) {
	q := r.URL.Query()
	if strings.HasSuffix(r.URL.Path, "/channel/test") {
		g.channelTest(w, q)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache, no-store, max-age=0, must-revalidate")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	sid := q.Get("SID")
	switch {
	case r.Method == http.MethodPost && sid == "":
		g.open(w, r, q, method)
	case r.Method == http.MethodPost:
		g.forward(w, r, q, sid)
	case q.Get("TYPE") == "terminate":
		g.sessions.remove(sid)
		w.WriteHeader(http.StatusOK)
	case q.Get("RID") == "rpc":
		g.back(w, r, q, sid)
	default:
		http.Error(w, "unsupported WebChannel request", http.StatusBadRequest)
	}
}

// channelTest answers the optional connection test: the init request asks for
// host prefixes (none), the second checks that chunks arrive unbuffered.
func (g *gateway) channelTest(w http.ResponseWriter, q url.Values) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if q.Get("MODE") == "init" {
		_, _ = io.WriteString(w, `["b",""]`)
		return
	}
	_, _ = io.WriteString(w, "11111")
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	time.Sleep(200 * time.Millisecond)
	_, _ = io.WriteString(w, "2")
}

// parseHeaders decodes the $httpHeaders parameter: "Name:value\r\n" lines.
func parseHeaders(s string) map[string]string {
	h := map[string]string{}
	for _, line := range strings.Split(s, "\r\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			h[strings.ToLower(strings.TrimSpace(k))] = strings.TrimSpace(v)
		}
	}
	return h
}

// messages extracts req<N>___data__ payloads from a forward-channel body,
// with the offset of the first one. The opening request also carries the
// SDK's headers (Authorization among them) as a "headers" field.
func messages(r *http.Request) (int, [][]byte, url.Values, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 16<<20))
	if err != nil {
		return 0, nil, nil, err
	}
	form, err := url.ParseQuery(string(body))
	if err != nil {
		return 0, nil, nil, err
	}
	count, _ := strconv.Atoi(form.Get("count"))
	ofs, _ := strconv.Atoi(form.Get("ofs"))
	var out [][]byte
	for i := 0; i < count; i++ {
		v := form.Get(fmt.Sprintf("req%d___data__", i))
		if v == "" {
			return 0, nil, nil, fmt.Errorf("message %d has no __data__", i)
		}
		out = append(out, []byte(v))
	}
	return ofs, out, form, nil
}

func (s *session) accept(ofs int, msgs [][]byte) {
	s.mu.Lock()
	var fresh [][]byte
	for i, m := range msgs {
		if ofs+i >= s.nextOfs {
			fresh = append(fresh, m)
		}
	}
	if ofs+len(msgs) > s.nextOfs {
		s.nextOfs = ofs + len(msgs)
	}
	s.mu.Unlock()
	for _, m := range fresh {
		select {
		case s.in <- m:
		case <-s.ctx.Done():
			return
		}
	}
}

func writeChunk(w io.Writer, body []byte) error {
	_, err := fmt.Fprintf(w, "%d\n%s", len(body), body)
	return err
}

func (g *gateway) open(w http.ResponseWriter, r *http.Request, q url.Values, method string) {
	ofs, msgs, form, err := messages(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	authz := parseHeaders(form.Get("headers"))["authorization"]
	if authz == "" {
		authz = parseHeaders(q.Get("$httpHeaders"))["authorization"]
	}
	if authz == "" {
		authz = r.Header.Get("Authorization")
	}
	ctx, cancel := context.WithCancel(outgoing(context.Background(), authz))
	s := &session{id: newSID(), ctx: ctx, cancel: cancel, wake: make(chan struct{}), in: make(chan []byte, 256), lastPoll: time.Now()}
	s.push([]byte(fmt.Sprintf(`["c",%q,"",8,14,%d]`, s.id, g.opts.Noop.Milliseconds())))
	if err := g.startStream(ctx, s, method); err != nil {
		cancel()
		writeError(w, err)
		return
	}
	g.sessions.add(s)
	s.accept(ofs, msgs)
	w.Header().Set("X-HTTP-Session-Id", s.id)
	frames, _, _ := s.after(-1)
	_ = writeChunk(w, renderFrames(frames[:1]))
	// Frame 0 travelled on this response; the back channel starts after it.
	s.ack(0)
}

func (g *gateway) forward(w http.ResponseWriter, r *http.Request, q url.Values, sid string) {
	s := g.sessions.get(sid)
	if s == nil {
		http.Error(w, "Unknown SID", http.StatusBadRequest)
		return
	}
	if aid, err := strconv.Atoi(q.Get("AID")); err == nil {
		s.ack(aid)
	}
	ofs, msgs, _, err := messages(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.accept(ofs, msgs)
	s.mu.Lock()
	// [backChannelPresent, lastArrayId, outstandingBytes]. Always claim the
	// back channel: in long-poll mode it is legitimately absent between polls,
	// and "missing" makes the client tear it down and reopen it.
	back := 1
	last := s.nextID - 1
	outstanding := 0
	for _, f := range s.out {
		outstanding += len(f.payload)
	}
	s.mu.Unlock()
	_ = writeChunk(w, []byte(fmt.Sprintf("[%d,%d,%d]", back, last, outstanding)))
}

func renderFrames(fs []frame) []byte {
	var b strings.Builder
	b.WriteString("[")
	for i, f := range fs {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "[%d,%s]", f.id, f.payload)
	}
	b.WriteString("]")
	return []byte(b.String())
}

func (g *gateway) back(w http.ResponseWriter, r *http.Request, q url.Values, sid string) {
	s := g.sessions.get(sid)
	if s == nil {
		http.Error(w, "Unknown SID", http.StatusBadRequest)
		return
	}
	last := -1
	if aid, err := strconv.Atoi(q.Get("AID")); err == nil {
		s.ack(aid)
		last = aid
	}
	buffered := q.Get("CI") == "1"
	s.mu.Lock()
	s.polling++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.polling--
		s.lastPoll = time.Now()
		s.mu.Unlock()
	}()
	flusher, _ := w.(http.Flusher)
	hang := time.NewTimer(g.opts.Hang)
	defer hang.Stop()
	noop := time.NewTicker(g.opts.Noop)
	defer noop.Stop()
	wrote := false
	for {
		frames, wake, done := s.after(last)
		if len(frames) > 0 {
			if err := writeChunk(w, renderFrames(frames)); err != nil {
				return
			}
			if flusher != nil {
				flusher.Flush()
			}
			last = frames[len(frames)-1].id
			wrote = true
			if buffered {
				return
			}
			continue
		}
		if done {
			if !wrote {
				w.WriteHeader(http.StatusOK)
			}
			g.sessions.remove(s.id)
			return
		}
		select {
		case <-wake:
		case <-noop.C:
			s.push([]byte(`["noop"]`))
		case <-hang.C:
			if !wrote {
				w.WriteHeader(http.StatusOK)
			}
			return
		case <-r.Context().Done():
			return
		}
	}
}

// startStream opens the gRPC stream behind a session and pumps both ways.
func (g *gateway) startStream(ctx context.Context, s *session, method string) error {
	type stream interface {
		Recv() (proto.Message, error)
		Send([]byte) error
		CloseSend() error
	}
	var st stream
	switch method {
	case "Listen":
		c, err := g.client.Listen(ctx)
		if err != nil {
			return err
		}
		st = listenStream{c}
	case "Write":
		c, err := g.client.Write(ctx)
		if err != nil {
			return err
		}
		st = writeAdapter{c}
	default:
		return status.Errorf(codes.Unimplemented, "no %s channel", method)
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-s.in:
				if err := st.Send(m); err != nil {
					if status.Code(err) == codes.InvalidArgument {
						s.finish(wrap(mustJSON(errorBody(err))))
						s.cancel()
					}
					return
				}
			}
		}
	}()
	go func() {
		for {
			m, err := st.Recv()
			if err != nil {
				switch {
				case errors.Is(err, io.EOF):
					s.finish([]byte(`["close"]`))
				case status.Code(err) == codes.Canceled && ctx.Err() != nil:
					// Session closed from our side; nobody is listening.
				default:
					s.finish(wrap(mustJSON(errorBody(err))))
				}
				return
			}
			b, err := marshal.Marshal(m)
			if err != nil {
				s.finish(wrap(mustJSON(errorBody(status.Error(codes.Internal, err.Error())))))
				s.cancel()
				return
			}
			s.push(wrap(b))
		}
	}()
	return nil
}

// wrap makes a stream message the [message] payload the SDK reads data[0] of.
func wrap(b []byte) []byte {
	out := make([]byte, 0, len(b)+2)
	out = append(out, '[')
	out = append(out, b...)
	return append(out, ']')
}

type listenStream struct {
	c pb.Firestore_ListenClient
}

func (l listenStream) Recv() (proto.Message, error) { return l.c.Recv() }
func (l listenStream) CloseSend() error             { return l.c.CloseSend() }
func (l listenStream) Send(b []byte) error {
	req := &pb.ListenRequest{}
	if err := unmarshal.Unmarshal(b, req); err != nil {
		return status.Errorf(codes.InvalidArgument, "bad Listen request: %v", err)
	}
	return l.c.Send(req)
}

type writeAdapter struct {
	c pb.Firestore_WriteClient
}

func (w writeAdapter) Recv() (proto.Message, error) { return w.c.Recv() }
func (w writeAdapter) CloseSend() error             { return w.c.CloseSend() }
func (w writeAdapter) Send(b []byte) error {
	req := &pb.WriteRequest{}
	if err := unmarshal.Unmarshal(b, req); err != nil {
		return status.Errorf(codes.InvalidArgument, "bad Write request: %v", err)
	}
	return w.c.Send(req)
}
