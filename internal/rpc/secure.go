package rpc

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/omelas-tech/kor/internal/auth"
	"github.com/omelas-tech/kor/internal/query"
	"github.com/omelas-tech/kor/internal/rules"
	"github.com/omelas-tech/kor/internal/store"
)

// security is what makes a Server a client endpoint: every request carries a
// verified Firebase identity (or none) and is decided by the security rules.
// A Server without it is the trusted admin endpoint, like Firestore's own
// service-account access, and must only be reachable by trusted callers.
type security struct {
	rules    atomic.Pointer[rules.Ruleset]
	verifier *auth.Verifier
	// prefix is "projects/<p>/databases/" — requests for any other project are
	// refused: an ID token is only valid for its own project.
	prefix string
	log    *slog.Logger
}

var rulesDecisions = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "kord_rules_decisions_total", Help: "Security-rules decisions on the client endpoint.",
}, []string{"method", "result"})

// NewClient returns the client-facing endpoint for the same store and change
// hub as trusted: identical API, every request authenticated and authorized.
func NewClient(trusted *Server, rs *rules.Ruleset, v *auth.Verifier, project string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	sec := &security{verifier: v, prefix: "projects/" + project + "/databases/", log: log}
	sec.rules.Store(rs)
	return &Server{store: trusted.store, hub: trusted.hub, sec: sec}
}

// SetRules swaps the ruleset of a client endpoint (a rules file reload).
// Requests already being decided finish on the ruleset they started with.
func (s *Server) SetRules(rs *rules.Ruleset) {
	if s.sec != nil && rs != nil {
		s.sec.rules.Store(rs)
	}
}

var errDenied = status.Error(codes.PermissionDenied, "Missing or insufficient permissions.")

// identity extracts and verifies the caller's ID token: nil auth for a
// signed-out caller, UNAUTHENTICATED for a token that does not verify.
func (sec *security) identity(ctx context.Context) (map[string]any, time.Time, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	var raw string
	for _, v := range md.Get("authorization") {
		if t, ok := strings.CutPrefix(v, "Bearer "); ok {
			raw = strings.TrimSpace(t)
		}
	}
	if raw == "" {
		return nil, time.Time{}, nil
	}
	a, err := sec.verifier.Verify(ctx, raw)
	if err != nil {
		if errors.Is(err, auth.ErrInvalid) {
			return nil, time.Time{}, status.Error(codes.Unauthenticated, "invalid or expired ID token")
		}
		return nil, time.Time{}, status.Error(codes.Unavailable, "cannot verify ID tokens right now")
	}
	var exp time.Time
	if tok, ok := a["token"].(map[string]any); ok {
		if e, ok := tok["exp"].(int64); ok {
			exp = time.Unix(e, 0)
		}
	}
	// Emulator-style unsigned tokens carry exp 3600 (1970) and Verify skips
	// the expiry check for them; arming the stream timer on it would end
	// every stream at once.
	if sec.verifier.Unsigned() && !exp.IsZero() && exp.Before(time.Now()) {
		exp = time.Time{}
	}
	return a, exp, nil
}

func (sec *security) checkName(name string) error {
	if !strings.HasPrefix(name, sec.prefix) {
		return status.Error(codes.PermissionDenied, "kor: this endpoint serves one project")
	}
	return nil
}

func (sec *security) decide(ctx context.Context, req *rules.Request, b *rules.Budget) bool {
	d := sec.rules.Load().Evaluate(ctx, req, b)
	result := "deny"
	if d.Allowed {
		result = "allow"
	} else {
		sec.log.Debug("rules: denied", "method", req.Method, "path", req.Path, "reason", d.Reason)
	}
	rulesDecisions.WithLabelValues(req.Method, result).Inc()
	return d.Allowed
}

// storeDocs serves get()/exists() for read requests (no batch to look after).
type storeDocs struct {
	st     *store.Store
	prefix string // "projects/<p>"
}

func (d storeDocs) name(p rules.Path) string { return d.prefix + string(p) }

func (d storeDocs) Get(ctx context.Context, p rules.Path) (*rules.Resource, error) {
	docs, err := d.st.GetDocuments(ctx, []string{d.name(p)})
	if err != nil {
		return nil, err
	}
	if doc, ok := docs[d.name(p)]; ok {
		return rules.NewResource(doc.Name, doc.Fields), nil
	}
	return nil, nil
}

func (d storeDocs) GetAfter(ctx context.Context, p rules.Path) (*rules.Resource, error) {
	return d.Get(ctx, p)
}

// guardDocs serves get() and getAfter() inside a commit.
type guardDocs struct {
	r      store.GuardReader
	prefix string
}

func (g guardDocs) read(ctx context.Context, p rules.Path, after bool) (*rules.Resource, error) {
	name := g.prefix + string(p)
	var doc *store.Doc
	var err error
	if after {
		doc, err = g.r.GetAfter(ctx, name)
	} else {
		doc, err = g.r.Get(ctx, name)
	}
	if err != nil || doc == nil {
		return nil, err
	}
	return rules.NewResource(doc.Name, doc.Fields), nil
}

func (g guardDocs) Get(ctx context.Context, p rules.Path) (*rules.Resource, error) {
	return g.read(ctx, p, false)
}
func (g guardDocs) GetAfter(ctx context.Context, p rules.Path) (*rules.Resource, error) {
	return g.read(ctx, p, true)
}

func (sec *security) projectPrefix() string {
	return strings.TrimSuffix(sec.prefix, "/databases/")
}

func resourceOf(d *store.Doc) *rules.Resource {
	if d == nil {
		return nil
	}
	return rules.NewResource(d.Name, d.Fields)
}

// canGet decides a single-document read.
func (sec *security) canGet(ctx context.Context, st *store.Store, a map[string]any, name string, doc *store.Doc, b *rules.Budget) bool {
	req := rules.DocRequest("get", name, a, time.Now().UTC(), resourceOf(doc), nil,
		storeDocs{st: st, prefix: sec.projectPrefix()})
	return sec.decide(ctx, req, b)
}

// canList decides a query (symbolically: see rules/query.go).
func (sec *security) canList(ctx context.Context, st *store.Store, a map[string]any, q *query.Query) bool {
	req := rules.QueryRequest(q, a, time.Now().UTC())
	req.Docs = storeDocs{st: st, prefix: sec.projectPrefix()}
	return sec.decide(ctx, req, rules.NewBudget(10))
}

// canListDoc is the per-document second check on a query's results. The
// symbolic check already proved the rule for every document the query can
// return; this re-checks each one concretely, so a gap in that proof can
// only ever deny, never leak.
func (sec *security) canListDoc(ctx context.Context, st *store.Store, a map[string]any, q *query.Query, doc *store.Doc) bool {
	req := rules.DocRequest("list", doc.Name, a, time.Now().UTC(), resourceOf(doc), nil,
		storeDocs{st: st, prefix: sec.projectPrefix()})
	qr := rules.QueryRequest(q, a, req.Time)
	req.Query = qr.Query
	return sec.decide(ctx, req, rules.NewBudget(10))
}

// guard is the commit guard enforcing write rules for one caller.
func (sec *security) guard(a map[string]any) store.Guard {
	return func(ctx context.Context, commitTime time.Time, docs []store.GuardDoc, r store.GuardReader) error {
		limit := 10
		if len(docs) > 1 {
			limit = 20
		}
		b := rules.NewBudget(limit)
		g := guardDocs{r: r, prefix: sec.projectPrefix()}
		for _, d := range docs {
			if err := sec.checkName(d.Name); err != nil {
				return err
			}
			method := "update"
			switch {
			case d.Before == nil && d.After != nil:
				method = "create"
			case d.After == nil:
				method = "delete"
			}
			req := rules.DocRequest(method, d.Name, a, commitTime, resourceOf(d.Before), resourceOf(d.After), g)
			if !sec.decide(ctx, req, b) {
				return errDenied
			}
		}
		return nil
	}
}

// authorizeQuery is the shared front of RunQuery and RunAggregationQuery.
func (s *Server) authorizeQuery(ctx context.Context, parent string, sq *pb.StructuredQuery) (map[string]any, *query.Query, error) {
	a, _, err := s.sec.identity(ctx)
	if err != nil {
		return nil, nil, err
	}
	if err := s.sec.checkName(parent); err != nil {
		return nil, nil, err
	}
	q, err := query.Parse(parent, sq)
	if err != nil {
		return nil, nil, err
	}
	if !s.sec.canList(ctx, s.store, a, q) {
		return nil, nil, errDenied
	}
	return a, q, nil
}
