package rules

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"
)

// Request is one access to decide: a single document operation, or a query.
type Request struct {
	Method string // get | list | create | update | delete
	// Path is the document's rules path. For a query it is the queried
	// collection plus an unknown document id (see QueryRequest).
	Path Path
	segs []pseg

	Auth     map[string]any // nil = signed out (request.auth == null)
	Time     time.Time
	Resource any // resource: *Resource, nil, or a symbolic resource
	New      any // request.resource: *Resource or nil
	Query    map[string]any

	Docs Getter
}

// Getter reads documents for get()/exists() (committed state) and
// getAfter()/existsAfter() (state after the request's writes).
type Getter interface {
	Get(ctx context.Context, p Path) (*Resource, error)
	GetAfter(ctx context.Context, p Path) (*Resource, error)
}

// pseg is a request path segment; unknown for a query's document id, any for
// a collection-group query's unknown ancestry.
type pseg struct {
	s       string
	unknown bool
	any     bool
}

// Budget caps document reads per request, shared by every evaluation of it
// (Firestore: 10 for a single document or query, 20 for batches).
type Budget struct {
	Limit int
	used  map[Path]*Resource
	after map[Path]*Resource
}

// NewBudget returns a read budget of limit distinct documents.
func NewBudget(limit int) *Budget {
	return &Budget{Limit: limit, used: map[Path]*Resource{}, after: map[Path]*Resource{}}
}

// DocRequest builds a single-document request. resource is the document as
// stored (nil if missing); newRes is request.resource for writes.
func DocRequest(method, name string, auth map[string]any, now time.Time, resource, newRes *Resource, docs Getter) *Request {
	r := &Request{Method: method, Path: PathFromName(name), Auth: auth, Time: now, Docs: docs}
	// Typed nils would make `resource == null` false.
	if resource != nil {
		r.Resource = resource
	}
	if newRes != nil {
		r.New = newRes
	}
	return r
}

// Decision is an evaluation's outcome.
type Decision struct {
	Allowed bool
	// Reason explains a denial for logs (never sent to clients verbatim).
	Reason string
}

// Evaluate decides a request.
func (rs *Ruleset) Evaluate(ctx context.Context, req *Request, b *Budget) Decision {
	if req.segs == nil {
		for _, s := range req.Path.segments() {
			req.segs = append(req.segs, pseg{s: s})
		}
	}
	if b == nil {
		b = NewBudget(10)
	}
	ev := &evaluator{ctx: ctx, rs: rs, req: req, budget: b}
	if ev.walk(rs.Root, req.segs, map[string]any{}) {
		return Decision{Allowed: true}
	}
	if ev.lastErr != "" {
		return Decision{Reason: ev.lastErr}
	}
	return Decision{Reason: "no allow statement matched"}
}

type evaluator struct {
	ctx     context.Context
	rs      *Ruleset
	req     *Request
	budget  *Budget
	lastErr string
}

func (ev *evaluator) walk(s *Scope, segs []pseg, binds map[string]any) bool {
	for _, child := range s.Children {
		for _, m := range ev.matchPrefix(child.Pattern, segs) {
			nb := make(map[string]any, len(binds)+len(m.binds))
			for k, v := range binds {
				nb[k] = v
			}
			for k, v := range m.binds {
				nb[k] = v
			}
			if m.consumed == len(segs) && ev.allows(child, nb) {
				return true
			}
			if m.consumed < len(segs) && ev.walk(child, segs[m.consumed:], nb) {
				return true
			}
		}
	}
	return false
}

type matchResult struct {
	consumed int
	binds    map[string]any
}

func (ev *evaluator) matchPrefix(pat []PatSeg, segs []pseg) []matchResult {
	var out []matchResult
	var rec func(pat []PatSeg, segs []pseg, consumed int, binds map[string]any)
	rec = func(pat []PatSeg, segs []pseg, consumed int, binds map[string]any) {
		if len(pat) == 0 {
			out = append(out, matchResult{consumed, binds})
			return
		}
		p := pat[0]
		with := func(k string, v any) map[string]any {
			nb := make(map[string]any, len(binds)+1)
			for kk, vv := range binds {
				nb[kk] = vv
			}
			nb[k] = v
			return nb
		}
		if p.Rec {
			min := 0
			if ev.rs.Version == "1" {
				min = 1
			}
			for k := min; k <= len(segs); k++ {
				var v any
				parts := make([]string, 0, k)
				for _, s := range segs[:k] {
					if s.unknown || s.any {
						v = Unknown
					}
					parts = append(parts, s.s)
				}
				if v == nil {
					v = Path("/" + strings.Join(parts, "/"))
				}
				rec(pat[1:], segs[k:], consumed+k, with(p.Var, v))
			}
			return
		}
		if len(segs) == 0 || segs[0].any {
			return
		}
		s := segs[0]
		if p.Var != "" {
			var v any = s.s
			if s.unknown {
				v = Unknown
			}
			rec(pat[1:], segs[1:], consumed+1, with(p.Var, v))
			return
		}
		if s.unknown || s.s != p.Lit {
			return
		}
		rec(pat[1:], segs[1:], consumed+1, binds)
	}
	rec(pat, segs, 0, map[string]any{})
	return out
}

func covers(method, want string) bool {
	switch method {
	case want:
		return true
	case "read":
		return want == "get" || want == "list"
	case "write":
		return want == "create" || want == "update" || want == "delete"
	}
	return false
}

func (ev *evaluator) allows(s *Scope, binds map[string]any) bool {
	for _, a := range s.Allows {
		ok := false
		for _, m := range a.Methods {
			if covers(m, ev.req.Method) {
				ok = true
			}
		}
		if !ok {
			continue
		}
		if a.Cond == nil {
			return true
		}
		v := ev.eval(a.Cond, &env{scope: s, binds: binds})
		if v == true {
			return true
		}
		if e, isE := v.(*Error); isE {
			ev.lastErr = "line " + strconv.Itoa(a.Line) + ": " + e.Msg
		}
	}
	return false
}

type env struct {
	scope  *Scope
	binds  map[string]any
	locals map[string]any
	depth  int
}

func (e *env) lookup(name string) (any, bool) {
	if v, ok := e.locals[name]; ok {
		return v, true
	}
	if v, ok := e.binds[name]; ok {
		return v, true
	}
	return nil, false
}

func (ev *evaluator) eval(x Expr, e *env) any {
	switch n := x.(type) {
	case Lit:
		return n.V
	case Ident:
		if v, ok := e.lookup(n.Name); ok {
			return v
		}
		switch n.Name {
		case "request":
			return ev.requestValue()
		case "resource":
			return ev.req.Resource
		}
		return errf("unknown identifier %s", n.Name)
	case Unary:
		return unaryOp(n.Op, ev.eval(n.X, e))
	case Binary:
		switch n.Op {
		case "&&":
			return ev.and(n, e)
		case "||":
			return ev.or(n, e)
		}
		return binaryOp(n.Op, ev.eval(n.X, e), ev.eval(n.Y, e))
	case Ternary:
		switch c := ev.eval(n.C, e).(type) {
		case bool:
			if c {
				return ev.eval(n.T, e)
			}
			return ev.eval(n.F, e)
		case *Error, unknown:
			return c
		default:
			return errf("ternary condition is %s, not bool", typeName(c))
		}
	case TypeIs:
		v := ev.eval(n.X, e)
		if isErr(v) || isUnknown(v) {
			return v
		}
		return isType(v, n.Type)
	case Member:
		return member(ev.eval(n.X, e), n.Name)
	case Index:
		return index(ev.eval(n.X, e), ev.eval(n.I, e))
	case Slice:
		var lo, hi any
		if n.Lo != nil {
			lo = ev.eval(n.Lo, e)
		}
		if n.Hi != nil {
			hi = ev.eval(n.Hi, e)
		}
		return slice(ev.eval(n.X, e), lo, hi)
	case ListLit:
		out := make([]any, len(n.Elems))
		for i, el := range n.Elems {
			v := ev.eval(el, e)
			if isErr(v) || isUnknown(v) {
				return v
			}
			out[i] = v
		}
		return out
	case MapLit:
		out := make(map[string]any, len(n.Keys))
		for i := range n.Keys {
			k := ev.eval(n.Keys[i], e)
			ks, ok := k.(string)
			if !ok {
				if isErr(k) || isUnknown(k) {
					return k
				}
				return errf("map key is %s, not string", typeName(k))
			}
			v := ev.eval(n.Vals[i], e)
			if isErr(v) || isUnknown(v) {
				return v
			}
			out[ks] = v
		}
		return out
	case PathLit:
		var b strings.Builder
		for _, part := range n.Parts {
			b.WriteByte('/')
			if part.X == nil {
				b.WriteString(part.Lit)
				continue
			}
			switch v := ev.eval(part.X, e).(type) {
			case string:
				b.WriteString(v)
			case int64:
				b.WriteString(strconv.FormatInt(v, 10))
			case Path:
				b.WriteString(strings.Trim(string(v), "/"))
			case *Error, unknown:
				return v
			default:
				return errf("cannot interpolate %s into a path", typeName(v))
			}
		}
		return Path(b.String())
	case Call:
		return ev.call(n, e)
	}
	return errf("unsupported expression %T", x)
}

func (ev *evaluator) and(n Binary, e *env) any {
	x := ev.eval(n.X, e)
	if x == false {
		return false
	}
	if _, ok := x.(bool); !ok && !isErr(x) && !isUnknown(x) {
		return errf("&& operand is %s, not bool", typeName(x))
	}
	y := ev.eval(n.Y, e)
	if y == false {
		return false
	}
	if _, ok := y.(bool); !ok && !isErr(y) && !isUnknown(y) {
		return errf("&& operand is %s, not bool", typeName(y))
	}
	if x == true && y == true {
		return true
	}
	if isErr(x) {
		return x
	}
	if isErr(y) {
		return y
	}
	return Unknown
}

func (ev *evaluator) or(n Binary, e *env) any {
	x := ev.eval(n.X, e)
	if x == true {
		return true
	}
	if _, ok := x.(bool); !ok && !isErr(x) && !isUnknown(x) {
		return errf("|| operand is %s, not bool", typeName(x))
	}
	y := ev.eval(n.Y, e)
	if y == true {
		return true
	}
	if _, ok := y.(bool); !ok && !isErr(y) && !isUnknown(y) {
		return errf("|| operand is %s, not bool", typeName(y))
	}
	if x == false && y == false {
		return false
	}
	if isErr(x) {
		return x
	}
	if isErr(y) {
		return y
	}
	return Unknown
}

func (ev *evaluator) requestValue() any {
	r := ev.req
	m := map[string]any{
		"method": r.Method,
		"path":   r.Path,
		"time":   r.Time,
	}
	if r.Auth != nil {
		m["auth"] = r.Auth
	} else {
		m["auth"] = nil
	}
	m["resource"] = r.New
	if r.Query != nil {
		m["query"] = r.Query
	}
	return m
}

var namespaces = map[string]bool{"math": true, "duration": true, "timestamp": true, "latlng": true, "hashing": true, "string": true}

func (ev *evaluator) call(n Call, e *env) any {
	evalArgs := func() ([]any, any) {
		out := make([]any, len(n.Args))
		for i, a := range n.Args {
			out[i] = ev.eval(a, e)
		}
		return out, nil
	}
	switch fn := n.Fn.(type) {
	case Ident:
		args, _ := evalArgs()
		for s := e.scope; s != nil; s = s.Parent {
			if f, ok := s.Funcs[fn.Name]; ok {
				return ev.callUser(f, s, args, e)
			}
		}
		for _, a := range args {
			if isErr(a) || isUnknown(a) {
				if fn.Name == "debug" {
					return a
				}
				return a
			}
		}
		return ev.global(fn.Name, args)
	case Member:
		if id, ok := fn.X.(Ident); ok && namespaces[id.Name] {
			if _, shadowed := e.lookup(id.Name); !shadowed {
				args, _ := evalArgs()
				for _, a := range args {
					if isErr(a) || isUnknown(a) {
						return a
					}
				}
				return namespaceCall(id.Name, fn.Name, args)
			}
		}
		recv := ev.eval(fn.X, e)
		args, _ := evalArgs()
		if isErr(recv) {
			return recv
		}
		for _, a := range args {
			if isErr(a) {
				return a
			}
		}
		return method(recv, fn.Name, args)
	}
	return errf("not callable")
}

func (ev *evaluator) callUser(f *Func, s *Scope, args []any, caller *env) any {
	if len(args) != len(f.Params) {
		return errf("%s takes %d arguments, got %d", f.Name, len(f.Params), len(args))
	}
	if caller.depth >= 20 {
		return errf("function call depth exceeded")
	}
	ne := &env{scope: s, binds: caller.binds, locals: map[string]any{}, depth: caller.depth + 1}
	for i, p := range f.Params {
		ne.locals[p] = args[i]
	}
	for _, l := range f.Lets {
		ne.locals[l.Name] = ev.eval(l.X, ne)
	}
	return ev.eval(f.Body, ne)
}

func (ev *evaluator) global(name string, args []any) any {
	switch name {
	case "get", "exists", "getAfter", "existsAfter":
		if len(args) != 1 {
			return errf("%s takes one path", name)
		}
		p, ok := args[0].(Path)
		if !ok {
			return errf("%s takes a path, got %s", name, typeName(args[0]))
		}
		res, err := ev.read(p, strings.HasSuffix(name, "After"))
		if err != nil {
			return err
		}
		if strings.HasPrefix(name, "exists") {
			return res != nil
		}
		if res == nil {
			return nil
		}
		return res
	case "debug":
		if len(args) == 1 {
			return args[0]
		}
	case "int":
		if len(args) == 1 {
			switch v := args[0].(type) {
			case int64:
				return v
			case float64:
				if math.IsNaN(v) || math.IsInf(v, 0) || v >= 9.223372036854775807e18 || v < -9.223372036854775808e18 {
					return errf("int() out of range")
				}
				return int64(v)
			case string:
				i, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return errf("int(): %q is not an integer", v)
				}
				return i
			}
		}
	case "float":
		if len(args) == 1 {
			switch v := args[0].(type) {
			case int64:
				return float64(v)
			case float64:
				return v
			case string:
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return errf("float(): %q is not a number", v)
				}
				return f
			}
		}
	case "string":
		if len(args) == 1 {
			return toString(args[0])
		}
	case "path":
		if len(args) == 1 {
			if s, ok := args[0].(string); ok {
				return Path("/" + strings.Trim(s, "/"))
			}
		}
	}
	return errf("unknown function %s/%d", name, len(args))
}

// read is the budgeted document read behind get() and friends.
func (ev *evaluator) read(p Path, after bool) (*Resource, *Error) {
	segs := p.segments()
	if len(segs) < 5 || segs[0] != "databases" || segs[2] != "documents" || len(segs)%2 == 0 {
		return nil, errf("%s is not a document path", p)
	}
	cache := ev.budget.used
	if after {
		cache = ev.budget.after
	}
	if r, ok := cache[p]; ok {
		return r, nil
	}
	if len(ev.budget.used)+len(ev.budget.after) >= ev.budget.Limit {
		return nil, errf("document read limit (%d) exceeded", ev.budget.Limit)
	}
	if ev.req.Docs == nil {
		return nil, errf("no document reader")
	}
	var r *Resource
	var err error
	if after {
		r, err = ev.req.Docs.GetAfter(ev.ctx, p)
	} else {
		r, err = ev.req.Docs.Get(ev.ctx, p)
	}
	if err != nil {
		return nil, errf("read %s: %v", p, err)
	}
	cache[p] = r
	return r, nil
}
