package e2e

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/omelas-tech/kor/internal/auth"
	"github.com/omelas-tech/kor/internal/pgtest"
	"github.com/omelas-tech/kor/internal/rpc"
	"github.com/omelas-tech/kor/internal/rules"
	"github.com/omelas-tech/kor/internal/store"
)

// Differential test for security rules: the same ruleset is loaded into
// Google's Firestore emulator and Kor's client endpoint, the same documents
// are seeded into both, and random users run random reads, writes and
// queries against both. Every decision must agree, with one allowance: Kor
// may deny a query the emulator allows (its query check is a sound
// approximation, see internal/rules/query.go) — reported, not failed. Kor
// allowing anything the emulator denies always fails.
//
// The generator knows nothing about any particular app: paths come from the
// ruleset's match patterns, field names from the rules text, and ids, users
// and values from a small vocabulary that makes wildcard/uid coincidences
// (alice reading /users/alice, chat "alice-bob") common.
//
//	KOR_FUZZ_EMULATOR=127.0.0.1:8432 KOR_RULES_FILE=path/to/firestore.rules \
//	  go test ./e2e/ -run TestRulesDifferential -count=1 -v
const rulesProject = "demo-kor-rules"

func TestRulesDifferential(t *testing.T) {
	emuAddr := emulatorAddr(t)
	src := secureRules
	if f := os.Getenv("KOR_RULES_FILE"); f != "" {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src = string(b)
	}
	rs, err := rules.Parse(src)
	if err != nil {
		t.Fatalf("kor cannot parse the ruleset: %v", err)
	}
	loadEmulatorRules(t, emuAddr, src)

	ctx := context.Background()
	st, err := store.Open(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	trusted := rpc.New(st)
	client := rpc.NewClient(trusted, rs, auth.New(rulesProject, auth.StaticKeys{}, auth.AcceptUnsignedTokens()), rulesProject, nil)
	korAdminAddr, korAddr := serveOn(t, trusted), serveOn(t, client)

	seed := time.Now().UnixNano()
	if s := os.Getenv("KOR_FUZZ_SEED"); s != "" {
		seed, _ = strconv.ParseInt(s, 10, 64)
	}
	rnd := rand.New(rand.NewSource(seed))
	t.Logf("seed %d (KOR_FUZZ_SEED reproduces)", seed)

	g := newGen(rnd, src)
	t.Logf("%d path templates, %d field names", len(g.paths), len(g.fields))
	emuAdmin := dialAsProject(t, emuAddr, "owner")
	korAdmin := dialAsProject(t, korAdminAddr, "")

	users := g.users()
	emuAs, korAs := map[string]*firestore.Client{}, map[string]*firestore.Client{}
	for name, tok := range users {
		emuAs[name] = dialAsProject(t, emuAddr, tok)
		korAs[name] = dialAsProject(t, korAddr, tok)
	}
	names := make([]string, 0, len(users))
	for n := range users {
		names = append(names, n)
	}
	sort.Strings(names)

	rounds := 60
	if s := os.Getenv("KOR_FUZZ_ROUNDS"); s != "" {
		rounds, _ = strconv.Atoi(s)
	}
	var ops, agreeAllow, agreeDeny, stricter int
	for round := 0; round < rounds; round++ {
		touched := map[string]bool{}
		g.world = map[string]map[string]any{}
		// Seed a world.
		for i := 0; i < 8; i++ {
			p, data := g.path(), g.doc()
			touched[p] = true
			g.world[p] = data
			if _, err := emuAdmin.Doc(p).Set(ctx, data); err != nil {
				t.Fatalf("seed emulator %s: %v", p, err)
			}
			if _, err := korAdmin.Doc(p).Set(ctx, data); err != nil {
				t.Fatalf("seed kor %s: %v", p, err)
			}
		}
		for i := 0; i < 25; i++ {
			who := names[rnd.Intn(len(names))]
			op := g.op()
			ops++
			emuErr := op.run(ctx, emuAs[who])
			korErr := op.run(ctx, korAs[who])
			for _, p := range op.paths {
				touched[p] = true
			}
			ed, kd := denied(emuErr), denied(korErr)
			switch {
			case ed && kd:
				agreeDeny++
			case !ed && !kd:
				agreeAllow++
			case ed && !kd:
				t.Fatalf("round %d: KOR ALLOWED what Firestore denies: %s as %s\n  emulator: %v\n  kor: %v", round, op.desc, who, emuErr, korErr)
			case op.query:
				stricter++
				t.Logf("kor stricter on a query (allowed by the emulator): %s as %s: %v", op.desc, who, korErr)
			default:
				t.Fatalf("round %d: kor denied what Firestore allows: %s as %s\n  kor: %v", round, op.desc, who, korErr)
			}
		}
		for p := range touched {
			_, _ = emuAdmin.Doc(p).Delete(ctx)
			_, _ = korAdmin.Doc(p).Delete(ctx)
		}
	}
	t.Logf("%d ops: %d both allowed, %d both denied, %d queries kor-stricter", ops, agreeAllow, agreeDeny, stricter)
	if agreeAllow == 0 {
		t.Fatal("nothing was ever allowed: the fuzz is not reaching the rules")
	}
}

func denied(err error) bool { return status.Code(err) == codes.PermissionDenied }

func serveOn(t *testing.T, s *rpc.Server) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer()
	s.Register(g)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return lis.Addr().String()
}

func dialAsProject(t *testing.T, addr, token string) *firestore.Client {
	t.Helper()
	return dialAsFor(t, addr, token, rulesProject)
}

func loadEmulatorRules(t *testing.T, addr, src string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"rules": map[string]any{"files": []any{map[string]any{"name": "firestore.rules", "content": src}}}})
	req, _ := http.NewRequest(http.MethodPut, "http://"+addr+"/emulator/v1/projects/"+rulesProject+":securityRules", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var b bytes.Buffer
		_, _ = b.ReadFrom(resp.Body)
		t.Fatalf("emulator rejected the ruleset: %s %s", resp.Status, b.String())
	}
}

// mockToken is the unsigned ID token the Firebase SDKs' mockUserToken builds.
func mockToken(uid, provider string, verified bool) string {
	now := time.Now().Unix()
	h, _ := json.Marshal(map[string]any{"alg": "none", "kid": "fakekid", "typ": "JWT"})
	p, _ := json.Marshal(map[string]any{
		"iss": "https://securetoken.google.com/" + rulesProject, "aud": rulesProject,
		"iat": now, "exp": now + 3600, "auth_time": now, "sub": uid, "user_id": uid,
		"email": uid + "@example.com", "email_verified": verified,
		"firebase": map[string]any{"sign_in_provider": provider, "identities": map[string]any{}},
	})
	return base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p) + "."
}

type gen struct {
	rnd    *rand.Rand
	paths  [][]string // segments; "" marks a wildcard to fill
	fields []string
	uids   []string
	ids    []string
	world  map[string]map[string]any // this round's seeded documents
}

var (
	patRE   = regexp.MustCompile(`match\s+(/[^\s{]*(?:\{[^}]*\}[^\s{]*)*)\s*\{`)
	fieldRE = regexp.MustCompile(`data\.([A-Za-z_][A-Za-z0-9_]*)|'([A-Za-z_][A-Za-z0-9_]*)'`)
)

func newGen(rnd *rand.Rand, src string) *gen {
	g := &gen{rnd: rnd, uids: []string{"alice", "bob", "carol"}}
	for _, a := range g.uids {
		g.ids = append(g.ids, a)
		for _, b := range g.uids {
			if a != b {
				g.ids = append(g.ids, a+"-"+b)
			}
		}
	}
	g.ids = append(g.ids, "x1")

	// Flatten nested match blocks into absolute templates by brace depth.
	type frame struct {
		segs  []string
		depth int
	}
	var stack []frame
	depth := 0
	seen := map[string]bool{}
	idx := 0
	for idx < len(src) {
		if m := patRE.FindStringSubmatchIndex(src[idx:]); m != nil && m[0] == 0 {
			pat := src[idx+m[2] : idx+m[3]]
			var base []string
			for len(stack) > 0 && stack[len(stack)-1].depth >= depth+1 {
				stack = stack[:len(stack)-1]
			}
			if len(stack) > 0 {
				base = append(base, stack[len(stack)-1].segs...)
			}
			segs := append([]string{}, base...)
			rec := false
			for _, s := range strings.Split(strings.Trim(pat, "/"), "/") {
				switch {
				case strings.Contains(s, "=**"):
					rec = true
				case strings.HasPrefix(s, "{"):
					segs = append(segs, "")
				default:
					segs = append(segs, s)
				}
			}
			depth++
			stack = append(stack, frame{segs: segs, depth: depth})
			idx += m[1]
			if rec || len(segs) < 3 || segs[0] != "databases" {
				continue
			}
			doc := segs[3:] // after databases/{db}/documents
			if len(doc)%2 == 1 || len(doc) == 0 {
				continue
			}
			if k := strings.Join(doc, "/"); !seen[k] {
				seen[k] = true
				g.paths = append(g.paths, doc)
			}
			continue
		}
		switch src[idx] {
		case '{':
			depth++
		case '}':
			depth--
			for len(stack) > 0 && stack[len(stack)-1].depth > depth {
				stack = stack[:len(stack)-1]
			}
		}
		idx++
	}
	fs := map[string]bool{}
	for _, m := range fieldRE.FindAllStringSubmatch(src, -1) {
		f := m[1] + m[2]
		if f != "" && f != "s" && f != "password" && !strings.Contains(f, "-") {
			fs[f] = true
		}
	}
	for f := range fs {
		g.fields = append(g.fields, f)
	}
	sort.Strings(g.fields)
	if len(g.fields) == 0 {
		g.fields = []string{"owner"}
	}
	return g
}

func (g *gen) users() map[string]string {
	return map[string]string{
		"anon":             "",
		"alice":            mockToken("alice", "google.com", true),
		"bob":              mockToken("bob", "password", true),
		"carol-unverified": mockToken("carol", "password", false),
	}
}

func (g *gen) pick(xs []string) string { return xs[g.rnd.Intn(len(xs))] }

func (g *gen) path() string {
	tpl := g.paths[g.rnd.Intn(len(g.paths))]
	out := make([]string, len(tpl))
	for i, s := range tpl {
		if s == "" {
			s = g.pick(g.ids)
		}
		out[i] = s
	}
	return strings.Join(out, "/")
}

func (g *gen) value(depth int) any {
	switch g.rnd.Intn(10) {
	case 0, 1:
		return g.pick(g.uids)
	case 2:
		return []any{g.pick(g.uids), g.pick(g.uids)}
	case 3:
		return []any{g.pick(g.uids)}
	case 4:
		return strings.Repeat("k", []int{4, 60, 120}[g.rnd.Intn(3)])
	case 5:
		return int64(g.rnd.Intn(4))
	case 6:
		return time.Now().Add([]time.Duration{-time.Hour, time.Hour}[g.rnd.Intn(2)])
	case 7:
		return g.rnd.Intn(2) == 0
	case 8:
		if depth < 1 {
			return g.doc1(depth + 1)
		}
		return "m"
	}
	return nil
}

func (g *gen) doc() map[string]any { return g.doc1(0) }

func (g *gen) doc1(depth int) map[string]any {
	d := map[string]any{}
	for i, n := 0, 1+g.rnd.Intn(4); i < n; i++ {
		d[g.pick(g.fields)] = g.value(depth)
	}
	return d
}

type fuzzOp struct {
	desc  string
	query bool
	paths []string
	run   func(ctx context.Context, c *firestore.Client) error
}

func (g *gen) op() fuzzOp {
	p := g.path()
	if len(g.world) > 0 && g.rnd.Intn(3) == 0 {
		// Near-miss writes: one field of a real document changed, added or
		// dropped. Narrow allow paths (update only readBy, keep participants)
		// are almost never hit by fresh random documents.
		ks := make([]string, 0, len(g.world))
		for k := range g.world {
			ks = append(ks, k)
		}
		sort.Strings(ks)
		p = ks[g.rnd.Intn(len(ks))]
		d := map[string]any{}
		for k, v := range g.world[p] {
			d[k] = v
		}
		f := g.pick(g.fields)
		if _, has := d[f]; has && g.rnd.Intn(3) == 0 {
			delete(d, f)
		} else {
			d[f] = g.value(0)
		}
		return fuzzOp{desc: fmt.Sprintf("mutate %s -> %v", p, d), paths: []string{p}, run: func(ctx context.Context, c *firestore.Client) error {
			_, err := c.Doc(p).Set(ctx, d)
			return err
		}}
	}
	switch g.rnd.Intn(6) {
	case 0, 1:
		return fuzzOp{desc: "get " + p, paths: []string{p}, run: func(ctx context.Context, c *firestore.Client) error {
			_, err := c.Doc(p).Get(ctx)
			if status.Code(err) == codes.NotFound {
				return nil
			}
			return err
		}}
	case 2:
		d := g.doc()
		return fuzzOp{desc: fmt.Sprintf("set %s %v", p, d), paths: []string{p}, run: func(ctx context.Context, c *firestore.Client) error {
			_, err := c.Doc(p).Set(ctx, d)
			return err
		}}
	case 3:
		d := g.doc()
		return fuzzOp{desc: fmt.Sprintf("merge %s %v", p, d), paths: []string{p}, run: func(ctx context.Context, c *firestore.Client) error {
			_, err := c.Doc(p).Set(ctx, d, firestore.MergeAll)
			return err
		}}
	case 4:
		return fuzzOp{desc: "delete " + p, paths: []string{p}, run: func(ctx context.Context, c *firestore.Client) error {
			_, err := c.Doc(p).Delete(ctx)
			return err
		}}
	}
	coll := p[:strings.LastIndex(p, "/")]
	f, op, v := g.pick(g.fields), "==", any(g.pick(g.uids))
	if g.rnd.Intn(2) == 0 {
		op = "array-contains"
	}
	withFilter := g.rnd.Intn(4) != 0
	desc := "query " + coll
	if withFilter {
		desc += fmt.Sprintf(" where %s %s %v", f, op, v)
	}
	return fuzzOp{desc: desc, query: true, run: func(ctx context.Context, c *firestore.Client) error {
		q := c.Collection(coll).Query
		if withFilter {
			q = q.Where(f, op, v)
		}
		_, err := q.Limit(20).Documents(ctx).GetAll()
		return err
	}}
}
