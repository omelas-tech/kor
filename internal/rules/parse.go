// Package rules implements the Cloud Firestore security rules language:
// parsing a firestore.rules file verbatim and evaluating requests against it.
package rules

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// ---- AST ----

// Ruleset is a parsed rules file.
type Ruleset struct {
	Version string // rules_version; "1" when absent
	Root    *Scope // the service cloud.firestore block
}

// Scope is a match block (the service block is the root scope, no pattern).
type Scope struct {
	Pattern  []PatSeg
	Funcs    map[string]*Func
	Allows   []*Allow
	Children []*Scope
	Parent   *Scope
}

// PatSeg is one segment of a match path: a literal, {var} or {var=**}.
type PatSeg struct {
	Lit string
	Var string
	Rec bool
}

// Allow is `allow m1, m2: if cond;`.
type Allow struct {
	Methods []string
	Cond    Expr // nil = unconditional
	Line    int
}

// Func is a user function.
type Func struct {
	Name   string
	Params []string
	Lets   []Let
	Body   Expr
}

// Let is a local binding inside a function.
type Let struct {
	Name string
	X    Expr
}

// Expr is an expression node.
type Expr interface{ exprNode() }

type (
	Lit   struct{ V any }
	Ident struct{ Name string }
	Unary struct {
		Op string
		X  Expr
	}
	Binary struct {
		Op   string
		X, Y Expr
	}
	Ternary struct{ C, T, F Expr }
	Member  struct {
		X    Expr
		Name string
	}
	Index struct{ X, I Expr }
	Slice struct{ X, Lo, Hi Expr }
	Call  struct {
		Fn   Expr
		Args []Expr
	}
	ListLit struct{ Elems []Expr }
	MapLit  struct{ Keys, Vals []Expr }
	TypeIs  struct {
		X    Expr
		Type string
	}
	PathLit struct{ Parts []PathPart }
)

// PathPart is a path-literal segment: Lit, or $(X).
type PathPart struct {
	Lit string
	X   Expr
}

func (Lit) exprNode()     {}
func (Ident) exprNode()   {}
func (Unary) exprNode()   {}
func (Binary) exprNode()  {}
func (Ternary) exprNode() {}
func (Member) exprNode()  {}
func (Index) exprNode()   {}
func (Slice) exprNode()   {}
func (Call) exprNode()    {}
func (ListLit) exprNode() {}
func (MapLit) exprNode()  {}
func (TypeIs) exprNode()  {}
func (PathLit) exprNode() {}

// ---- parser (scanner and parser in one: path literals need the raw text) ----

type parser struct {
	src  string
	pos  int
	line int
}

// Parse parses a firestore.rules source.
func Parse(src string) (rs *Ruleset, err error) {
	p := &parser{src: src, line: 1}
	defer func() {
		if r := recover(); r != nil {
			if pe, ok := r.(parseError); ok {
				rs, err = nil, pe
				return
			}
			panic(r)
		}
	}()
	rs = &Ruleset{Version: "1"}
	p.skip()
	if p.peekWord("rules_version") {
		p.word()
		p.expect("=")
		rs.Version = p.stringLit()
		p.accept(";")
	}
	p.expectWord("service")
	name := p.dotted()
	if name != "cloud.firestore" {
		p.fail("service %q is not cloud.firestore", name)
	}
	rs.Root = &Scope{Funcs: map[string]*Func{}}
	p.expect("{")
	p.scopeBody(rs.Root)
	p.expect("}")
	p.skip()
	if p.pos < len(p.src) {
		p.fail("unexpected text after the service block")
	}
	if rs.Version != "1" && rs.Version != "2" {
		return nil, fmt.Errorf("rules: unsupported rules_version %q", rs.Version)
	}
	return rs, nil
}

type parseError struct {
	line int
	msg  string
}

func (e parseError) Error() string { return fmt.Sprintf("rules: line %d: %s", e.line, e.msg) }

func (p *parser) fail(f string, a ...any) { panic(parseError{p.line, fmt.Sprintf(f, a...)}) }

func (p *parser) skip() {
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		switch {
		case c == '\n':
			p.line++
			p.pos++
		case c == ' ' || c == '\t' || c == '\r':
			p.pos++
		case strings.HasPrefix(p.src[p.pos:], "//"):
			for p.pos < len(p.src) && p.src[p.pos] != '\n' {
				p.pos++
			}
		case strings.HasPrefix(p.src[p.pos:], "/*"):
			end := strings.Index(p.src[p.pos+2:], "*/")
			if end < 0 {
				p.fail("unterminated comment")
			}
			p.line += strings.Count(p.src[p.pos:p.pos+2+end], "\n")
			p.pos += end + 4
		default:
			return
		}
	}
}

func (p *parser) peek(s string) bool {
	p.skip()
	return strings.HasPrefix(p.src[p.pos:], s)
}

// peekOp is peek for operators, refusing a prefix of a longer operator.
func (p *parser) peekOp(s string) bool {
	if !p.peek(s) {
		return false
	}
	rest := p.src[p.pos+len(s):]
	switch s {
	case "=", "!", "<", ">":
		return !strings.HasPrefix(rest, "=")
	case "&":
		return !strings.HasPrefix(rest, "&")
	case "|":
		return !strings.HasPrefix(rest, "|")
	}
	return true
}

func (p *parser) accept(s string) bool {
	if p.peekOp(s) {
		p.pos += len(s)
		return true
	}
	return false
}

func (p *parser) expect(s string) {
	if !p.accept(s) {
		p.fail("expected %q", s)
	}
}

func isIdentStart(r rune) bool { return r == '_' || unicode.IsLetter(r) }
func isIdentPart(r rune) bool  { return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) }

func (p *parser) peekWord(w string) bool {
	p.skip()
	if !strings.HasPrefix(p.src[p.pos:], w) {
		return false
	}
	if r, _ := utf8.DecodeRuneInString(p.src[p.pos+len(w):]); isIdentPart(r) {
		return false
	}
	return true
}

func (p *parser) word() string {
	p.skip()
	start := p.pos
	r, n := utf8.DecodeRuneInString(p.src[p.pos:])
	if !isIdentStart(r) {
		p.fail("expected an identifier")
	}
	p.pos += n
	for p.pos < len(p.src) {
		r, n := utf8.DecodeRuneInString(p.src[p.pos:])
		if !isIdentPart(r) {
			break
		}
		p.pos += n
	}
	return p.src[start:p.pos]
}

func (p *parser) expectWord(w string) {
	if !p.peekWord(w) {
		p.fail("expected %q", w)
	}
	p.pos += len(w)
}

func (p *parser) dotted() string {
	parts := []string{p.word()}
	for p.accept(".") {
		parts = append(parts, p.word())
	}
	return strings.Join(parts, ".")
}

func (p *parser) scopeBody(s *Scope) {
	for {
		switch {
		case p.peek("}"):
			return
		case p.peekWord("match"):
			p.pos += len("match")
			child := &Scope{Pattern: p.matchPattern(), Funcs: map[string]*Func{}, Parent: s}
			p.expect("{")
			p.scopeBody(child)
			p.expect("}")
			s.Children = append(s.Children, child)
		case p.peekWord("allow"):
			s.Allows = append(s.Allows, p.allow())
		case p.peekWord("function"):
			f := p.function()
			if _, dup := s.Funcs[f.Name]; dup {
				p.fail("function %s declared twice in one scope", f.Name)
			}
			s.Funcs[f.Name] = f
		default:
			p.fail("expected match, allow, function or }")
		}
	}
}

func (p *parser) matchPattern() []PatSeg {
	p.skip()
	var segs []PatSeg
	for p.pos < len(p.src) && p.src[p.pos] == '/' {
		p.pos++
		if p.pos < len(p.src) && p.src[p.pos] == '{' {
			end := strings.IndexByte(p.src[p.pos:], '}')
			if end < 0 {
				p.fail("unterminated wildcard")
			}
			inner := p.src[p.pos+1 : p.pos+end]
			p.pos += end + 1
			seg := PatSeg{Var: inner}
			if name, ok := strings.CutSuffix(inner, "=**"); ok {
				seg = PatSeg{Var: name, Rec: true}
			}
			if seg.Var == "" {
				p.fail("empty wildcard name")
			}
			segs = append(segs, seg)
			continue
		}
		start := p.pos
		for p.pos < len(p.src) && isPathChar(p.src[p.pos]) {
			p.pos++
		}
		if p.pos == start {
			p.fail("empty path segment in match")
		}
		segs = append(segs, PatSeg{Lit: p.src[start:p.pos]})
	}
	if len(segs) == 0 {
		p.fail("match needs a path")
	}
	return segs
}

func isPathChar(c byte) bool {
	return c == '_' || c == '-' || c == '.' || c == '~' || c == '%' || c == '@' || c == '+' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c >= 0x80
}

var methodNames = map[string]bool{"read": true, "write": true, "get": true, "list": true, "create": true, "update": true, "delete": true}

func (p *parser) allow() *Allow {
	p.expectWord("allow")
	a := &Allow{Line: p.line}
	for {
		m := p.word()
		if !methodNames[m] {
			p.fail("unknown method %q", m)
		}
		a.Methods = append(a.Methods, m)
		if !p.accept(",") {
			break
		}
	}
	if p.accept(":") {
		p.expectWord("if")
		a.Cond = p.expr()
	}
	p.accept(";")
	return a
}

func (p *parser) function() *Func {
	p.expectWord("function")
	f := &Func{Name: p.word()}
	p.expect("(")
	if !p.peek(")") {
		for {
			f.Params = append(f.Params, p.word())
			if !p.accept(",") {
				break
			}
		}
	}
	p.expect(")")
	p.expect("{")
	for p.peekWord("let") {
		p.pos += 3
		name := p.word()
		p.expect("=")
		f.Lets = append(f.Lets, Let{Name: name, X: p.expr()})
		p.expect(";")
	}
	p.expectWord("return")
	f.Body = p.expr()
	p.accept(";")
	p.expect("}")
	return f
}

// ---- expressions, lowest precedence first ----

func (p *parser) expr() Expr {
	c := p.or()
	if p.accept("?") {
		t := p.expr()
		p.expect(":")
		f := p.expr()
		return Ternary{c, t, f}
	}
	return c
}

func (p *parser) or() Expr {
	x := p.and()
	for p.accept("||") {
		x = Binary{"||", x, p.and()}
	}
	return x
}

func (p *parser) and() Expr {
	x := p.rel()
	for p.accept("&&") {
		x = Binary{"&&", x, p.rel()}
	}
	return x
}

func (p *parser) rel() Expr {
	x := p.add()
	for {
		switch {
		case p.accept("=="):
			x = Binary{"==", x, p.add()}
		case p.accept("!="):
			x = Binary{"!=", x, p.add()}
		case p.accept("<="):
			x = Binary{"<=", x, p.add()}
		case p.accept(">="):
			x = Binary{">=", x, p.add()}
		case p.accept("<"):
			x = Binary{"<", x, p.add()}
		case p.accept(">"):
			x = Binary{">", x, p.add()}
		case p.peekWord("in"):
			p.pos += 2
			x = Binary{"in", x, p.add()}
		case p.peekWord("is"):
			p.pos += 2
			x = TypeIs{x, p.word()}
		default:
			return x
		}
	}
}

func (p *parser) add() Expr {
	x := p.mul()
	for {
		switch {
		case p.accept("+"):
			x = Binary{"+", x, p.mul()}
		case p.accept("-"):
			x = Binary{"-", x, p.mul()}
		default:
			return x
		}
	}
}

func (p *parser) mul() Expr {
	x := p.unary()
	for {
		switch {
		case p.accept("*"):
			x = Binary{"*", x, p.unary()}
		case p.peek("/") && !p.peek("//") && !p.peek("/*"):
			p.pos++
			x = Binary{"/", x, p.unary()}
		case p.accept("%"):
			x = Binary{"%", x, p.unary()}
		default:
			return x
		}
	}
}

func (p *parser) unary() Expr {
	switch {
	case p.accept("!"):
		return Unary{"!", p.unary()}
	case p.accept("-"):
		return Unary{"-", p.unary()}
	}
	return p.postfix(p.primary())
}

func (p *parser) postfix(x Expr) Expr {
	for {
		switch {
		case p.accept("."):
			x = Member{x, p.word()}
		case p.accept("("):
			x = Call{x, p.args(")")}
		case p.accept("["):
			if p.accept(":") {
				hi := p.expr()
				p.expect("]")
				x = Slice{x, nil, hi}
				continue
			}
			i := p.expr()
			if p.accept(":") {
				var hi Expr
				if !p.peek("]") {
					hi = p.expr()
				}
				p.expect("]")
				x = Slice{x, i, hi}
				continue
			}
			p.expect("]")
			x = Index{x, i}
		default:
			return x
		}
	}
}

func (p *parser) args(close string) []Expr {
	var out []Expr
	if p.accept(close) {
		return out
	}
	for {
		out = append(out, p.expr())
		if !p.accept(",") {
			break
		}
	}
	p.expect(close)
	return out
}

func (p *parser) primary() Expr {
	p.skip()
	if p.pos >= len(p.src) {
		p.fail("unexpected end of input")
	}
	c := p.src[p.pos]
	switch {
	case c == '(':
		p.pos++
		x := p.expr()
		p.expect(")")
		return x
	case c == '[':
		p.pos++
		return ListLit{p.args("]")}
	case c == '{':
		p.pos++
		m := MapLit{}
		if p.accept("}") {
			return m
		}
		for {
			m.Keys = append(m.Keys, p.expr())
			p.expect(":")
			m.Vals = append(m.Vals, p.expr())
			if !p.accept(",") {
				break
			}
		}
		p.expect("}")
		return m
	case c == '\'' || c == '"':
		return Lit{p.stringLit()}
	case c == '/':
		return p.pathLit()
	case c >= '0' && c <= '9' || (c == '.' && p.pos+1 < len(p.src) && p.src[p.pos+1] >= '0' && p.src[p.pos+1] <= '9'):
		return Lit{p.number()}
	}
	w := p.word()
	switch w {
	case "true":
		return Lit{true}
	case "false":
		return Lit{false}
	case "null":
		return Lit{nil}
	}
	return Ident{w}
}

func (p *parser) number() any {
	start := p.pos
	isFloat := false
	for p.pos < len(p.src) {
		c := p.src[p.pos]
		switch {
		case c >= '0' && c <= '9':
		case c == '.':
			// "1.size()" is not a thing; a dot followed by a digit is a fraction.
			if p.pos+1 >= len(p.src) || p.src[p.pos+1] < '0' || p.src[p.pos+1] > '9' {
				goto done
			}
			isFloat = true
		case c == 'e' || c == 'E':
			isFloat = true
			if p.pos+1 < len(p.src) && (p.src[p.pos+1] == '+' || p.src[p.pos+1] == '-') {
				p.pos++
			}
		default:
			goto done
		}
		p.pos++
	}
done:
	text := p.src[start:p.pos]
	if isFloat {
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			p.fail("bad number %q", text)
		}
		return f
	}
	i, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		p.fail("bad integer %q", text)
	}
	return i
}

func (p *parser) stringLit() string {
	p.skip()
	if p.pos >= len(p.src) || (p.src[p.pos] != '\'' && p.src[p.pos] != '"') {
		p.fail("expected a string")
	}
	q := p.src[p.pos]
	p.pos++
	var b strings.Builder
	for {
		if p.pos >= len(p.src) {
			p.fail("unterminated string")
		}
		c := p.src[p.pos]
		if c == q {
			p.pos++
			return b.String()
		}
		if c == '\n' {
			p.fail("newline in string")
		}
		if c != '\\' {
			b.WriteByte(c)
			p.pos++
			continue
		}
		p.pos++
		if p.pos >= len(p.src) {
			p.fail("unterminated escape")
		}
		e := p.src[p.pos]
		p.pos++
		switch e {
		case 'n':
			b.WriteByte('\n')
		case 't':
			b.WriteByte('\t')
		case 'r':
			b.WriteByte('\r')
		case '\\', '\'', '"', '/', '.', '-', '*', '+', '?', '(', ')', '[', ']', '{', '}', '|', '^', '$':
			// Regex escapes inside rules strings ('\\.' and '\.') both reach the
			// regex engine as an escaped character.
			if e != '\\' && e != '\'' && e != '"' {
				b.WriteByte('\\')
			}
			b.WriteByte(e)
		case 'u':
			if p.pos+4 > len(p.src) {
				p.fail("bad \\u escape")
			}
			n, err := strconv.ParseUint(p.src[p.pos:p.pos+4], 16, 32)
			if err != nil {
				p.fail("bad \\u escape")
			}
			b.WriteRune(rune(n))
			p.pos += 4
		default:
			// Unknown escapes pass through for the regex engine (\d, \w, \s ...).
			b.WriteByte('\\')
			b.WriteByte(e)
		}
	}
}

func (p *parser) pathLit() Expr {
	var parts []PathPart
	for p.pos < len(p.src) && p.src[p.pos] == '/' {
		p.pos++
		switch {
		case strings.HasPrefix(p.src[p.pos:], "$("):
			p.pos += 2
			x := p.expr()
			p.expect(")")
			parts = append(parts, PathPart{X: x})
		case p.pos < len(p.src) && p.src[p.pos] == '(':
			end := strings.IndexByte(p.src[p.pos:], ')')
			if end < 0 {
				p.fail("unterminated path segment")
			}
			parts = append(parts, PathPart{Lit: p.src[p.pos : p.pos+end+1]})
			p.pos += end + 1
		default:
			start := p.pos
			for p.pos < len(p.src) && isPathChar(p.src[p.pos]) {
				p.pos++
			}
			if p.pos == start {
				p.fail("empty path segment")
			}
			parts = append(parts, PathPart{Lit: p.src[start:p.pos]})
		}
	}
	return PathLit{parts}
}
