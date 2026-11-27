// Package auth verifies Firebase Auth ID tokens, the bearer tokens every
// Firebase client SDK attaches to Firestore requests, and turns them into the
// request.auth value security rules see.
package auth

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

// GoogleCertsURL publishes the keys Firebase Auth signs ID tokens with.
const GoogleCertsURL = "https://www.googleapis.com/robot/v1/metadata/x509/securetoken@system.gserviceaccount.com"

// ErrInvalid is wrapped by every rejection; callers answer UNAUTHENTICATED.
var ErrInvalid = errors.New("invalid ID token")

// KeySource returns the current signing keys by key id.
type KeySource interface {
	Keys(ctx context.Context) (map[string]*rsa.PublicKey, error)
}

// Verifier checks ID tokens for one Firebase project.
type Verifier struct {
	project string
	keys    KeySource
	now     func() time.Time
	// unsigned accepts alg "none" tokens: what the Firebase emulators and the
	// SDKs' mockUserToken issue. Tests only — anyone can mint one.
	unsigned bool
}

// Option configures a Verifier.
type Option func(*Verifier)

// WithClock overrides the clock (tests).
func WithClock(now func() time.Time) Option { return func(v *Verifier) { v.now = now } }

// AcceptUnsignedTokens enables emulator-style unsigned tokens. Never in
// production: it lets any caller claim to be any user.
func AcceptUnsignedTokens() Option { return func(v *Verifier) { v.unsigned = true } }

// New returns a verifier for project, fetching Google's keys when keys is nil.
func New(project string, keys KeySource, opts ...Option) *Verifier {
	if keys == nil {
		keys = NewGoogleKeys(GoogleCertsURL)
	}
	v := &Verifier{project: project, keys: keys, now: time.Now}
	for _, o := range opts {
		o(v)
	}
	return v
}

// Unsigned reports whether the verifier accepts unsigned tokens.
func (v *Verifier) Unsigned() bool { return v.unsigned }

const skew = time.Minute

// Verify validates token and returns request.auth: {uid, token: claims}.
func (v *Verifier) Verify(ctx context.Context, token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("%w: not a JWT", ErrInvalid)
	}
	var header struct {
		Alg string `json:"alg"`
		Kid string `json:"kid"`
	}
	if err := decodeSegment(parts[0], &header); err != nil {
		return nil, fmt.Errorf("%w: header: %v", ErrInvalid, err)
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("%w: payload encoding", ErrInvalid)
	}

	switch {
	case header.Alg == "RS256":
		keys, err := v.keys.Keys(ctx)
		if err != nil {
			return nil, fmt.Errorf("auth: signing keys unavailable: %w", err)
		}
		key, ok := keys[header.Kid]
		if !ok {
			return nil, fmt.Errorf("%w: unknown key id %q", ErrInvalid, header.Kid)
		}
		sig, err := base64.RawURLEncoding.DecodeString(parts[2])
		if err != nil {
			return nil, fmt.Errorf("%w: signature encoding", ErrInvalid)
		}
		sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
		if err := rsa.VerifyPKCS1v15(key, crypto.SHA256, sum[:], sig); err != nil {
			return nil, fmt.Errorf("%w: bad signature", ErrInvalid)
		}
	case header.Alg == "none" && v.unsigned:
	default:
		return nil, fmt.Errorf("%w: algorithm %q", ErrInvalid, header.Alg)
	}

	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.UseNumber()
	var raw map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: payload: %v", ErrInvalid, err)
	}
	claims := normalize(raw).(map[string]any)

	now := v.now()
	sub, _ := claims["sub"].(string)
	if sub == "" || len(sub) > 128 {
		return nil, fmt.Errorf("%w: sub", ErrInvalid)
	}
	if !v.unsigned || header.Alg != "none" {
		if aud, _ := claims["aud"].(string); aud != v.project {
			return nil, fmt.Errorf("%w: audience %q", ErrInvalid, aud)
		}
		if iss, _ := claims["iss"].(string); iss != "https://securetoken.google.com/"+v.project {
			return nil, fmt.Errorf("%w: issuer %q", ErrInvalid, iss)
		}
		exp, ok1 := claims["exp"].(int64)
		iat, ok2 := claims["iat"].(int64)
		if !ok1 || !ok2 {
			return nil, fmt.Errorf("%w: exp/iat", ErrInvalid)
		}
		if now.After(time.Unix(exp, 0).Add(skew)) {
			return nil, fmt.Errorf("%w: expired", ErrInvalid)
		}
		if time.Unix(iat, 0).After(now.Add(skew)) {
			return nil, fmt.Errorf("%w: issued in the future", ErrInvalid)
		}
		if at, ok := claims["auth_time"].(int64); ok && time.Unix(at, 0).After(now.Add(skew)) {
			return nil, fmt.Errorf("%w: auth_time in the future", ErrInvalid)
		}
	}
	return map[string]any{"uid": sub, "token": claims}, nil
}

func decodeSegment(s string, v any) error {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// normalize turns JSON numbers into the rules int/float distinction.
func normalize(v any) any {
	switch t := v.(type) {
	case json.Number:
		if i, err := strconv.ParseInt(t.String(), 10, 64); err == nil {
			return i
		}
		f, _ := t.Float64()
		if f == math.Trunc(f) && math.Abs(f) < 1<<53 {
			return int64(f)
		}
		return f
	case map[string]any:
		for k, e := range t {
			t[k] = normalize(e)
		}
		return t
	case []any:
		for i, e := range t {
			t[i] = normalize(e)
		}
		return t
	}
	return v
}

// GoogleKeys fetches and caches the x509 certificates at a URL.
type GoogleKeys struct {
	url    string
	client *http.Client

	mu      sync.Mutex
	keys    map[string]*rsa.PublicKey
	expires time.Time
}

// NewGoogleKeys returns a key source for url.
func NewGoogleKeys(url string) *GoogleKeys {
	return &GoogleKeys{url: url, client: &http.Client{Timeout: 10 * time.Second}}
}

var maxAgeRE = regexp.MustCompile(`max-age=(\d+)`)

// Keys returns cached keys, refetching after the Cache-Control max-age. A
// failed refresh keeps serving the previous keys rather than locking every
// client out.
func (g *GoogleKeys) Keys(ctx context.Context) (map[string]*rsa.PublicKey, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.keys != nil && time.Now().Before(g.expires) {
		return g.keys, nil
	}
	keys, ttl, err := g.fetch(ctx)
	if err != nil {
		if g.keys != nil {
			g.expires = time.Now().Add(time.Minute)
			return g.keys, nil
		}
		return nil, err
	}
	g.keys, g.expires = keys, time.Now().Add(ttl)
	return keys, nil
}

func (g *GoogleKeys) fetch(ctx context.Context) (map[string]*rsa.PublicKey, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, g.url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := g.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, 0, fmt.Errorf("auth: %s: %s", g.url, resp.Status)
	}
	var certs map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&certs); err != nil {
		return nil, 0, err
	}
	keys, err := ParseCerts(certs)
	if err != nil {
		return nil, 0, err
	}
	ttl := time.Hour
	if m := maxAgeRE.FindStringSubmatch(resp.Header.Get("Cache-Control")); m != nil {
		if s, err := strconv.Atoi(m[1]); err == nil && s > 0 {
			ttl = time.Duration(s) * time.Second
		}
	}
	return keys, ttl, nil
}

// ParseCerts parses a {kid: PEM certificate} map.
func ParseCerts(certs map[string]string) (map[string]*rsa.PublicKey, error) {
	keys := make(map[string]*rsa.PublicKey, len(certs))
	for kid, p := range certs {
		block, _ := pem.Decode([]byte(p))
		if block == nil {
			return nil, fmt.Errorf("auth: key %s: not PEM", kid)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("auth: key %s: %w", kid, err)
		}
		pub, ok := cert.PublicKey.(*rsa.PublicKey)
		if !ok {
			return nil, fmt.Errorf("auth: key %s is not RSA", kid)
		}
		keys[kid] = pub
	}
	return keys, nil
}

// StaticKeys is a fixed key set (tests).
type StaticKeys map[string]*rsa.PublicKey

// Keys implements KeySource.
func (s StaticKeys) Keys(context.Context) (map[string]*rsa.PublicKey, error) { return s, nil }
