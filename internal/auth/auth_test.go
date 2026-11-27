package auth

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

var now = time.Date(2026, 11, 28, 12, 0, 0, 0, time.UTC)

func mint(t *testing.T, key *rsa.PrivateKey, alg, kid string, claims map[string]any) string {
	t.Helper()
	h, _ := json.Marshal(map[string]any{"alg": alg, "kid": kid, "typ": "JWT"})
	p, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(h) + "." + base64.RawURLEncoding.EncodeToString(p)
	if key == nil {
		return signing + "."
	}
	sum := sha256.Sum256([]byte(signing))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		t.Fatal(err)
	}
	return signing + "." + base64.RawURLEncoding.EncodeToString(sig)
}

func claims(mod func(map[string]any)) map[string]any {
	c := map[string]any{
		"iss": "https://securetoken.google.com/demo", "aud": "demo", "sub": "alice",
		"iat": now.Add(-time.Minute).Unix(), "exp": now.Add(time.Hour).Unix(), "auth_time": now.Add(-time.Hour).Unix(),
		"email_verified": true, "firebase": map[string]any{"sign_in_provider": "password"},
	}
	if mod != nil {
		mod(c)
	}
	return c
}

func TestVerify(t *testing.T) {
	key, _ := rsa.GenerateKey(rand.Reader, 2048)
	other, _ := rsa.GenerateKey(rand.Reader, 2048)
	v := New("demo", StaticKeys{"k1": &key.PublicKey}, WithClock(func() time.Time { return now }))
	ctx := context.Background()

	good := mint(t, key, "RS256", "k1", claims(nil))
	a, err := v.Verify(ctx, good)
	if err != nil {
		t.Fatal(err)
	}
	tok := a["token"].(map[string]any)
	if a["uid"] != "alice" || tok["email_verified"] != true || tok["firebase"].(map[string]any)["sign_in_provider"] != "password" {
		t.Fatalf("auth = %v", a)
	}
	if _, isInt := tok["exp"].(int64); !isInt {
		t.Fatalf("numeric claims must be ints, got %T", tok["exp"])
	}

	bad := map[string]string{
		"other project":      mint(t, key, "RS256", "k1", claims(func(c map[string]any) { c["aud"] = "evil" })),
		"other issuer":       mint(t, key, "RS256", "k1", claims(func(c map[string]any) { c["iss"] = "https://securetoken.google.com/evil" })),
		"expired":            mint(t, key, "RS256", "k1", claims(func(c map[string]any) { c["exp"] = now.Add(-2 * time.Minute).Unix() })),
		"issued in future":   mint(t, key, "RS256", "k1", claims(func(c map[string]any) { c["iat"] = now.Add(time.Hour).Unix() })),
		"no subject":         mint(t, key, "RS256", "k1", claims(func(c map[string]any) { c["sub"] = "" })),
		"signed by stranger": mint(t, other, "RS256", "k1", claims(nil)),
		"unknown key id":     mint(t, key, "RS256", "k9", claims(nil)),
		"unsigned":           mint(t, nil, "none", "", claims(nil)),
		"hs256 confusion":    mint(t, nil, "HS256", "k1", claims(nil)),
		"garbage":            "not.a.jwt.at.all",
		"tampered payload":   good[:len(good)/2] + "A" + good[len(good)/2+1:],
	}
	for name, tokn := range bad {
		if _, err := v.Verify(ctx, tokn); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	emu := New("demo", StaticKeys{}, AcceptUnsignedTokens())
	if a, err := emu.Verify(ctx, mint(t, nil, "none", "", map[string]any{"sub": "bob"})); err != nil || a["uid"] != "bob" {
		t.Fatalf("unsigned in emulator mode: %v %v", a, err)
	}
}
