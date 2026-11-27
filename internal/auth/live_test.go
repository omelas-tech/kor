package auth

import (
	"context"
	"os"
	"testing"
)

// KOR_LIVE_KEYS=1 fetches Google's real signing certificates.
func TestLiveGoogleKeys(t *testing.T) {
	if os.Getenv("KOR_LIVE_KEYS") == "" {
		t.Skip("set KOR_LIVE_KEYS=1 to fetch Google's certificates")
	}
	g := NewGoogleKeys(GoogleCertsURL)
	keys, err := g.Keys(context.Background())
	if err != nil || len(keys) == 0 {
		t.Fatalf("keys=%d err=%v", len(keys), err)
	}
	if g.expires.IsZero() {
		t.Fatal("no cache expiry from Cache-Control")
	}
	t.Logf("%d keys, cached until %v", len(keys), g.expires)
}
