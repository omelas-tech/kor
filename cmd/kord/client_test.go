package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/omelas-tech/kor/internal/rpc"
)

func TestServeClientRefusesUnsafeConfigs(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "ok.rules")
	bad := filepath.Join(dir, "bad.rules")
	os.WriteFile(good, []byte(`rules_version = '2'; service cloud.firestore { match /databases/{d}/documents { match /x/{y} { allow read: if true; } } }`), 0o600)
	os.WriteFile(bad, []byte(`service cloud.firestore { match /x {`), 0o600)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	trusted := rpc.New(nil)
	cases := map[string]struct {
		addr, rules, project, cert string
		unsigned                   bool
		want                       string
	}{
		"no rules":                 {"127.0.0.1:0", "", "p", "", false, "needs -rules"},
		"no project":               {"127.0.0.1:0", good, "", "", false, "needs -rules"},
		"unsigned tokens off-host": {"0.0.0.0:0", good, "p", "", true, "unsigned"},
		"off-host without tls":     {"0.0.0.0:0", good, "p", "", false, "TLS"},
		"rules that do not parse":  {"127.0.0.1:0", bad, "p", "", false, ""},
	}
	for name, c := range cases {
		stop, err := serveClient(context.Background(), log, trusted, c.addr, c.rules, c.project, c.cert, c.cert, c.unsigned)
		if err == nil {
			stop()
			t.Errorf("%s: started", name)
			continue
		}
		if c.want != "" && !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err %q, want it to mention %q", name, err, c.want)
		}
	}
	stop, err := serveClient(context.Background(), log, trusted, "127.0.0.1:0", good, "p", "", "", true)
	if err != nil {
		t.Fatalf("loopback test config: %v", err)
	}
	stop()
}
