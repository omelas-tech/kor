// Package pgtest spins up a throwaway PostgreSQL cluster for tests using the
// locally installed initdb/pg_ctl binaries. Each test binary gets one cluster
// (via Main); each test gets a fresh database (via DSN).
package pgtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	clusterPort int
	clusterErr  error
	dbCounter   atomic.Int64
)

// Main wraps testing.M: it starts the cluster, runs the tests, and tears the
// cluster down. Call from TestMain in packages that need Postgres.
func Main(m *testing.M) {
	code := run(m)
	os.Exit(code)
}

func run(m *testing.M) int {
	dir, err := os.MkdirTemp("", "korpg-*")
	if err != nil {
		clusterErr = err
		return m.Run()
	}
	defer os.RemoveAll(dir)

	initdb, err1 := exec.LookPath("initdb")
	pgctl, err2 := exec.LookPath("pg_ctl")
	if err1 != nil || err2 != nil {
		clusterErr = fmt.Errorf("initdb/pg_ctl not found in PATH; install PostgreSQL to run these tests")
		return m.Run()
	}

	dataDir := dir + "/data"
	if out, err := exec.Command(initdb, "-D", dataDir, "-A", "trust", "-U", "kor",
		"--no-sync", "-E", "UTF8", "--locale=C").CombinedOutput(); err != nil {
		clusterErr = fmt.Errorf("initdb: %v\n%s", err, out)
		return m.Run()
	}

	port, err := freePort()
	if err != nil {
		clusterErr = err
		return m.Run()
	}

	opts := fmt.Sprintf("-p %d -c listen_addresses=127.0.0.1 -c fsync=off -c synchronous_commit=off -c full_page_writes=off", port)
	if out, err := exec.Command(pgctl, "-D", dataDir, "-w", "-t", "30",
		"-o", opts, "-l", dir+"/postgres.log", "start").CombinedOutput(); err != nil {
		log, _ := os.ReadFile(dir + "/postgres.log")
		clusterErr = fmt.Errorf("pg_ctl start: %v\n%s\n%s", err, out, log)
		return m.Run()
	}
	defer func() {
		_ = exec.Command(pgctl, "-D", dataDir, "-m", "immediate", "stop").Run()
	}()

	clusterPort = port
	return m.Run()
}

// DSN creates a fresh database in the test cluster and returns its DSN.
// Tests are skipped when no cluster is available (e.g. Postgres not
// installed), and fail when the cluster exists but provisioning breaks.
func DSN(t *testing.T) string {
	t.Helper()
	if clusterErr != nil {
		t.Skipf("pgtest: no cluster: %v", clusterErr)
	}
	if clusterPort == 0 {
		t.Skip("pgtest: cluster not started (missing pgtest.Main in TestMain?)")
	}
	name := fmt.Sprintf("kortest_%d_%d", os.Getpid(), dbCounter.Add(1))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN())
	if err != nil {
		t.Fatalf("pgtest: connect: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("pgtest: create database: %v", err)
	}
	return fmt.Sprintf("postgres://kor@127.0.0.1:%d/%s", clusterPort, name)
}

func adminDSN() string {
	return fmt.Sprintf("postgres://kor@127.0.0.1:%d/postgres", clusterPort)
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}
