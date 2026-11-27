// kord is the Kor server daemon: a Firestore wire-protocol-compatible
// document database backed by PostgreSQL.
//
// Point any Firestore SDK at it:
//
//	FIRESTORE_EMULATOR_HOST=localhost:6565 ./your-app
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	"github.com/omelas-tech/kor/internal/auth"
	"github.com/omelas-tech/kor/internal/feed"
	"github.com/omelas-tech/kor/internal/rpc"
	"github.com/omelas-tech/kor/internal/rules"
	"github.com/omelas-tech/kor/internal/store"
)

// indexReloadInterval trades staleness for load. Index changes are rare and
// administrative, so a few seconds of lag costs nothing, while the query itself
// is one small read against a table with a handful of rows.
const indexReloadInterval = 30 * time.Second

func main() {
	listen := flag.String("listen", envOr("KORD_LISTEN", "127.0.0.1:6565"), "address to serve gRPC on")
	dsn := flag.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	logLevel := flag.String("log", envOr("KORD_LOG", "info"), "log level: debug|info|warn|error")
	metricsAddr := flag.String("metrics", envOr("KORD_METRICS", "127.0.0.1:6566"), "Prometheus /metrics address (empty = disabled)")
	allowPublic := flag.Bool("i-know-this-is-unauthenticated", envOr("KORD_ALLOW_PUBLIC_BIND", "") != "",
		"permit binding a non-loopback address. kord has NO authentication: any client that can reach the port has full read/write on every document.")
	clientListen := flag.String("client-listen", os.Getenv("KORD_CLIENT_LISTEN"),
		"address for the client endpoint (Firebase client SDKs): every request authenticated by ID token and decided by -rules. Empty = disabled")
	rulesFile := flag.String("rules", os.Getenv("KORD_RULES"), "firestore.rules file the client endpoint enforces (reloaded when it changes)")
	project := flag.String("project", os.Getenv("KORD_PROJECT"), "Firebase project id whose ID tokens the client endpoint accepts")
	tlsCert := flag.String("client-tls-cert", os.Getenv("KORD_CLIENT_TLS_CERT"), "TLS certificate for the client endpoint")
	tlsKey := flag.String("client-tls-key", os.Getenv("KORD_CLIENT_TLS_KEY"), "TLS key for the client endpoint")
	unsigned := flag.Bool("accept-unsigned-test-tokens", false,
		"TESTS ONLY: accept unsigned (alg none) ID tokens, as the Firebase emulators issue. Anyone can then act as any user. Requires a loopback -client-listen")
	flag.Parse()

	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
	if *dsn == "" {
		log.Error("missing -pg-dsn / KORD_PG_DSN")
		os.Exit(2)
	}

	// Refuse a non-loopback bind unless the operator opted in explicitly.
	//
	// kord has no authentication, so reachability IS authorization: anyone who
	// can open a TCP connection gets full read/write on every document, with no
	// identity and no per-collection rules. That makes the listen address a
	// security control, not a config value — and a one-character edit
	// ("127.0.0.1" -> "0.0.0.0") the difference between a private database and
	// a public one. Documentation cannot catch that; refusing to boot can.
	//
	// The opt-out is deliberately unpleasant to type, and names the risk rather
	// than the mechanism, so nobody sets it without reading what it means.
	if !*allowPublic {
		if err := requireLoopback(*listen); err != nil {
			log.Error("refusing to start", "addr", *listen, "err", err,
				"remedy", "bind 127.0.0.1, put kord behind a private network, or pass -i-know-this-is-unauthenticated")
			os.Exit(2)
		}
	} else if host, _, e := net.SplitHostPort(*listen); e == nil && !isLoopbackHost(host) {
		log.Warn("SERVING UNAUTHENTICATED ON A NON-LOOPBACK ADDRESS — every client that can reach this port has full read/write on all data",
			"addr", *listen)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(ctx, *dsn)
	if err != nil {
		log.Error("open store", "err", err)
		os.Exit(1)
	}
	defer st.Close()

	// Composite index definitions live in Postgres, so the CLI can register and
	// backfill them against a running server. Reloading on a ticker rather than
	// only at boot means enabling an index does not require a restart — and,
	// more importantly, that a backfill finishing elsewhere takes effect on its
	// own, instead of leaving an index that is complete in the database but
	// still considered pending here.
	//
	// A failure to load is logged, not fatal: indexes are an optimisation, and
	// the general query path serves every query correctly without them.
	if n, err := st.LoadIndexes(ctx); err != nil {
		log.Warn("loading composite indexes", "err", err, "ready", n)
	} else if n > 0 {
		log.Info("composite indexes loaded", "ready", n)
	}
	go func() {
		t := time.NewTicker(indexReloadInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if _, err := st.LoadIndexes(ctx); err != nil {
					log.Warn("reloading composite indexes", "err", err)
				}
				// Picks up feed-image registrations made by the CLI.
				if _, err := st.LoadFeedImages(ctx); err != nil {
					log.Warn("reloading feed image collections", "err", err)
				}
			}
		}
	}()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "addr", *listen, "err", err)
		os.Exit(1)
	}

	g := grpc.NewServer(
		grpc.UnaryInterceptor(rpc.UnaryLogging(log)),
		grpc.StreamInterceptor(rpc.StreamLogging(log)),
	)
	trusted := rpc.New(st)
	trusted.Register(g)

	if *clientListen != "" {
		stopClient, err := serveClient(ctx, log, trusted, *clientListen, *rulesFile, *project, *tlsCert, *tlsKey, *unsigned)
		if err != nil {
			log.Error("refusing to start the client endpoint", "err", err)
			os.Exit(2)
		}
		defer stopClient()
	}
	rpc.StartStatsReporter(ctx, log, time.Minute)
	if *metricsAddr != "" {
		metricsSrv := rpc.ServeMetrics(*metricsAddr, feed.Handler(st, log))
		defer metricsSrv.Close()
		log.Info("metrics + change-feed API listening", "addr", *metricsAddr,
			"feed_images", st.FeedImageCollections())
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		g.GracefulStop()
	}()

	log.Info("kord serving", "addr", lis.Addr().String())
	if err := g.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// requireLoopback rejects any listen address reachable from off-host.
//
// The unspecified addresses (0.0.0.0, ::, and an empty host as in ":6565") are
// the dangerous ones: they bind every interface including the public one, and
// they are exactly what someone reaches for when a container or a private
// network makes loopback inconvenient.
func requireLoopback(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("cannot parse listen address %q: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("listen address %q binds every interface; use 127.0.0.1", addr)
	}
	if isLoopbackHost(host) {
		return nil
	}
	return fmt.Errorf("listen host %q is not loopback and kord has no authentication", host)
}

// isLoopbackHost reports whether host resolves only to loopback. A hostname
// that resolves anywhere else is treated as non-loopback.
func isLoopbackHost(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	if host == "localhost" {
		return true
	}
	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return false
	}
	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}
	return true
}

// serveClient starts the authenticated client endpoint on its own listener.
// It shares the store and change hub with the trusted endpoint but is a
// separate gRPC server, so nothing reachable on it bypasses the rules.
func serveClient(ctx context.Context, log *slog.Logger, trusted *rpc.Server, addr, rulesFile, project, cert, key string, unsigned bool) (func(), error) {
	if rulesFile == "" || project == "" {
		return nil, fmt.Errorf("-client-listen needs -rules and -project")
	}
	loopback := requireLoopback(addr) == nil
	if unsigned && !loopback {
		return nil, fmt.Errorf("-accept-unsigned-test-tokens on non-loopback %s would let anyone act as any user", addr)
	}
	if !loopback && (cert == "" || key == "") {
		// ID tokens are bearer credentials: in clear text they are stolen.
		return nil, fmt.Errorf("client endpoint %s is off-host: give -client-tls-cert/-client-tls-key or terminate TLS in front of a loopback bind", addr)
	}
	rs, mtime, err := loadRules(rulesFile)
	if err != nil {
		return nil, err
	}
	var opts []auth.Option
	if unsigned {
		opts = append(opts, auth.AcceptUnsignedTokens())
		log.Warn("CLIENT ENDPOINT ACCEPTS UNSIGNED TOKENS — any caller can claim any identity (tests only)")
	}
	client := rpc.NewClient(trusted, rs, auth.New(project, nil, opts...), project, log)

	sopts := []grpc.ServerOption{
		grpc.UnaryInterceptor(rpc.UnaryLogging(log)),
		grpc.StreamInterceptor(rpc.StreamLogging(log)),
		// Mobile clients hold streams open for hours behind NATs.
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}),
	}
	if cert != "" {
		creds, err := credentials.NewServerTLSFromFile(cert, key)
		if err != nil {
			return nil, fmt.Errorf("client TLS: %w", err)
		}
		sopts = append(sopts, grpc.Creds(creds))
	}
	g := grpc.NewServer(sopts...)
	client.Register(g)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := g.Serve(lis); err != nil {
			log.Error("client endpoint", "err", err)
		}
	}()
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				fi, err := os.Stat(rulesFile)
				if err != nil || fi.ModTime().Equal(mtime) {
					continue
				}
				next, m, err := loadRules(rulesFile)
				if err != nil {
					// Keep enforcing the last good ruleset; a typo must not open
					// or close the whole database.
					log.Error("rules reload failed; keeping the previous ruleset", "file", rulesFile, "err", err)
					mtime = fi.ModTime()
					continue
				}
				client.SetRules(next)
				mtime = m
				log.Info("rules reloaded", "file", rulesFile)
			}
		}
	}()
	log.Info("client endpoint serving", "addr", lis.Addr().String(), "project", project, "rules", rulesFile, "tls", cert != "")
	return g.GracefulStop, nil
}

func loadRules(path string) (*rules.Ruleset, time.Time, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	src, err := os.ReadFile(path)
	if err != nil {
		return nil, time.Time{}, err
	}
	rs, err := rules.Parse(string(src))
	if err != nil {
		return nil, time.Time{}, err
	}
	return rs, fi.ModTime(), nil
}
