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

	"github.com/omelas-tech/kor/internal/rpc"
	"github.com/omelas-tech/kor/internal/store"
)

func main() {
	listen := flag.String("listen", envOr("KORD_LISTEN", "127.0.0.1:6565"), "address to serve gRPC on")
	dsn := flag.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	logLevel := flag.String("log", envOr("KORD_LOG", "info"), "log level: debug|info|warn|error")
	metricsAddr := flag.String("metrics", envOr("KORD_METRICS", "127.0.0.1:6566"), "Prometheus /metrics address (empty = disabled)")
	allowPublic := flag.Bool("i-know-this-is-unauthenticated", envOr("KORD_ALLOW_PUBLIC_BIND", "") != "",
		"permit binding a non-loopback address. kord has NO authentication: any client that can reach the port has full read/write on every document.")
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

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen", "addr", *listen, "err", err)
		os.Exit(1)
	}

	g := grpc.NewServer(
		grpc.UnaryInterceptor(rpc.UnaryLogging(log)),
		grpc.StreamInterceptor(rpc.StreamLogging(log)),
	)
	rpc.New(st).Register(g)
	rpc.StartStatsReporter(ctx, log, time.Minute)
	if *metricsAddr != "" {
		metricsSrv := rpc.ServeMetrics(*metricsAddr)
		defer metricsSrv.Close()
		log.Info("metrics listening", "addr", *metricsAddr)
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
