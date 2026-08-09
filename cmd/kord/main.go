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
