package rpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

// rpcStats accumulates per-method counters, reported periodically so the
// pilot deployment is observable without a metrics stack.
type rpcStats struct {
	mu     sync.Mutex
	counts map[string]int64
	errors map[string]int64
}

var stats = &rpcStats{counts: map[string]int64{}, errors: map[string]int64{}}

func (s *rpcStats) record(method string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.counts[method]++
	if err != nil {
		s.errors[method]++
	}
}

func (s *rpcStats) snapshotAndReset() (counts, errors map[string]int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts, errors = s.counts, s.errors
	s.counts, s.errors = map[string]int64{}, map[string]int64{}
	return counts, errors
}

// StartStatsReporter logs accumulated RPC counters every interval (skipping
// idle periods).
func StartStatsReporter(ctx context.Context, log *slog.Logger, interval time.Duration) {
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				counts, errors := stats.snapshotAndReset()
				if len(counts) == 0 {
					continue
				}
				attrs := make([]any, 0, len(counts)*2+2)
				for m, n := range counts {
					attrs = append(attrs, m, n)
				}
				if len(errors) > 0 {
					attrs = append(attrs, "errors", errors)
				}
				log.Info("kord rpc stats", attrs...)
			}
		}
	}()
}

// UnaryLogging returns a unary interceptor that counts and debug-logs RPCs.
func UnaryLogging(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		stats.record(info.FullMethod, err)
		rpcDuration.WithLabelValues(info.FullMethod, status.Code(err).String()).Observe(time.Since(start).Seconds())
		log.Debug("rpc", "method", info.FullMethod, "code", status.Code(err).String(),
			"ms", time.Since(start).Milliseconds())
		return resp, err
	}
}

// StreamLogging returns the streaming counterpart of UnaryLogging.
func StreamLogging(log *slog.Logger) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		stats.record(info.FullMethod, err)
		rpcDuration.WithLabelValues(info.FullMethod, status.Code(err).String()).Observe(time.Since(start).Seconds())
		log.Debug("rpc", "method", info.FullMethod, "code", status.Code(err).String(),
			"ms", time.Since(start).Milliseconds())
		return err
	}
}
