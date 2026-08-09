package rpc

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var rpcDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "kord_rpc_duration_seconds",
		Help:    "Firestore RPC latency served by kord, by full method and status code.",
		Buckets: []float64{0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	},
	[]string{"method", "code"},
)

// ServeMetrics exposes Prometheus metrics on addr (bind loopback only).
// Returns the server so callers can Close it; errors are returned via the
// channel-free convention of http.Server (caller logs ListenAndServe result).
func ServeMetrics(addr string) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	return srv
}
