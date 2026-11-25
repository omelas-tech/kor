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

// ServeMetrics exposes Prometheus metrics on addr (bind loopback only), plus
// any extra handlers (the change-feed API) on the same admin listener.
// Returns the server so callers can Close it.
func ServeMetrics(addr string, extra ...http.Handler) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	for _, h := range extra {
		mux.Handle("/feed/", h)
	}
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() { _ = srv.ListenAndServe() }()
	return srv
}
