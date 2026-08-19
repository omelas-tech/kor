package store

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// queryPath counts which execution path answered each query.
//
// Registering an index and having queries actually USE it are different claims,
// and only the second one matters. Without this, an index can be backfilled,
// marked ready and quietly serve nothing — the planner declining every shape
// looks exactly like the planner working. This is the production form of the
// non-vacuity checks the differential tests make.
//
//	indexed  — served from index_entries, cost O(limit)
//	merged   — an indexed query spanning several ranges (`in`, contains-any)
//	contains — an indexed query served by an array-contains definition
//	name     — the __name__-ordered fast path
//	general  — full evaluation in Go, cost O(matching documents)
//
// merged and contains are subsets of indexed, counted separately because they
// exercise different code.
var queryPath = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Name: "kord_query_path_total",
		Help: "Queries by execution path (merged and contains are subsets of indexed).",
	},
	[]string{"path"},
)
