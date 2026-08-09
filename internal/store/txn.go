package store

import (
	"context"
	"crypto/rand"
	"sync"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Transactions follow the design's "no held Postgres snapshots" model:
// BeginTransaction allocates a server-side read-set; transactional reads are
// served at latest-committed state while recording (doc, update_time); Commit
// re-locks every read and written document with SELECT ... FOR UPDATE,
// verifies the recorded versions are unchanged, and applies the writes — or
// returns ABORTED so the SDK's RunTransaction retries. This is serializable
// for point-read transactions; queries inside transactions are rejected
// upstream until predicate validation exists.
const txnTTL = 5 * time.Minute

type txnEntry struct {
	reads   map[string]time.Time // doc name -> update_time at read (zero = missing)
	created time.Time
}

type txnRegistry struct {
	mu sync.Mutex
	m  map[string]*txnEntry
}

func (r *txnRegistry) begin() []byte {
	id := make([]byte, 16)
	_, _ = rand.Read(id)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]*txnEntry{}
	}
	// Lazy expiry sweep; transaction counts are small (one per in-flight
	// RunTransaction), so a full walk is fine.
	now := time.Now()
	for k, e := range r.m {
		if now.Sub(e.created) > txnTTL {
			delete(r.m, k)
		}
	}
	r.m[string(id)] = &txnEntry{reads: map[string]time.Time{}, created: now}
	return id
}

func (r *txnRegistry) record(id []byte, reads map[string]time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[string(id)]
	if !ok {
		return status.Error(codes.InvalidArgument, "unknown or expired transaction")
	}
	for name, t := range reads {
		// First read wins: the version the transaction logic actually saw.
		if _, seen := e.reads[name]; !seen {
			e.reads[name] = t
		}
	}
	return nil
}

func (r *txnRegistry) take(id []byte) (*txnEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.m[string(id)]
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "unknown or expired transaction")
	}
	delete(r.m, string(id))
	return e, nil
}

func (r *txnRegistry) drop(id []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.m, string(id))
}

// BeginTxn starts a transaction and returns its opaque id.
func (s *Store) BeginTxn() []byte { return s.txns.begin() }

// RecordTxnReads notes the versions observed by transactional reads.
func (s *Store) RecordTxnReads(id []byte, reads map[string]time.Time) error {
	return s.txns.record(id, reads)
}

// RollbackTxn discards a transaction.
func (s *Store) RollbackTxn(id []byte) { s.txns.drop(id) }

// CommitTxn atomically verifies the transaction's reads and applies writes.
// Version drift returns ABORTED, which Firestore SDKs retry automatically.
func (s *Store) CommitTxn(ctx context.Context, id []byte, ws []*pb.Write) ([]*pb.WriteResult, time.Time, error) {
	e, err := s.txns.take(id)
	if err != nil {
		return nil, time.Time{}, err
	}
	extraLocks := make([]string, 0, len(e.reads))
	for name := range e.reads {
		extraLocks = append(extraLocks, name)
	}
	return s.applyCommit(ctx, ws, extraLocks, e.reads)
}
