# Realtime: `Listen` and `Write`

kord implements the two streaming RPCs the client SDKs use beyond plain
reads and commits:

- **`Listen`** — `onSnapshot` (JS, Flutter, iOS, Android) and `Snapshots()`
  (Go). The native and JS SDKs also use it for a server `get()`.
- **`Write`** — the native mobile SDKs (and firebase-js under Node) send their
  writes on this stream instead of `Commit`.

Browsers are not covered yet: firebase-js in a browser speaks WebChannel, not
gRPC (roadmap phase 5). Kor still has no authentication or security rules
(phase 4), so these streams are only for trusted callers on loopback or a
private network, like every other RPC.

## How changes reach a listener

One poller per kord reads the change log (the same low-water-marked feed the
functions runtime and search indexers use) every 100 ms while at least one
stream is open, and fans each batch out to every stream. It stops when the
last stream closes, and re-reads the feed head when the next one opens, so a
new listener is never replayed history.

A target's initial result is read **after** the stream subscribed to the
hub. A change committed before the read is in the result; one committed after
it arrives as an event; one in between may be both. That overlap is harmless:
a document is sent only when its update time differs from what the client
already holds.

For each change, kord re-reads the document's current state (the feed
carries no data) and decides membership with the same `Matches` and cursor
check `RunQuery` uses — filters, every orderBy field present, start/end
cursors. A query with a `limit` or `offset` re-runs instead: there,
membership depends on the other documents (deleting the first result pulls
the next one in).

The client then gets `DocumentChange`, `DocumentRemove` (left the result) or
`DocumentDelete`, followed by a global `NO_CHANGE` with a `read_time`, which is
what tells every SDK to raise a snapshot. Read times never go backwards on a
stream, because the mobile SDKs reject a snapshot older than one they raised.

## Deliberate simplifications

- **Resume tokens resync instead of replaying.** A target added with a resume
  token (a client reconnecting) gets `RESET` and the full current result.
  That is always correct; a true incremental resume would need to know which
  documents left the result while the client was away (Firestore's existence
  filter), and costs one re-read of each listener's result per reconnect.
- **Slow consumers are disconnected, not buffered forever.** A stream that
  falls 256 batches behind ends with `UNAVAILABLE`; the SDK reconnects and
  resyncs. Counted in `kord_listen_dropped_total`.
- **No existence filters, no bloom filters.** Sent only by Firestore to
  repair drifted clients; Kor's client view is exact.
- **`Write` tokens acknowledge position only.** Every batch is committed
  before it is answered, so there is nothing to resume.
- **A failed batch ends the `Write` stream with its error**, as Firestore's
  does: the SDK rejects that batch and reopens for the next.

Latency is bounded by the poll interval: a change reaches listeners within
about 100 ms of commit.

## Metrics

`kord_listen_streams` (open streams), `kord_listen_targets` (active targets),
`kord_listen_dropped_total` (streams ended for falling behind). RPC latency
for the streams is in `kord_rpc_duration_seconds` like every other method,
measured over the stream's whole life.

## Tests

- `e2e/listen_test.go` — the Go SDK end to end (query and document
  lifecycles, limit windows) plus raw gRPC for what the Go SDK never sends:
  multi-target streams, target removal, per-target errors, resume, and the
  `Write` stream.
- `e2e/listen_diff_test.go` — differential against Google's emulator: random
  writes with seven listeners on both stores; after every write each
  listener must equal the emulator's own query result.
  `KOR_FUZZ_EMULATOR=127.0.0.1:8432 go test ./e2e/ -run TestListenDifferential`
- `e2e/js/run.sh` — the real firebase-js SDK over gRPC: one app writes while
  another listens, batches, transactions, server gets, target removal, and
  recovery after a rejected write.
  `FIREBASE_NODE_MODULES=/path/to/node_modules e2e/js/run.sh`
