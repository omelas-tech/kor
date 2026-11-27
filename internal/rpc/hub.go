package rpc

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/omelas-tech/kor/internal/store"
)

// hub tails the change log once and fans each batch out to every open Listen
// stream. One poller serves all listeners, so the cost of realtime is one
// indexed changelog read per tick, not one per client.
//
// It runs only while somebody listens. The cursor is (re)taken from the feed
// head inside subscribe, before the subscriber runs its initial query: any
// change at or below the head is visible to that query, and any change above
// it is delivered as an event. The two can overlap (a change visible to the
// query AND delivered), never leave a gap; listeners dedupe by update time.
type hub struct {
	st   *store.Store
	log  *slog.Logger
	poll time.Duration

	mu      sync.Mutex
	subs    map[*subscriber]struct{}
	cursor  store.Cursor
	running bool
}

// subscriber is one Listen stream's inbox. lost closes when the hub had to
// drop a batch for it; the stream then ends with UNAVAILABLE and the client
// reconnects and resyncs, which is the only correct recovery from a gap.
type subscriber struct {
	ch   chan []store.Change
	lost chan struct{}
	once sync.Once
}

const subscriberBuffer = 256

var (
	listenStreams = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kord_listen_streams", Help: "Open Listen streams.",
	})
	listenTargets = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "kord_listen_targets", Help: "Active Listen targets across all streams.",
	})
	listenDropped = promauto.NewCounter(prometheus.CounterOpts{
		Name: "kord_listen_dropped_total", Help: "Listen streams ended because they fell behind the change feed.",
	})
)

func newHub(st *store.Store, log *slog.Logger) *hub {
	if log == nil {
		log = slog.Default()
	}
	return &hub{st: st, log: log, poll: 100 * time.Millisecond, subs: map[*subscriber]struct{}{}}
}

func (h *hub) subscribe(ctx context.Context) (*subscriber, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.running {
		head, err := h.st.FeedHead(ctx)
		if err != nil {
			return nil, err
		}
		h.cursor = head
		h.running = true
		go h.run()
	}
	s := &subscriber{ch: make(chan []store.Change, subscriberBuffer), lost: make(chan struct{})}
	h.subs[s] = struct{}{}
	listenStreams.Inc()
	return s, nil
}

func (h *hub) unsubscribe(s *subscriber) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.subs[s]; ok {
		delete(h.subs, s)
		listenStreams.Dec()
	}
}

func (h *hub) run() {
	t := time.NewTicker(h.poll)
	defer t.Stop()
	for range t.C {
		h.mu.Lock()
		if len(h.subs) == 0 {
			// Nobody listens: stop. The next subscribe re-reads the head, so
			// changes made while idle are never replayed to a new listener.
			h.running = false
			h.mu.Unlock()
			return
		}
		cursor := h.cursor
		h.mu.Unlock()

		for {
			batch, err := h.st.ReadFeed(context.Background(), cursor, nil, false, 1000)
			if err != nil {
				h.log.Warn("listen: read change feed", "err", err)
				break
			}
			if len(batch) == 0 {
				break
			}
			last := batch[len(batch)-1]
			cursor = store.Cursor{XID: last.XID, Seq: last.Seq}
			h.deliver(batch, cursor)
			if len(batch) < 1000 {
				break
			}
		}
	}
}

func (h *hub) deliver(batch []store.Change, cursor store.Cursor) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cursor = cursor
	for s := range h.subs {
		select {
		case s.ch <- batch:
		default:
			s.once.Do(func() {
				listenDropped.Inc()
				close(s.lost)
			})
		}
	}
}
