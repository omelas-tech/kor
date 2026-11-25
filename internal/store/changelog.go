package store

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Change is one document event.
//
// By default it carries no document data. Consumers re-read current state by
// name, which makes the feed idempotent and self-healing: replaying an event
// twice converges, and a consumer that falls behind skips straight to the
// latest value instead of replaying a history it no longer cares about.
//
// Collections registered in feed_images (RegisterFeedImages) additionally carry
// the document as it was before and after this write — what a Firestore
// trigger receives, and something no later read can reconstruct. HasImages
// says whether they were captured; when it is true, a nil Before means the
// document did not exist (a create) and a nil After means it was deleted.
type Change struct {
	Seq          int64
	XID          uint64
	DocName      string
	CollectionID string
	Kind         string // write | delete
	At           time.Time
	HasImages    bool
	Before       *pb.Document
	After        *pb.Document
}

// Cursor is a resume position. The zero value starts from the beginning.
type Cursor struct {
	XID uint64
	Seq int64
}

// appendChange records one event inside the caller's transaction.
//
// Sharing the transaction is what makes the feed trustworthy: an event that
// committed without its document, or a document without its event, would leave
// consumers permanently out of step with no way to detect it.
//
// When images is non-nil the before/after documents are stored with the event
// (either may be nil: create has no before, delete no after).
func appendChange(ctx context.Context, tx pgx.Tx, name, collectionID, kind string, images *changeImages) error {
	var before, after []byte
	if images != nil {
		var err error
		if before, err = encodeImage(images.before); err != nil {
			return fmt.Errorf("store: changelog before-image for %s: %w", name, err)
		}
		if after, err = encodeImage(images.after); err != nil {
			return fmt.Errorf("store: changelog after-image for %s: %w", name, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO changelog (xid, doc_name, collection_id, kind, has_images, before, after)
		VALUES (pg_current_xact_id(), $1, $2, $3, $4, $5, $6)`,
		name, collectionID, kind, images != nil, before, after); err != nil {
		return fmt.Errorf("store: append changelog for %s: %w", name, err)
	}
	return nil
}

// changeImages is the pair captured for a registered collection.
type changeImages struct{ before, after *pb.Document }

// imageOf builds the Document a trigger would see. nil for "does not exist".
func imageOf(name string, exists bool, fields map[string]*pb.Value, created, updated time.Time) *pb.Document {
	if !exists {
		return nil
	}
	return &pb.Document{
		Name:       name,
		Fields:     fields,
		CreateTime: timestamppb.New(created),
		UpdateTime: timestamppb.New(updated),
	}
}

func encodeImage(d *pb.Document) ([]byte, error) {
	if d == nil {
		return nil, nil
	}
	return proto.Marshal(d)
}

func decodeImage(b []byte) (*pb.Document, error) {
	if b == nil {
		return nil, nil
	}
	d := &pb.Document{}
	if err := proto.Unmarshal(b, d); err != nil {
		return nil, err
	}
	return d, nil
}

// Changes returns events after the cursor, in commit order, up to limit.
//
// collectionID filters to one collection when non-empty. Images are not
// loaded; use ReadFeed for those.
func (s *Store) Changes(ctx context.Context, after Cursor, collectionID string, limit int) ([]Change, error) {
	var colls []string
	if collectionID != "" {
		colls = []string{collectionID}
	}
	return s.ReadFeed(ctx, after, colls, false, limit)
}

// ReadFeed returns events after the cursor, in commit order, up to limit.
//
// Only rows below the low-water mark are returned — see the 0004 migration
// comment. The cost is latency, not correctness: an event stays invisible while
// any older transaction is still open, so a long-running transaction holds the
// feed back. That is the right trade for a change feed, where a late event is
// recoverable and a lost one is not.
//
// collections filters when non-empty; images also decodes the before/after
// documents of rows that captured them.
func (s *Store) ReadFeed(ctx context.Context, after Cursor, collections []string, images bool, limit int) ([]Change, error) {
	if limit <= 0 {
		limit = 500
	}
	// The cursor xid crosses the wire as text: pgx has no encoder for xid8, and
	// uint64 does not fit int8 across the whole range.
	args := []any{strconv.FormatUint(after.XID, 10), after.Seq, limit}
	where := `WHERE xid < pg_snapshot_xmin(pg_current_snapshot())
	            AND (xid > $1::text::xid8 OR (xid = $1::text::xid8 AND seq > $2))`
	if len(collections) > 0 {
		args = append(args, collections)
		where += fmt.Sprintf(" AND collection_id = ANY($%d)", len(args))
	}
	cols := `seq, xid::text, doc_name, collection_id, kind, at, has_images`
	if images {
		cols += `, before, after`
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+cols+` FROM changelog `+where+`
		ORDER BY xid, seq LIMIT $3`, args...)
	if err != nil {
		return nil, fmt.Errorf("store: read changelog: %w", err)
	}
	defer rows.Close()
	var out []Change
	for rows.Next() {
		var c Change
		var xid string
		var before, after []byte
		dest := []any{&c.Seq, &xid, &c.DocName, &c.CollectionID, &c.Kind, &c.At, &c.HasImages}
		if images {
			dest = append(dest, &before, &after)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		if c.XID, err = strconv.ParseUint(xid, 10, 64); err != nil {
			return nil, fmt.Errorf("store: changelog xid %q: %w", xid, err)
		}
		if images && c.HasImages {
			if c.Before, err = decodeImage(before); err != nil {
				return nil, fmt.Errorf("store: changelog %d before-image: %w", c.Seq, err)
			}
			if c.After, err = decodeImage(after); err != nil {
				return nil, fmt.Errorf("store: changelog %d after-image: %w", c.Seq, err)
			}
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// FeedHead is the cursor of the newest event a reader could see right now.
//
// A consumer that only cares about events from now on (a trigger runtime being
// installed, not replaying history) starts here instead of at the zero cursor.
func (s *Store) FeedHead(ctx context.Context) (Cursor, error) {
	var xid string
	var seq int64
	err := s.Pool.QueryRow(ctx, `
		SELECT xid::text, seq FROM changelog
		WHERE xid < pg_snapshot_xmin(pg_current_snapshot())
		ORDER BY xid DESC, seq DESC LIMIT 1`).Scan(&xid, &seq)
	if err == pgx.ErrNoRows {
		return Cursor{}, nil
	}
	if err != nil {
		return Cursor{}, fmt.Errorf("store: feed head: %w", err)
	}
	x, err := strconv.ParseUint(xid, 10, 64)
	if err != nil {
		return Cursor{}, err
	}
	return Cursor{XID: x, Seq: seq}, nil
}

// LoadFeedImages refreshes the in-memory set of collections whose changes
// carry images. RegisterFeedImages updates it immediately; reloading picks up
// registrations made by another process (the CLI) against the same database.
func (s *Store) LoadFeedImages(ctx context.Context) ([]string, error) {
	rows, err := s.Pool.Query(ctx, `SELECT collection_id FROM feed_images ORDER BY collection_id`)
	if err != nil {
		return nil, fmt.Errorf("store: load feed_images: %w", err)
	}
	colls, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(colls))
	for _, c := range colls {
		set[c] = true
	}
	s.feedImages.Store(&set)
	return colls, nil
}

// RegisterFeedImages makes future changes in these collections carry images,
// and returns the full registered set. Takes effect for commits that start
// after it returns; a commit already in flight may still log without images,
// which is why Change.HasImages exists.
func (s *Store) RegisterFeedImages(ctx context.Context, collections []string) ([]string, error) {
	for _, c := range collections {
		if c == "" {
			return nil, fmt.Errorf("store: empty collection id")
		}
	}
	if _, err := s.Pool.Exec(ctx, `
		INSERT INTO feed_images (collection_id) SELECT unnest($1::text[])
		ON CONFLICT DO NOTHING`, collections); err != nil {
		return nil, fmt.Errorf("store: register feed_images: %w", err)
	}
	return s.LoadFeedImages(ctx)
}

// UnregisterFeedImages stops capturing images for these collections.
func (s *Store) UnregisterFeedImages(ctx context.Context, collections []string) ([]string, error) {
	if _, err := s.Pool.Exec(ctx, `DELETE FROM feed_images WHERE collection_id = ANY($1)`, collections); err != nil {
		return nil, fmt.Errorf("store: unregister feed_images: %w", err)
	}
	return s.LoadFeedImages(ctx)
}

// FeedImageCollections is the registered set, sorted.
func (s *Store) FeedImageCollections() []string {
	m := s.feedImages.Load()
	if m == nil {
		return nil
	}
	out := make([]string, 0, len(*m))
	for c := range *m {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func (s *Store) capturesImages(collectionID string) bool {
	m := s.feedImages.Load()
	return m != nil && (*m)[collectionID]
}

// PruneChanges deletes events older than the given age.
//
// Retention is the operator's call and there is no safe universal default: the
// feed must outlive the slowest consumer's downtime, and only the operator
// knows what that is. Pruning below a consumer's cursor does not error — it
// silently makes that consumer's resume position meaningless, which is why this
// is explicit rather than automatic.
func (s *Store) PruneChanges(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.Pool.Exec(ctx,
		`DELETE FROM changelog WHERE at < now() - $1::interval`,
		fmt.Sprintf("%d seconds", int(olderThan.Seconds())))
	if err != nil {
		return 0, fmt.Errorf("store: prune changelog: %w", err)
	}
	return tag.RowsAffected(), nil
}
