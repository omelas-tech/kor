// kor is the Kor command-line tool: collection import from a live Firestore
// project, export back to it, content verification, and store stats.
//
//	kor import -collection my_collection -project my-project -kor 127.0.0.1:6565 [-creds sa.json]
//	kor export -collection my_collection -project my-project -kor 127.0.0.1:6565 [-creds sa.json] [-dry-run] [-rate 400]
//	kor verify -collection my_collection -project my-project -kor 127.0.0.1:6565 [-creds sa.json] [-sample 0]
//	kor bench  -collection my_collection -project my-project -pg-dsn postgres://... [-n 300]
//	kor stats  -pg-dsn postgres://...
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"slices"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/jackc/pgx/v5"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/genproto/googleapis/type/latlng"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "import":
		runImport(os.Args[2:])
	case "export":
		runExport(os.Args[2:])
	case "verify":
		runVerify(os.Args[2:])
	case "stats":
		runStats(os.Args[2:])
	case "index":
		runIndex(os.Args[2:])
	case "bench":
		runBench(os.Args[2:])
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kor <import|export|verify|stats|bench|index> [flags]")
	os.Exit(2)
}

// runBench compares point-read latency for the SAME documents served by Kor
// and by real Firestore, interleaved from one process, and prints
// percentiles. The honest per-op comparison behind any endpoint-level number.
func runBench(args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	var cf copyFlags
	fs.StringVar(&cf.collection, "collection", "", "collection id (required)")
	fs.StringVar(&cf.project, "project", "", "GCP project id (required)")
	fs.StringVar(&cf.korAddr, "kor", "127.0.0.1:6565", "kord gRPC address")
	fs.StringVar(&cf.creds, "creds", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), "service account JSON")
	n := fs.Int("n", 300, "number of documents to read per backend")
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "kor PostgreSQL DSN (samples random doc ids)")
	_ = fs.Parse(args)
	if cf.collection == "" || cf.project == "" || *dsn == "" {
		fs.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect pg: %v", err)
	}
	rows, err := conn.Query(ctx,
		`SELECT doc_id FROM documents WHERE collection_id=$1 ORDER BY random() LIMIT $2`, cf.collection, *n)
	if err != nil {
		log.Fatalf("sample ids: %v", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			log.Fatal(err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	conn.Close(ctx)
	if len(ids) == 0 {
		log.Fatalf("no documents found in %s", cf.collection)
	}

	src := sourceClient(ctx, cf)
	defer src.Close()
	kor := korClient(ctx, cf)
	defer kor.Close()

	// Warm both channels so connection setup isn't measured.
	for i := 0; i < 5 && i < len(ids); i++ {
		_, _ = src.Collection(cf.collection).Doc(ids[i]).Get(ctx)
		_, _ = kor.Collection(cf.collection).Doc(ids[i]).Get(ctx)
	}

	timeGet := func(c *firestore.Client, id string) (time.Duration, error) {
		t0 := time.Now()
		_, err := c.Collection(cf.collection).Doc(id).Get(ctx)
		return time.Since(t0), err
	}

	var korDur, fsDur []time.Duration
	var korErr, fsErr int
	for i, id := range ids {
		// Alternate order per iteration so neither backend benefits from the
		// other's page-cache warmup of the same doc.
		order := []struct {
			c    *firestore.Client
			durs *[]time.Duration
			errs *int
		}{{kor, &korDur, &korErr}, {src, &fsDur, &fsErr}}
		if i%2 == 1 {
			order[0], order[1] = order[1], order[0]
		}
		for _, b := range order {
			d, err := timeGet(b.c, id)
			if err != nil {
				*b.errs++
				continue
			}
			*b.durs = append(*b.durs, d)
		}
	}

	report := func(name string, durs []time.Duration, errs int) {
		if len(durs) == 0 {
			fmt.Printf("%-10s no successful reads (%d errors)\n", name, errs)
			return
		}
		sorted := append([]time.Duration(nil), durs...)
		slices.Sort(sorted)
		pct := func(p float64) time.Duration {
			idx := int(p*float64(len(sorted))) - 1
			if idx < 0 {
				idx = 0
			}
			return sorted[idx]
		}
		var sum time.Duration
		for _, d := range sorted {
			sum += d
		}
		fmt.Printf("%-10s n=%d errs=%d  p50=%s  p90=%s  p95=%s  p99=%s  avg=%s\n",
			name, len(sorted), errs,
			pct(0.50).Round(10*time.Microsecond), pct(0.90).Round(10*time.Microsecond),
			pct(0.95).Round(10*time.Microsecond), pct(0.99).Round(10*time.Microsecond),
			(sum / time.Duration(len(sorted))).Round(10*time.Microsecond))
	}
	fmt.Printf("bench: %d docs from %s, interleaved single-threaded point reads\n", len(ids), cf.collection)
	report("kor", korDur, korErr)
	report("firestore", fsDur, fsErr)
}

type copyFlags struct {
	collection string
	project    string
	korAddr    string
	creds      string
	sample     int
}

func parseCopyFlags(name string, args []string, withSample bool) copyFlags {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	var cf copyFlags
	fs.StringVar(&cf.collection, "collection", "", "collection id to copy (required)")
	fs.StringVar(&cf.project, "project", "", "source GCP project id (required)")
	fs.StringVar(&cf.korAddr, "kor", "127.0.0.1:6565", "kord gRPC address")
	fs.StringVar(&cf.creds, "creds", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), "service account JSON for the source project")
	if withSample {
		fs.IntVar(&cf.sample, "sample", 0, "verify only every Nth document (0 = all)")
	}
	_ = fs.Parse(args)
	if cf.collection == "" || cf.project == "" {
		fs.Usage()
		os.Exit(2)
	}
	return cf
}

// sourceClient connects to real Firestore with normal credentials.
func sourceClient(ctx context.Context, cf copyFlags) *firestore.Client {
	var opts []option.ClientOption
	if cf.creds != "" {
		opts = append(opts, option.WithCredentialsFile(cf.creds))
	}
	c, err := firestore.NewClient(ctx, cf.project, opts...)
	if err != nil {
		log.Fatalf("source client: %v", err)
	}
	return c
}

// korClient connects the official SDK to kord over an explicit insecure gRPC
// connection — no FIRESTORE_EMULATOR_HOST needed, so both clients coexist in
// one process.
func korClient(ctx context.Context, cf copyFlags) *firestore.Client {
	conn, err := grpc.NewClient(cf.korAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial kor: %v", err)
	}
	c, err := firestore.NewClient(ctx, cf.project, option.WithGRPCConn(conn))
	if err != nil {
		log.Fatalf("kor client: %v", err)
	}
	return c
}

// forEachDoc pages through a collection with fresh short-lived queries
// (OrderBy __name__ + StartAfter cursor). A single long Documents() stream
// over a large collection hits Firestore's "Query timed out" after ~60s;
// paging sidesteps it and makes runs resumable. Transient page errors retry
// from the cursor.
//
// durable reports the last id the caller has durably persisted (for the
// importer, the last committed doc). Retries resume from it rather than from
// the failed attempt's own progress, which may be empty.
func forEachDoc(ctx context.Context, src *firestore.Client, collection, startAfter string, durable func() string, fn func(*firestore.DocumentSnapshot) error) (last string, total int, err error) {
	const pageSize = 3000
	cursor := startAfter
	for {
		q := src.Collection(collection).OrderBy(firestore.DocumentID, firestore.Asc).Limit(pageSize)
		if cursor != "" {
			q = q.StartAfter(cursor)
		}
		var pageDocs int
		var pageErr error
		for attempt := 1; attempt <= 4; attempt++ {
			var pageLast string
			pageDocs, pageLast, pageErr = readPage(ctx, q, fn)
			if pageErr == nil {
				cursor = pageLast
				break
			}
			// A failed attempt reports only the docs IT processed, so pageLast
			// may be "" (or behind) even though earlier attempts got further.
			// Resume from the caller's durable cursor — the last *committed*
			// doc — and never let a failure move the cursor backwards, which
			// would restart the whole scan from the beginning.
			if attempt < 4 {
				if resumeAt := durable(); resumeAt != "" {
					cursor = resumeAt
				}
				log.Printf("page after %q failed (attempt %d): %v — retrying", cursor, attempt, pageErr)
				time.Sleep(time.Duration(attempt*attempt) * time.Second)
				q = src.Collection(collection).OrderBy(firestore.DocumentID, firestore.Asc).Limit(pageSize)
				if cursor != "" {
					q = q.StartAfter(cursor)
				}
			}
		}
		if pageErr != nil {
			return cursor, total, pageErr
		}
		total += pageDocs
		if pageDocs < pageSize {
			return cursor, total, nil
		}
	}
}

// estimateSize approximates a document's encoded payload in bytes. It only has
// to be good enough to keep a batch under a message-size cap, so it uses fixed
// costs for scalars rather than encoding anything.
func estimateSize(v any) int {
	switch t := v.(type) {
	case nil:
		return 1
	case string:
		return len(t) + 2
	case []byte:
		return len(t) + 2
	case map[string]any:
		n := 2
		for k, val := range t {
			n += len(k) + 3 + estimateSize(val)
		}
		return n
	case []any:
		n := 2
		for _, val := range t {
			n += estimateSize(val) + 1
		}
		return n
	default:
		// numbers, bools, timestamps, refs, geopoints
		return 16
	}
}

func readPage(ctx context.Context, q firestore.Query, fn func(*firestore.DocumentSnapshot) error) (n int, last string, err error) {
	it := q.Documents(ctx)
	defer it.Stop()
	for {
		snap, err := it.Next()
		if err == iterator.Done {
			return n, last, nil
		}
		if err != nil {
			return n, last, err
		}
		if err := fn(snap); err != nil {
			return n, last, err
		}
		n++
		last = snap.Ref.ID
	}
}

func runImport(args []string) {
	fs := flag.NewFlagSet("import", flag.ExitOnError)
	var cf copyFlags
	fs.StringVar(&cf.collection, "collection", "", "collection id to copy (required)")
	fs.StringVar(&cf.project, "project", "", "source GCP project id (required)")
	fs.StringVar(&cf.korAddr, "kor", "127.0.0.1:6565", "kord gRPC address")
	fs.StringVar(&cf.creds, "creds", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), "service account JSON for the source project")
	resumeAfter := fs.String("resume-after", "", "resume after this document id (overrides state file)")
	stateFile := fs.String("state", "", "path to a cursor state file (written per batch, read on start)")
	idsFile := fs.String("ids", "", "targeted sync: copy only the doc ids listed in this file (one per line)")
	_ = fs.Parse(args)
	if cf.collection == "" || cf.project == "" {
		fs.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	src := sourceClient(ctx, cf)
	defer src.Close()
	dst := korClient(ctx, cf)
	defer dst.Close()

	if *idsFile != "" {
		importIDs(ctx, cf, src, dst, *idsFile)
		return
	}

	cursor := *resumeAfter
	if cursor == "" && *stateFile != "" {
		if b, err := os.ReadFile(*stateFile); err == nil {
			cursor = string(bytes.TrimSpace(b))
			log.Printf("resuming after %q from state file", cursor)
		}
	}

	start := time.Now()
	// Cap a commit by BOTH document count and estimated payload bytes. Count
	// alone is not enough: collections vary by two orders of magnitude in doc
	// size (a text cache averages ~1.5 KB/doc, a TMDB person mirror ~22 KB with
	// a 640 KB tail), so a fixed 100-doc batch can build a >20 MB Commit that
	// blows past gRPC's 4 MB default message limit and dies as a deadline. The
	// byte cap keeps a commit well inside that regardless of collection shape.
	const (
		batchSize  = 100
		batchBytes = 2 << 20 // 2 MiB of estimated document payload
	)
	batch := dst.Batch()
	inBatch, batchedBytes, imported := 0, 0, 0
	var lastInBatch, lastCommitted string
	flush := func() error {
		if inBatch == 0 {
			return nil
		}
		_, err := batch.Commit(ctx)
		// Reset unconditionally: on failure the retry re-reads these docs from
		// lastCommitted, so keeping the failed writes would recommit them and
		// grow the batch on every attempt until it can never succeed.
		batch = dst.Batch()
		inBatch, batchedBytes = 0, 0
		if err != nil {
			return fmt.Errorf("commit batch: %w", err)
		}
		lastCommitted = lastInBatch
		if *stateFile != "" {
			_ = os.WriteFile(*stateFile, []byte(lastCommitted), 0o644)
		}
		return nil
	}

	durable := func() string { return lastCommitted }

	last, total, err := forEachDoc(ctx, src, cf.collection, cursor, durable, func(snap *firestore.DocumentSnapshot) error {
		data := snap.Data()
		batch.Set(dst.Collection(cf.collection).Doc(snap.Ref.ID), data)
		inBatch++
		batchedBytes += estimateSize(data)
		imported++
		lastInBatch = snap.Ref.ID
		if inBatch >= batchSize || batchedBytes >= batchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if imported%5000 == 0 {
			log.Printf("imported %d docs (%.0f/s), at %s", imported, float64(imported)/time.Since(start).Seconds(), snap.Ref.ID)
		}
		return nil
	})
	if err != nil {
		_ = flush()
		log.Fatalf("import failed at cursor %q after %d docs: %v (re-run to resume)", last, imported, err)
	}
	if err := flush(); err != nil {
		log.Fatalf("final flush: %v", err)
	}
	log.Printf("DONE: imported %d docs from %s/%s in %s (source scanned %d)",
		imported, cf.project, cf.collection, time.Since(start).Round(time.Second), total)
}

// importIDs copies an explicit id list source→kor (targeted sync after an
// out-of-band batch wrote to the source store).
func importIDs(ctx context.Context, cf copyFlags, src, dst *firestore.Client, idsFile string) {
	raw, err := os.ReadFile(idsFile)
	if err != nil {
		log.Fatalf("read ids file: %v", err)
	}
	var refs []*firestore.DocumentRef
	for _, line := range bytes.Split(raw, []byte("\n")) {
		id := string(bytes.TrimSpace(line))
		if id != "" {
			refs = append(refs, src.Collection(cf.collection).Doc(id))
		}
	}
	copied, missing := 0, 0
	for start := 0; start < len(refs); start += 100 {
		end := start + 100
		if end > len(refs) {
			end = len(refs)
		}
		snaps, err := src.GetAll(ctx, refs[start:end])
		if err != nil {
			log.Fatalf("source GetAll: %v", err)
		}
		batch := dst.Batch()
		n := 0
		for _, s := range snaps {
			if !s.Exists() {
				missing++
				log.Printf("MISSING in source: %s", s.Ref.ID)
				continue
			}
			batch.Set(dst.Collection(cf.collection).Doc(s.Ref.ID), s.Data())
			n++
		}
		if n > 0 {
			if _, err := batch.Commit(ctx); err != nil {
				log.Fatalf("kor commit: %v", err)
			}
			copied += n
		}
	}
	log.Printf("SYNC DONE: %d copied, %d missing (of %d requested)", copied, missing, len(refs))
}

func runVerify(args []string) {
	cf := parseCopyFlags("verify", args, true)
	ctx := context.Background()
	src := sourceClient(ctx, cf)
	defer src.Close()
	dst := korClient(ctx, cf)
	defer dst.Close()

	const chunk = 100
	var (
		pending  []*firestore.DocumentSnapshot
		total    int
		checked  int
		missing  int
		mismatch int
	)
	compareChunk := func() {
		if len(pending) == 0 {
			return
		}
		refs := make([]*firestore.DocumentRef, len(pending))
		for i, s := range pending {
			refs[i] = dst.Collection(cf.collection).Doc(s.Ref.ID)
		}
		snaps, err := dst.GetAll(ctx, refs)
		if err != nil {
			log.Fatalf("kor GetAll: %v", err)
		}
		for i, ks := range snaps {
			if !ks.Exists() {
				missing++
				log.Printf("MISSING in kor: %s", pending[i].Ref.ID)
				continue
			}
			if !equalData(pending[i].Data(), ks.Data()) {
				mismatch++
				log.Printf("MISMATCH: %s", pending[i].Ref.ID)
			}
			checked++
		}
		pending = pending[:0]
	}

	// verify is read-only, so there is no durable write position to resume
	// from — retries fall back to the failed attempt's own progress.
	_, scanned, err := forEachDoc(ctx, src, cf.collection, "", func() string { return "" }, func(snap *firestore.DocumentSnapshot) error {
		total++
		if cf.sample > 1 && total%cf.sample != 0 {
			return nil
		}
		pending = append(pending, snap)
		if len(pending) >= chunk {
			compareChunk()
		}
		if total%20000 == 0 {
			log.Printf("verify progress: %d scanned, %d checked", total, checked)
		}
		return nil
	})
	if err != nil {
		log.Fatalf("source iterate after %d docs: %v", scanned, err)
	}
	compareChunk()
	log.Printf("VERIFY: source=%d checked=%d missing=%d mismatch=%d", total, checked, missing, mismatch)
	if missing > 0 || mismatch > 0 {
		os.Exit(1)
	}
}

func runStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "kor PostgreSQL DSN")
	_ = fs.Parse(args)
	if *dsn == "" {
		log.Fatal("stats: -pg-dsn required")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, *dsn)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, `
		SELECT collection_id, count(*), pg_size_pretty(sum(pg_column_size(data))::bigint)
		FROM documents GROUP BY collection_id ORDER BY count(*) DESC`)
	if err != nil {
		log.Fatalf("query: %v", err)
	}
	defer rows.Close()
	fmt.Printf("%-40s %10s %12s\n", "COLLECTION", "DOCS", "DATA")
	for rows.Next() {
		var coll, size string
		var n int64
		if err := rows.Scan(&coll, &n, &size); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-40s %10d %12s\n", coll, n, size)
	}
}

// equalData deep-compares two decoded snapshot maps with Firestore-appropriate
// semantics (times by instant, refs by path, NaN==NaN, bytes by content).
func equalData(a, b any) bool {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, x := range av {
			y, ok := bv[k]
			if !ok || !equalData(x, y) {
				return false
			}
		}
		return true
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !equalData(av[i], bv[i]) {
				return false
			}
		}
		return true
	case time.Time:
		bv, ok := b.(time.Time)
		return ok && av.Truncate(time.Microsecond).Equal(bv.Truncate(time.Microsecond))
	case []byte:
		bv, ok := b.([]byte)
		return ok && bytes.Equal(av, bv)
	case *firestore.DocumentRef:
		bv, ok := b.(*firestore.DocumentRef)
		return ok && av.Path == bv.Path
	case *latlng.LatLng:
		bv, ok := b.(*latlng.LatLng)
		return ok && av.GetLatitude() == bv.GetLatitude() && av.GetLongitude() == bv.GetLongitude()
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return false
		}
		if math.IsNaN(av) {
			return math.IsNaN(bv)
		}
		return av == bv
	default:
		return a == b
	}
}
