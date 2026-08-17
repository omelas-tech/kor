package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"cloud.google.com/go/firestore"
)

// runExport copies a collection Kor → Firestore: the reverse of `kor import`,
// and the thing that makes a migration reversible.
//
// Without it, a collection served from Kor is a one-way door. Documents written
// after the cutover exist only in Kor, so "point the application back at
// Firestore" silently loses every one of them. With it, rollback is: replay,
// then flip. That is the difference between a migration you can undo and a
// migration you merely believe in.
//
// The scan side is identical to import — forEachDoc pages any store that speaks
// the Firestore protocol, which is the whole point of Kor — so the interesting
// differences are all on the write side:
//
//   - It writes to a live Firestore project. `-dry-run` is offered first in the
//     flag list on purpose, and the summary always states which mode ran.
//   - Firestore throttles sustained writes to a collection where Kor does not,
//     so writes are rate-limited (default 400/s, inside the documented 500/s
//     soft ceiling). Removing the limit on a large collection earns
//     RESOURCE_EXHAUSTED, not speed.
//   - Batches cap at 500 operations, Firestore's hard per-commit limit, as well
//     as by estimated bytes.
//
// Scope, deliberately: this replays writes, it does not replicate deletes. A
// document deleted in Kor after the cutover stays in Firestore. Reconciling
// that means deleting from a store the operator is rolling *back to*, which is
// not a decision a replay tool should make on its own — and in practice deletes
// are vanishingly rare compared to writes. Diff the two with `kor verify`
// afterwards if it matters.
func runExport(args []string) {
	fs := flag.NewFlagSet("export", flag.ExitOnError)
	var cf copyFlags
	dryRun := fs.Bool("dry-run", false, "read and report, write nothing to Firestore")
	fs.StringVar(&cf.collection, "collection", "", "collection id to copy (required)")
	fs.StringVar(&cf.project, "project", "", "destination GCP project id (required)")
	fs.StringVar(&cf.korAddr, "kor", "127.0.0.1:6565", "kord gRPC address (the SOURCE)")
	fs.StringVar(&cf.creds, "creds", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), "service account JSON for the destination project")
	resumeAfter := fs.String("resume-after", "", "resume after this document id (overrides state file)")
	stateFile := fs.String("state", "", "path to a cursor state file (written per batch, read on start)")
	idsFile := fs.String("ids", "", "targeted replay: copy only the doc ids listed in this file (one per line)")
	rate := fs.Int("rate", 400, "max documents written per second (0 = unlimited; Firestore's soft ceiling is ~500/s per collection)")
	_ = fs.Parse(args)
	if cf.collection == "" || cf.project == "" {
		fs.Usage()
		os.Exit(2)
	}

	ctx := context.Background()
	// Note the inversion against runImport: Kor is the source, Firestore the
	// destination.
	src := korClient(ctx, cf)
	defer src.Close()
	dst := sourceClient(ctx, cf)
	defer dst.Close()

	mode := "LIVE"
	if *dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("export %s: kor(%s) -> firestore(%s), collection %s, rate %d/s",
		mode, cf.korAddr, cf.project, cf.collection, *rate)

	if *idsFile != "" {
		exportIDs(ctx, cf, src, dst, *idsFile, *dryRun, newLimiter(*rate))
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
	// 500 is Firestore's hard per-commit operation limit — lower than the 100
	// import uses against Kor for message-size reasons, but the byte cap below
	// is what actually binds on large documents.
	const (
		batchSize  = 500
		batchBytes = 2 << 20
	)
	lim := newLimiter(*rate)
	batch := dst.Batch()
	inBatch, batchedBytes, exported := 0, 0, 0
	var lastInBatch, lastCommitted string

	flush := func() error {
		if inBatch == 0 {
			return nil
		}
		if !*dryRun {
			lim.wait(inBatch)
			if _, err := batch.Commit(ctx); err != nil {
				// Reset unconditionally, as import does: a retry re-reads from
				// lastCommitted, so keeping failed writes would recommit them
				// and grow the batch until it can never succeed.
				batch = dst.Batch()
				inBatch, batchedBytes = 0, 0
				return fmt.Errorf("commit batch: %w", err)
			}
		}
		batch = dst.Batch()
		inBatch, batchedBytes = 0, 0
		lastCommitted = lastInBatch
		if *stateFile != "" && !*dryRun {
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
		exported++
		lastInBatch = snap.Ref.ID
		if inBatch >= batchSize || batchedBytes >= batchBytes {
			if err := flush(); err != nil {
				return err
			}
		}
		if exported%5000 == 0 {
			log.Printf("exported %d docs (%.0f/s), at %s", exported, float64(exported)/time.Since(start).Seconds(), snap.Ref.ID)
		}
		return nil
	})
	if err != nil {
		_ = flush()
		log.Fatalf("export failed at cursor %q after %d docs: %v (re-run to resume)", last, exported, err)
	}
	if err := flush(); err != nil {
		log.Fatalf("final flush: %v", err)
	}
	log.Printf("DONE (%s): exported %d docs to %s/%s in %s (kor scanned %d)",
		mode, exported, cf.project, cf.collection, time.Since(start).Round(time.Second), total)
}

// exportIDs replays an explicit id list Kor → Firestore, for reconciling a
// known set rather than a whole collection.
func exportIDs(ctx context.Context, cf copyFlags, src, dst *firestore.Client, idsFile string, dryRun bool, lim *limiter) {
	raw, err := os.ReadFile(idsFile)
	if err != nil {
		log.Fatalf("read ids file: %v", err)
	}
	var refs []*firestore.DocumentRef
	for _, line := range bytes.Split(raw, []byte("\n")) {
		if id := string(bytes.TrimSpace(line)); id != "" {
			refs = append(refs, src.Collection(cf.collection).Doc(id))
		}
	}

	copied, missing := 0, 0
	for start := 0; start < len(refs); start += 100 {
		end := min(start+100, len(refs))
		snaps, err := src.GetAll(ctx, refs[start:end])
		if err != nil {
			log.Fatalf("kor GetAll: %v", err)
		}
		batch := dst.Batch()
		n := 0
		for _, s := range snaps {
			if !s.Exists() {
				missing++
				log.Printf("MISSING in kor: %s", s.Ref.ID)
				continue
			}
			batch.Set(dst.Collection(cf.collection).Doc(s.Ref.ID), s.Data())
			n++
		}
		if n > 0 && !dryRun {
			lim.wait(n)
			if _, err := batch.Commit(ctx); err != nil {
				log.Fatalf("firestore commit: %v", err)
			}
		}
		copied += n
	}
	mode := "LIVE"
	if dryRun {
		mode = "DRY-RUN"
	}
	log.Printf("REPLAY DONE (%s): %d copied, %d missing (of %d requested)", mode, copied, missing, len(refs))
}

// limiter is a minimal write-rate governor: it spends a per-second budget and
// sleeps when a batch would overrun it.
//
// Firestore admits bursts happily and then starts returning RESOURCE_EXHAUSTED,
// so an unthrottled replay of a large collection fails partway through rather
// than running slowly — the worse outcome when the reason you are running it is
// that something has already gone wrong.
type limiter struct {
	perSec  int
	window  time.Time
	spent   int
	enabled bool
}

func newLimiter(perSec int) *limiter {
	return &limiter{perSec: perSec, window: time.Now(), enabled: perSec > 0}
}

// wait blocks until n more writes fit inside the current one-second window.
func (l *limiter) wait(n int) {
	if !l.enabled {
		return
	}
	if elapsed := time.Since(l.window); elapsed >= time.Second {
		l.window, l.spent = time.Now(), 0
	}
	if l.spent+n > l.perSec {
		if rest := time.Second - time.Since(l.window); rest > 0 {
			time.Sleep(rest)
		}
		l.window, l.spent = time.Now(), 0
	}
	l.spent += n
}
