package main

import (
	"context"
	"flag"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/omelas-tech/kor/internal/mirror"
)

// runMirror keeps collection trees identical in Firestore and kord, both
// ways, while clients are split between the two (see internal/mirror).
//
//	kor mirror -project my-project -roots chats,comments \
//	  -groups chats,messages,comments [-kor 127.0.0.1:6565] \
//	  [-feed http://127.0.0.1:6566] [-state /var/lib/kor/mirror.cursor] [-dry-run]
func runMirror(args []string) {
	fs := flag.NewFlagSet("mirror", flag.ExitOnError)
	var cf copyFlags
	fs.StringVar(&cf.project, "project", "", "GCP project id (required)")
	fs.StringVar(&cf.korAddr, "kor", "127.0.0.1:6565", "kord trusted gRPC address")
	fs.StringVar(&cf.creds, "creds", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"), "service account JSON for the project")
	feedURL := fs.String("feed", "http://127.0.0.1:6566", "kord change-feed API")
	roots := fs.String("roots", "", "top-level collections to mirror, with everything under them (required)")
	groups := fs.String("groups", "", "collection ids to follow: the roots plus every subcollection name under them (default: the roots)")
	state := fs.String("state", "", "file holding the kord feed cursor across restarts (recommended)")
	dryRun := fs.Bool("dry-run", false, "log what would be written, write nothing")
	verbose := fs.Bool("v", false, "debug logging")
	_ = fs.Parse(args)
	if cf.project == "" || *roots == "" {
		fs.Usage()
		os.Exit(2)
	}
	if *groups == "" {
		*groups = *roots
	}
	level := slog.LevelInfo
	if *verbose {
		level = slog.LevelDebug
	}
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	src := sourceClient(ctx, cf)
	defer src.Close()
	dst := korClient(ctx, cf)
	defer dst.Close()

	m, err := mirror.New(mirror.Config{
		Firestore: src, Kor: dst, FeedURL: strings.TrimSuffix(*feedURL, "/"),
		Roots: split(*roots), Groups: split(*groups),
		StateFile: *state, DryRun: *dryRun, Log: logger,
	})
	if err != nil {
		log.Fatal(err)
	}
	go func() {
		select {
		case <-m.Ready():
			logger.Info("mirror: initial copy complete; following both sides")
		case <-ctx.Done():
		}
	}()
	if err := m.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatal(err)
	}
}

func split(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
