package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"github.com/omelas-tech/kor/internal/index"
	"github.com/omelas-tech/kor/internal/store"
)

// `kor index` is the operator surface for composite indexes. It talks to
// Postgres directly rather than through kord, because registering and
// backfilling are administrative acts on the store, not queries against it.

func runIndex(args []string) {
	if len(args) == 0 {
		indexUsage()
	}
	switch args[0] {
	case "apply":
		runIndexApply(args[1:])
	case "list":
		runIndexList(args[1:])
	case "backfill":
		runIndexBackfill(args[1:])
	case "drop":
		runIndexDrop(args[1:])
	default:
		indexUsage()
	}
}

func indexUsage() {
	fmt.Fprintln(os.Stderr, "usage: kor index <apply|list|backfill|drop> [flags]")
	os.Exit(2)
}

func fatal(format string, args ...any) { log.Fatalf(format, args...) }

func openAdminStore(ctx context.Context, dsn string) *store.Store {
	if dsn == "" {
		fatal("-pg-dsn is required")
	}
	s, err := store.Open(ctx, dsn)
	if err != nil {
		fatal("open store: %v", err)
	}
	return s
}

// runIndexApply registers definitions from a firestore.indexes.json.
//
// It does NOT backfill, and therefore does not enable anything: a registered
// index holds entries only for documents written after registration, so serving
// reads from it would silently return fewer documents than the collection
// holds. Backfill is a separate, explicit step for exactly that reason.
func runIndexApply(args []string) {
	fs := flag.NewFlagSet("index apply", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	file := fs.String("file", "firestore.indexes.json", "Firestore index configuration")
	only := fs.String("collection", "", "comma-separated collections to register (default: those Kor holds)")
	all := fs.Bool("all", false, "register every collection in the file, even ones Kor does not serve")
	dry := fs.Bool("dry-run", false, "print what would be registered and exit")
	_ = fs.Parse(args)

	f, err := os.Open(*file)
	if err != nil {
		fatal("open %s: %v", *file, err)
	}
	defer f.Close()
	defs, skipped, err := index.ParseConfig(f)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	s := openAdminStore(ctx, *dsn)
	defer s.Close()

	// Default to the collections Kor actually holds. A project's index file
	// describes its whole Firestore; registering all of it against a store
	// serving a few collections buys nothing and makes every write to those
	// collections maintain entries no query will ever read.
	var want map[string]bool
	switch {
	case *only != "":
		want = map[string]bool{}
		for _, c := range strings.Split(*only, ",") {
			if c = strings.TrimSpace(c); c != "" {
				want[c] = true
			}
		}
	case *all:
		want = nil
	default:
		if want, err = s.CollectionsWithDocuments(ctx); err != nil {
			fatal("%v", err)
		}
		if len(want) == 0 {
			fatal("Kor holds no documents; pass -collection or -all to register anyway")
		}
	}

	var selected []index.Def
	for _, d := range defs {
		if want == nil || want[d.CollectionID] {
			selected = append(selected, d)
		}
	}
	sort.Slice(selected, func(i, j int) bool { return selected[i].Spec() < selected[j].Spec() })

	for _, sk := range skipped {
		if want == nil || want[sk.Collection] {
			fmt.Printf("SKIP  %-28s %s\n", sk.Collection, sk.Reason)
		}
	}
	for _, d := range selected {
		fmt.Printf("INDEX %s\n", d.Spec())
	}
	fmt.Printf("\n%d definition(s) selected from %d in %s\n", len(selected), len(defs), *file)
	if *dry {
		return
	}
	if len(selected) == 0 {
		return
	}

	// Merge with what is already registered: SetIndexes replaces the registry
	// wholesale, so applying a subset must not unregister the rest.
	existing, err := s.ListIndexes(ctx)
	if err != nil {
		fatal("%v", err)
	}
	merged := append([]index.Def(nil), selected...)
	have := map[int64]bool{}
	for _, d := range selected {
		have[d.ID()] = true
	}
	for _, st := range existing {
		d, err := index.ParseSpec(st.Spec)
		if err != nil || have[d.ID()] {
			continue
		}
		merged = append(merged, d)
	}
	if err := s.SetIndexes(ctx, merged); err != nil {
		fatal("%v", err)
	}
	fmt.Println("registered. Run `kor index backfill` before these serve any read.")
}

func runIndexList(args []string) {
	fs := flag.NewFlagSet("index list", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	_ = fs.Parse(args)

	ctx := context.Background()
	s := openAdminStore(ctx, *dsn)
	defer s.Close()

	rows, err := s.ListIndexes(ctx)
	if err != nil {
		fatal("%v", err)
	}
	if len(rows) == 0 {
		fmt.Println("no indexes registered")
		return
	}
	fmt.Printf("%-9s %12s  %s\n", "STATUS", "ENTRIES", "SPEC")
	for _, r := range rows {
		status := "PENDING"
		if r.Ready {
			status = "ready"
		}
		fmt.Printf("%-9s %12d  %s\n", status, r.Entries, r.Spec)
	}
	fmt.Println("\nPENDING means registered but not backfilled: it is maintained on write " +
		"but never used to serve a read.")
}

func runIndexBackfill(args []string) {
	fs := flag.NewFlagSet("index backfill", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	spec := fs.String("spec", "", "backfill only this spec (default: every pending index)")
	force := fs.Bool("force", false, "also rebuild indexes already marked ready")
	_ = fs.Parse(args)

	ctx := context.Background()
	s := openAdminStore(ctx, *dsn)
	defer s.Close()

	rows, err := s.ListIndexes(ctx)
	if err != nil {
		fatal("%v", err)
	}
	var todo []index.Def
	for _, r := range rows {
		if *spec != "" && r.Spec != *spec {
			continue
		}
		if r.Ready && !*force {
			continue
		}
		d, err := index.ParseSpec(r.Spec)
		if err != nil {
			fmt.Printf("SKIP  %s: %v\n", r.Spec, err)
			continue
		}
		todo = append(todo, d)
	}
	if len(todo) == 0 {
		fmt.Println("nothing to backfill")
		return
	}
	for _, d := range todo {
		n, err := s.BackfillIndex(ctx, d)
		if err != nil {
			fatal("backfill %s: %v", d.Spec(), err)
		}
		fmt.Printf("ready  %12d entries  %s\n", n, d.Spec())
	}
}

func runIndexDrop(args []string) {
	fs := flag.NewFlagSet("index drop", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	spec := fs.String("spec", "", "spec to drop, as printed by `kor index list` (required)")
	_ = fs.Parse(args)
	if *spec == "" {
		fatal("-spec is required")
	}
	d, err := index.ParseSpec(*spec)
	if err != nil {
		fatal("%v", err)
	}

	ctx := context.Background()
	s := openAdminStore(ctx, *dsn)
	defer s.Close()

	n, err := s.DropIndex(ctx, d)
	if err != nil {
		fatal("%v", err)
	}
	fmt.Printf("dropped %s (%d entries removed)\n", d.Spec(), n)
}
