package main

import (
	"context"
	"flag"
	"fmt"
	"os"
)

// runDrop removes a collection from Kor. It is the undo for an import that went
// wrong, and it is deliberately awkward: destructive, irreversible from Kor's
// side, and only safe when the source of truth is elsewhere.
func runDrop(args []string) {
	fs := flag.NewFlagSet("drop", flag.ExitOnError)
	dsn := fs.String("pg-dsn", os.Getenv("KORD_PG_DSN"), "PostgreSQL DSN (required)")
	collection := fs.String("collection", "", "collection id to remove (required)")
	dry := fs.Bool("dry-run", false, "report what would be removed and exit")
	yes := fs.Bool("yes", false, "actually delete; without it this is a dry run")
	batch := fs.Int("batch", 2000, "documents per transaction")
	_ = fs.Parse(args)

	if *collection == "" {
		fatal("-collection is required")
	}
	ctx := context.Background()
	s := openAdminStore(ctx, *dsn)
	defer s.Close()

	n, err := s.CountCollection(ctx, *collection)
	if err != nil {
		fatal("%v", err)
	}
	fmt.Printf("%s holds %d document(s)\n", *collection, n)
	if n == 0 {
		return
	}
	// Default to reporting rather than deleting. The flag is the confirmation:
	// there is no undo inside Kor, so the only recovery is re-importing from
	// the source — which is fine for a mirror and not fine for anything else.
	if *dry || !*yes {
		fmt.Println("dry run — pass -yes to delete. Kor cannot undo this; recovery means re-importing from the source.")
		return
	}

	deleted, err := s.DropCollection(ctx, *collection, *batch, func(done int64) {
		if done%20000 == 0 {
			fmt.Printf("  deleted %d/%d\n", done, n)
		}
	})
	if err != nil {
		fatal("after deleting %d: %v", deleted, err)
	}
	fmt.Printf("deleted %d document(s) from %s\n", deleted, *collection)
}
