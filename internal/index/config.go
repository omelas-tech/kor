package index

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// Firestore's own index configuration — the firestore.indexes.json the Firebase
// CLI reads — is the definition source. Kor deliberately does not invent a
// format: the file already exists in every project that would migrate here, it
// is already the reviewed record of which composite indexes a codebase needs,
// and keeping one source avoids the two drifting.

type fileConfig struct {
	Indexes []fileIndex `json:"indexes"`
}

type fileIndex struct {
	CollectionGroup string      `json:"collectionGroup"`
	QueryScope      string      `json:"queryScope"`
	Fields          []fileField `json:"fields"`
}

type fileField struct {
	FieldPath string `json:"fieldPath"`
	Order     string `json:"order"`
	// Present for array-contains and vector indexes, which are different data
	// structures rather than orderings.
	ArrayConfig  string          `json:"arrayConfig"`
	VectorConfig json.RawMessage `json:"vectorConfig"`
}

// Skipped records a definition Kor will not serve, and why. Skips are returned
// rather than logged so the caller can print them: an index silently dropped
// here becomes a query that mysteriously stays slow later.
type Skipped struct {
	Collection string
	Reason     string
}

// ParseConfig reads firestore.indexes.json into definitions Kor can serve,
// alongside the ones it cannot.
//
// Unsupported kinds are skipped, never approximated. An array-contains index is
// an inverted index over element values and a vector index is an ANN structure;
// neither is an ordering, so neither can be expressed as a sortkey range.
// Pretending otherwise would produce an index that plans and returns wrong
// results, which is worse than not having it.
func ParseConfig(r io.Reader) ([]Def, []Skipped, error) {
	var cfg fileConfig
	if err := json.NewDecoder(r).Decode(&cfg); err != nil {
		return nil, nil, fmt.Errorf("index: parse config: %w", err)
	}

	var defs []Def
	var skipped []Skipped
	seen := map[int64]bool{}

	for _, fi := range cfg.Indexes {
		if fi.CollectionGroup == "" {
			return nil, nil, fmt.Errorf("index: entry with no collectionGroup")
		}
		d := Def{
			CollectionID: fi.CollectionGroup,
			Group:        strings.EqualFold(fi.QueryScope, "COLLECTION_GROUP"),
		}
		var reason string
		for _, ff := range fi.Fields {
			switch {
			case ff.ArrayConfig != "":
				reason = "array-contains index: an inverted index over element values, not an ordering"
			case len(ff.VectorConfig) > 0:
				reason = "vector index: an approximate-nearest-neighbour structure, not an ordering"
			case ff.FieldPath == "":
				reason = "field with no fieldPath"
			}
			if reason != "" {
				break
			}
			desc := strings.EqualFold(ff.Order, "DESCENDING")
			if !desc && !strings.EqualFold(ff.Order, "ASCENDING") {
				reason = fmt.Sprintf("unrecognised order %q", ff.Order)
				break
			}
			d.Fields = append(d.Fields, Field{Path: ff.FieldPath, Desc: desc})
		}
		if reason == "" && len(d.Fields) == 0 {
			reason = "no ordered fields"
		}
		if reason != "" {
			skipped = append(skipped, Skipped{Collection: fi.CollectionGroup, Reason: reason})
			continue
		}
		// Firestore's own file sometimes lists the implicit terminator
		// explicitly; the two spellings must not become two indexes.
		if last := d.Fields[len(d.Fields)-1]; last.Path == NameField {
			d.Fields = d.Fields[:len(d.Fields)-1]
			if len(d.Fields) == 0 {
				skipped = append(skipped, Skipped{fi.CollectionGroup, "__name__ only: served by the general path"})
				continue
			}
		}
		if id := d.ID(); seen[id] {
			continue // duplicate spec
		} else {
			seen[id] = true
		}
		defs = append(defs, d)
	}
	return defs, skipped, nil
}

// ParseSpec reconstructs a Def from the canonical spec string, so Postgres can
// be the source of truth at runtime rather than the config file: kord reads
// index_defs and never needs the JSON on the server.
func ParseSpec(spec string) (Def, error) {
	var d Def
	if rest, ok := strings.CutPrefix(spec, "group:"); ok {
		d.Group = true
		spec = rest
	}
	parts := strings.Split(spec, "|")
	if len(parts) < 2 || parts[0] == "" {
		return Def{}, fmt.Errorf("index: malformed spec %q", spec)
	}
	d.CollectionID = parts[0]
	for _, p := range parts[1:] {
		path, dir, ok := strings.Cut(p, " ")
		if !ok || path == "" {
			return Def{}, fmt.Errorf("index: malformed field %q in spec %q", p, spec)
		}
		switch dir {
		case "asc":
			d.Fields = append(d.Fields, Field{Path: path})
		case "desc":
			d.Fields = append(d.Fields, Field{Path: path, Desc: true})
		default:
			return Def{}, fmt.Errorf("index: malformed direction %q in spec %q", dir, spec)
		}
	}
	return d, nil
}
