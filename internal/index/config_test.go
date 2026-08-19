package index

import (
	"strings"
	"testing"
)

func TestParseConfigReadsOrderedFields(t *testing.T) {
	defs, skipped, err := ParseConfig(strings.NewReader(`{"indexes":[
		{"collectionGroup":"posts","queryScope":"COLLECTION","fields":[
			{"fieldPath":"author","order":"ASCENDING"},
			{"fieldPath":"score","order":"DESCENDING"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 0 {
		t.Fatalf("unexpected skips: %v", skipped)
	}
	if len(defs) != 1 || defs[0].Spec() != "posts|author asc|score desc" {
		t.Fatalf("got %+v", defs)
	}
}

// A vector index is an approximate-nearest-neighbour structure, not an
// ordering, so it cannot be expressed as a sort-key range. Approximating it
// would produce an index the planner accepts and then serves wrong results
// from — strictly worse than not having it.
func TestParseConfigSkipsUnservableKindsWithAReason(t *testing.T) {
	defs, skipped, err := ParseConfig(strings.NewReader(`{"indexes":[
		{"collectionGroup":"embeds","fields":[
			{"fieldPath":"vec","vectorConfig":{"dimension":768}}]},
		{"collectionGroup":"weird","fields":[
			{"fieldPath":"tags","arrayConfig":"SOMETHING_ELSE"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 0 {
		t.Fatalf("nothing here is servable, got %+v", defs)
	}
	if len(skipped) != 2 {
		t.Fatalf("want 2 skips, got %v", skipped)
	}
	for _, s := range skipped {
		if s.Reason == "" {
			t.Error("a skip without a reason becomes a query that mysteriously stays slow")
		}
	}
}

// array-contains IS servable: the component fans a document out into one entry
// per element, which turns `array-contains x` into a prefix lookup.
func TestParseConfigReadsArrayContains(t *testing.T) {
	defs, skipped, err := ParseConfig(strings.NewReader(`{"indexes":[
		{"collectionGroup":"chats","queryScope":"COLLECTION","fields":[
			{"fieldPath":"participants","arrayConfig":"CONTAINS"},
			{"fieldPath":"updatedAt","order":"DESCENDING"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 0 {
		t.Fatalf("unexpected skips: %v", skipped)
	}
	if len(defs) != 1 || defs[0].Spec() != "chats|participants contains|updatedAt desc" {
		t.Fatalf("got %+v", defs)
	}
	if p, ok := defs[0].ContainsPath(); !ok || p != "participants" {
		t.Errorf("ContainsPath = %q, %v", p, ok)
	}
}

// Firestore allows one array-contains field per index; two would need a cross
// product of elements, which is a different structure again.
func TestParseConfigRejectsTwoArrayContainsFields(t *testing.T) {
	defs, skipped, err := ParseConfig(strings.NewReader(`{"indexes":[
		{"collectionGroup":"x","fields":[
			{"fieldPath":"a","arrayConfig":"CONTAINS"},
			{"fieldPath":"b","arrayConfig":"CONTAINS"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 0 || len(skipped) != 1 {
		t.Fatalf("defs=%+v skipped=%v", defs, skipped)
	}
}

// Firestore's file sometimes spells the implicit terminator out. The two
// spellings must be one index, or a project gets two copies of it — both
// maintained on every write, only one ever planned.
func TestParseConfigFoldsExplicitNameTerminator(t *testing.T) {
	defs, _, err := ParseConfig(strings.NewReader(`{"indexes":[
		{"collectionGroup":"posts","fields":[
			{"fieldPath":"author","order":"ASCENDING"},
			{"fieldPath":"__name__","order":"ASCENDING"}]},
		{"collectionGroup":"posts","fields":[
			{"fieldPath":"author","order":"ASCENDING"}]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(defs) != 1 {
		t.Fatalf("want the two spellings folded into one index, got %d: %+v", len(defs), defs)
	}
}

func TestParseSpecRoundTrips(t *testing.T) {
	for _, d := range []Def{
		{CollectionID: "posts", Fields: []Field{{Path: "author"}, {Path: "score", Desc: true}}},
		{CollectionID: "posts", Fields: []Field{{Path: "a.b.c"}}, Group: true},
		{CollectionID: "x", Fields: []Field{{Path: "f", Desc: true}, {Path: NameField, Desc: true}}},
	} {
		got, err := ParseSpec(d.Spec())
		if err != nil {
			t.Fatalf("%s: %v", d.Spec(), err)
		}
		if got.Spec() != d.Spec() || got.ID() != d.ID() {
			t.Errorf("round trip changed identity: %s -> %s", d.Spec(), got.Spec())
		}
	}
}

func TestParseSpecRejectsMalformed(t *testing.T) {
	for _, bad := range []string{"", "posts", "posts|", "posts|f", "posts|f sideways", "|f asc"} {
		if _, err := ParseSpec(bad); err == nil {
			t.Errorf("accepted malformed spec %q", bad)
		}
	}
}
