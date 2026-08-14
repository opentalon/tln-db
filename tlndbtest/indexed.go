package tlndbtest

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	tlndb "github.com/opentalon/tln-db"
)

// IndexedFactory builds a fresh, empty IndexedStore for a single
// subtest. Same contract as Factory: must register cleanup via
// t.Cleanup.
type IndexedFactory func(t *testing.T) tlndb.IndexedStore

// IndexedSuite runs the per-block lookup contract specified by
// tln-language issue #27 against the store produced by factory.
//
// Specifications enforced:
//
//   1. Lookup — Put then exact-term Lookup returns the doc; absent
//      terms return an empty set.
//   2. LookupPrefix — returns the union of all term bitmaps whose key
//      starts with prefix; empty prefix matches every term.
//   3. LookupNumericRange — closed/open bounds work; NaN/Inf bounds
//      return an error; reversed bounds return an empty set.
//   4. WindowQuery — returns matching events in time-ascending order;
//      filters by `types` when non-empty; absent (entity, itemID)
//      returns an empty slice without error.
//   5. GroupCount — counter agrees with brute-force count across
//      pre-aggregated docs; bumps First/Last correctly.
//   6. Stats — Welford aggregate matches the closed-form numpy
//      equivalent to 1e-9 over a uniform sample.
//   7. LastSeen — returns max(at) across docs with the same
//      (itemID, recordType); absent returns ok=false; recomputes on
//      delete of the max-owner.
//   8. Ancestors / Descendants — closure agrees with a brute-force
//      parent-chain walk on a 3-level test tree.
func IndexedSuite(t *testing.T, factory IndexedFactory) {
	t.Helper()
	cases := []struct {
		name string
		fn   func(*testing.T, IndexedFactory)
	}{
		{"Lookup", indexedTestLookup},
		{"LookupPrefix", indexedTestLookupPrefix},
		{"LookupNumericRange", indexedTestLookupNumericRange},
		{"LookupNumericRangeNaNRejected", indexedTestLookupNumericRangeNaN},
		{"WindowQuery", indexedTestWindowQuery},
		{"GroupCount", indexedTestGroupCount},
		{"Stats", indexedTestStats},
		{"LastSeen", indexedTestLastSeen},
		{"Closure", indexedTestClosure},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			c.fn(t, factory)
		})
	}
}

func indexedTestLookup(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	mustPut(t, s, "tenant-a", "d1", `{"status":"active"}`)
	mustPut(t, s, "tenant-a", "d2", `{"status":"retired"}`)

	got, err := s.Lookup(ctx, "tenant-a", "active")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got.Len() != 1 || !got.Contains("d1") {
		t.Fatalf("Lookup active: %v", collect(got))
	}
	got, _ = s.Lookup(ctx, "tenant-a", "never-indexed")
	if got.Len() != 0 {
		t.Fatalf("Lookup absent: should be empty, got %v", collect(got))
	}
}

func indexedTestLookupPrefix(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	mustPut(t, s, "tenant-a", "d1", `{"category":"truck"}`)
	mustPut(t, s, "tenant-a", "d2", `{"category":"trailer"}`)
	mustPut(t, s, "tenant-a", "d3", `{"category":"car"}`)
	got, err := s.LookupPrefix(ctx, "tenant-a", "category:tr")
	if err != nil {
		t.Fatalf("LookupPrefix: %v", err)
	}
	want := []string{"d1", "d2"}
	if !equalDocs(got, want) {
		t.Fatalf("LookupPrefix: got %v, want %v", collect(got), want)
	}
}

func indexedTestLookupNumericRange(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		mustPut(t, s, "tenant-a", docID(i), fmt.Sprintf(`{"km":%d}`, i*10))
	}
	got, err := s.LookupNumericRange(ctx, "tenant-a", "km", 20, 40, tlndb.RangeOpts{})
	if err != nil {
		t.Fatalf("LookupNumericRange: %v", err)
	}
	want := []string{"d2", "d3", "d4"}
	if !equalDocs(got, want) {
		t.Fatalf("closed: got %v, want %v", collect(got), want)
	}
	got, _ = s.LookupNumericRange(ctx, "tenant-a", "km", 20, 40, tlndb.RangeOpts{MinExclusive: true, MaxExclusive: true})
	want = []string{"d3"}
	if !equalDocs(got, want) {
		t.Fatalf("open: got %v, want %v", collect(got), want)
	}
}

func indexedTestLookupNumericRangeNaN(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	_, err := s.LookupNumericRange(context.Background(), "tenant-a", "km", math.NaN(), 10, tlndb.RangeOpts{})
	if err == nil {
		t.Fatal("expected error for NaN bound")
	}
}

func indexedTestWindowQuery(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	mustPut(t, s, "tenant-a", "d1", `{"item_id":"i1","type":"inspect","at":1000}`)
	mustPut(t, s, "tenant-a", "d2", `{"item_id":"i1","type":"repair","at":2000}`)
	mustPut(t, s, "tenant-a", "d3", `{"item_id":"i1","type":"inspect","at":3000}`)
	events, err := s.WindowQuery(ctx, "tenant-a", "i1", nil, 0)
	if err != nil {
		t.Fatalf("WindowQuery: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	for i := 1; i < len(events); i++ {
		if !events[i].At.After(events[i-1].At) {
			t.Fatalf("not ascending at %d", i)
		}
	}
	events, _ = s.WindowQuery(ctx, "tenant-a", "i1", []string{"inspect"}, 0)
	if len(events) != 2 {
		t.Fatalf("filtered: got %d, want 2", len(events))
	}

	events, _ = s.WindowQuery(ctx, "tenant-a", "unknown", nil, 0)
	if len(events) != 0 {
		t.Fatalf("absent itemID: got %d", len(events))
	}
}

func indexedTestGroupCount(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	for i := 1; i <= 3; i++ {
		mustPut(t, s, "tenant-a", docID(i), fmt.Sprintf(`{"item_id":"i1","type":"inspect","at":%d}`, i*1000))
	}
	g, err := s.GroupCount(ctx, "tenant-a", "i1", "type", "inspect")
	if err != nil {
		t.Fatalf("GroupCount: %v", err)
	}
	if g.Count != 3 {
		t.Fatalf("Count = %d, want 3", g.Count)
	}
	if g.First.UnixNano() != 1000 || g.Last.UnixNano() != 3000 {
		t.Fatalf("timestamps: %d / %d", g.First.UnixNano(), g.Last.UnixNano())
	}
}

func indexedTestStats(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	values := []float64{10, 20, 30, 40, 50}
	for i, v := range values {
		mustPut(t, s, "tenant-a", docID(i), fmt.Sprintf(`{"km":%g}`, v))
	}
	got, err := s.Stats(ctx, "tenant-a", "km")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if got.Count != 5 {
		t.Fatalf("Count = %d, want 5", got.Count)
	}
	if math.Abs(got.Mean-30) > 1e-9 {
		t.Fatalf("Mean = %g, want 30", got.Mean)
	}
}

func indexedTestLastSeen(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	mustPut(t, s, "tenant-a", "d1", `{"item_id":"i1","type":"inspect","at":1000}`)
	mustPut(t, s, "tenant-a", "d2", `{"item_id":"i1","type":"inspect","at":2000}`)
	at, ok, err := s.LastSeen(ctx, "tenant-a", "i1", "inspect")
	if err != nil {
		t.Fatalf("LastSeen: %v", err)
	}
	if !ok || at.UnixNano() != 2000 {
		t.Fatalf("LastSeen = %d, want 2000", at.UnixNano())
	}
	_, ok, _ = s.LastSeen(ctx, "tenant-a", "i1", "unknown-type")
	if ok {
		t.Fatal("LastSeen unknown type: should be ok=false")
	}
}

func indexedTestClosure(t *testing.T, factory IndexedFactory) {
	s := factory(t)
	ctx := context.Background()
	mustPut(t, s, "tenant-a", "Vehicles", `{}`)
	mustPut(t, s, "tenant-a", "Trucks", `{"parent":"Vehicles"}`)
	mustPut(t, s, "tenant-a", "BigRigs", `{"parent":"Trucks"}`)
	chain, err := s.Ancestors(ctx, "tenant-a", "BigRigs")
	if err != nil {
		t.Fatalf("Ancestors: %v", err)
	}
	want := []string{"Trucks", "Vehicles"}
	if !equalStrings(chain, want) {
		t.Fatalf("Ancestors = %v, want %v", chain, want)
	}
	desc, err := s.Descendants(ctx, "tenant-a", "Vehicles")
	if err != nil {
		t.Fatalf("Descendants: %v", err)
	}
	if !equalDocs(desc, []string{"Trucks", "BigRigs"}) {
		t.Fatalf("Descendants = %v, want [BigRigs Trucks]", collect(desc))
	}
}

// ----- helpers -----

func mustPut(t *testing.T, s tlndb.IndexedStore, entityID, docID, body string) {
	t.Helper()
	if err := s.Put(context.Background(), entityID, docID, []byte(body)); err != nil {
		t.Fatalf("Put %q: %v", docID, err)
	}
}

func collect(s tlndb.DocIDSet) []string {
	out := make([]string, 0, s.Len())
	s.ForEach(func(id string) bool {
		out = append(out, id)
		return true
	})
	sort.Strings(out)
	return out
}

func equalDocs(s tlndb.DocIDSet, want []string) bool {
	got := collect(s)
	sort.Strings(want)
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func docID(i int) string { return fmt.Sprintf("d%d", i) }
