package bboltstore_test

import (
	"context"
	"sort"
	"testing"

	tlndb "github.com/opentalon/tln-db"
)

func TestPutThenLookup(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "tenant-a", "vehicle-1", []byte(`{"status":"active","category":"truck","km":45000}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "tenant-a", "vehicle-2", []byte(`{"status":"active","category":"car","km":12000}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Put(ctx, "tenant-a", "vehicle-3", []byte(`{"status":"retired","category":"truck","km":99999}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Bare term lookup
	got, err := s.Lookup(ctx, "tenant-a", "active")
	if err != nil {
		t.Fatalf("Lookup active: %v", err)
	}
	if !sameDocs(got, []string{"vehicle-1", "vehicle-2"}) {
		t.Fatalf("Lookup active: got %v, want [vehicle-1, vehicle-2]", flatten(got))
	}

	// Composite term lookup
	got, err = s.Lookup(ctx, "tenant-a", "category:truck")
	if err != nil {
		t.Fatalf("Lookup category:truck: %v", err)
	}
	if !sameDocs(got, []string{"vehicle-1", "vehicle-3"}) {
		t.Fatalf("Lookup category:truck: got %v, want [vehicle-1, vehicle-3]", flatten(got))
	}
}

func TestPutThenLookupPrefix(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	docs := map[string][]byte{
		"a": []byte(`{"category":"truck"}`),
		"b": []byte(`{"category":"trailer"}`),
		"c": []byte(`{"category":"car"}`),
	}
	if err := s.BatchPut(ctx, "tenant-a", docs); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	got, err := s.LookupPrefix(ctx, "tenant-a", "category:tr")
	if err != nil {
		t.Fatalf("LookupPrefix: %v", err)
	}
	if !sameDocs(got, []string{"a", "b"}) {
		t.Fatalf("LookupPrefix: got %v, want [a b]", flatten(got))
	}
}

func TestDeleteRemovesFromIndex(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`{"status":"active"}`)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(ctx, "tenant-a", "doc-1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	got, err := s.Lookup(ctx, "tenant-a", "active")
	if err != nil {
		t.Fatalf("Lookup after Delete: %v", err)
	}
	if got.Len() != 0 {
		t.Fatalf("Lookup after Delete: got %d entries, want 0 (%v)", got.Len(), flatten(got))
	}
}

func TestUpdateRefreshesIndex(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()

	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`{"status":"active"}`)); err != nil {
		t.Fatalf("Put v1: %v", err)
	}
	if err := s.Put(ctx, "tenant-a", "doc-1", []byte(`{"status":"retired"}`)); err != nil {
		t.Fatalf("Put v2: %v", err)
	}

	gotActive, _ := s.Lookup(ctx, "tenant-a", "active")
	if gotActive.Len() != 0 {
		t.Fatalf("active should be gone after update, got %v", flatten(gotActive))
	}
	gotRetired, _ := s.Lookup(ctx, "tenant-a", "retired")
	if !sameDocs(gotRetired, []string{"doc-1"}) {
		t.Fatalf("retired: got %v, want [doc-1]", flatten(gotRetired))
	}
}

func TestLookupOpaqueBytesIsNoOp(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	// Non-JSON bytes are valid documents per the DocumentStore
	// contract; they just don't get indexed.
	if err := s.Put(ctx, "tenant-a", "raw-1", []byte("not json at all")); err != nil {
		t.Fatalf("Put raw bytes: %v", err)
	}
	if got, _ := s.Get(ctx, "tenant-a", "raw-1"); string(got) != "not json at all" {
		t.Fatalf("Get raw: %q", got)
	}
	prefixed, _ := s.LookupPrefix(ctx, "tenant-a", "")
	if prefixed.Len() != 0 {
		t.Fatalf("non-JSON doc should not contribute terms, got %v", flatten(prefixed))
	}
}

func flatten(s tlndb.DocIDSet) []string {
	out := make([]string, 0, s.Len())
	s.ForEach(func(id string) bool {
		out = append(out, id)
		return true
	})
	sort.Strings(out)
	return out
}

func sameDocs(s tlndb.DocIDSet, want []string) bool {
	got := flatten(s)
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
