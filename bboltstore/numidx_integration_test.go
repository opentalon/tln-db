package bboltstore_test

import (
	"context"
	"math"
	"testing"

	tlndb "github.com/opentalon/tln-db"
)

func TestPutThenLookupNumericRange(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	if err := s.BatchPut(ctx, "tenant-a", map[string][]byte{
		"v1": []byte(`{"km":10}`),
		"v2": []byte(`{"km":50}`),
		"v3": []byte(`{"km":75}`),
		"v4": []byte(`{"km":100}`),
	}); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	got, err := s.LookupNumericRange(ctx, "tenant-a", "km", 25, 80, tlndb.RangeOpts{})
	if err != nil {
		t.Fatalf("LookupNumericRange: %v", err)
	}
	if !sameDocs(got, []string{"v2", "v3"}) {
		t.Fatalf("got %v, want [v2 v3]", flatten(got))
	}
}

func TestLookupNumericRangeExclusiveBounds(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.BatchPut(ctx, "tenant-a", map[string][]byte{
		"v1": []byte(`{"km":10}`),
		"v2": []byte(`{"km":20}`),
		"v3": []byte(`{"km":30}`),
	})
	got, _ := s.LookupNumericRange(ctx, "tenant-a", "km", 10, 30, tlndb.RangeOpts{MinExclusive: true, MaxExclusive: true})
	if !sameDocs(got, []string{"v2"}) {
		t.Fatalf("got %v, want [v2]", flatten(got))
	}
}

func TestLookupNumericRangeRejectsNaN(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	_, err := s.LookupNumericRange(context.Background(), "tenant-a", "km", math.NaN(), 100, tlndb.RangeOpts{})
	if err == nil {
		t.Fatal("expected error for NaN bound")
	}
}

func TestUpdateRemovesOldNumeric(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	ctx := context.Background()
	_ = s.Put(ctx, "tenant-a", "v1", []byte(`{"km":10}`))
	_ = s.Put(ctx, "tenant-a", "v1", []byte(`{"km":100}`))

	// Range covering only the old value: should be empty.
	got, _ := s.LookupNumericRange(ctx, "tenant-a", "km", 0, 50, tlndb.RangeOpts{})
	if got.Len() != 0 {
		t.Fatalf("old value should be gone, got %v", flatten(got))
	}
	got, _ = s.LookupNumericRange(ctx, "tenant-a", "km", 50, 200, tlndb.RangeOpts{})
	if !sameDocs(got, []string{"v1"}) {
		t.Fatalf("new value: got %v, want [v1]", flatten(got))
	}
}
